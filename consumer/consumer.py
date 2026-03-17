# python
# !/usr/bin/env python3
import argparse
import os
import logging
import sys
import json
import ast
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.streaming import StreamingQuery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("spark-kafka-consumer")


def create_spark(app_name="spark-kafka-mongodb-consumer", kafka_package=None, mongo_package=None):
    """
    Creates a SparkSession with specified application name and optional Kafka and MongoDB packages.
    """
    builder = SparkSession.builder.appName(app_name)
    packages = []
    if kafka_package:
        packages.append(kafka_package)
    if mongo_package:
        packages.append(mongo_package)

    if packages:
        builder = builder.config("spark.jars.packages", ",".join(packages))

    spark = builder \
            .config("spark.master", "local[*]") \
            .config("spark.executor.cores", "2") \
            .config("spark.executor.memory", "2g") \
            .config("spark.driver.memory", "1g") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider") \
            .config("spark.sql.streaming.minBatchesToRetain", "20") \
            .config("spark.sql.streaming.maxBatchesToRetain", "100") \
            .config("spark.sql.streaming.stateStore.stateSchemaCheck", "false") \
            .config("spark.sql.streaming.stateStore.compression.codec", "lz4") \
            .config("spark.sql.streaming.stateStore.rocksdb.compression", "snappy") \
            .getOrCreate()

    try:
        logger.info("Spark version: %s", spark.version)
    except Exception:
        pass
    return spark


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--brokers", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="test-topic", help="Kafka topic to subscribe")
    parser.add_argument("--checkpoint-base", default="/Users/jay/PycharmProjects/AvriocProject/storage/checkpoint",
                        help="Base directory for Spark checkpointing")
    parser.add_argument("--starting-offsets", default="earliest", help="startingOffsets for Kafka (earliest/latest)")
    parser.add_argument("--kafka-package",
                        default="org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0",
                        help="Kafka connector package for Spark (set to empty to skip)")
    parser.add_argument("--mongo-uri",
                        default="mongodb://admin:password@localhost:27017/?authSource=admin",
                        help="MongoDB connection URI (e.g., mongodb://username:password@localhost:27017/kpi_db)")
    parser.add_argument("--mongo-package",
                        default="org.mongodb.spark:mongo-spark-connector_2.13:10.4.0",
                        help="MongoDB connector package for Spark (set to empty to skip)")
    args = parser.parse_args()

    # Create SparkSession with Kafka and MongoDB connector packages
    spark = create_spark(
        kafka_package=args.kafka_package if args.kafka_package else None,
        mongo_package=args.mongo_package if args.mongo_package else None
    )
    spark.sparkContext.setLogLevel("WARN")

    # Define schema for incoming Kafka messages, ensuring timestamp is TimestampType for windowing
    schema = T.StructType([
        T.StructField("user_id", T.StringType(), True),
        T.StructField("item_id", T.StringType(), True),
        T.StructField("interaction_type", T.StringType(), True),
        T.StructField("timestamp", T.TimestampType(), True),
    ])

    try:
        # Read stream from Kafka
        raw = spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", args.brokers) \
            .option("subscribe", args.topic) \
            .option("startingOffsets", args.starting_offsets) \
            .option("maxOffsetsPerTrigger", "10000") \
            .option("failOnDataLoss", "false") \
            .load()
        # Ensure offsets are committed on shutdown
        spark.conf.set("spark.sql.streaming.kafka.commitOffsetsOnStop", "true")
        # Enable checkpointing for fault tolerance
    except Exception as e:
        msg = str(e)
        if "NoSuchMethodError" in msg or "Failed to find data source: kafka" in msg:
            logger.error("Kafka source or connector issue: %s", msg)
            logger.error("Ensure Kafka is running and the correct Spark-Kafka connector package is used.")
            sys.exit(1)
        logger.exception("Error creating Kafka stream")
        sys.exit(1)

    # Parse JSON value, cast timestamp, and add watermark for stateful aggregations
    parsed = raw.selectExpr("CAST(value AS STRING) as json_str") \
        .select(F.from_json(F.col("json_str"), schema).alias("data")) \
        .select("data.*") \
        .filter(F.col("user_id").isNotNull() & F.col("item_id").isNotNull() & F.col("timestamp").isNotNull()) \
        .withWatermark("timestamp", "10 minutes")  # 10 minute watermark to allow for late data

    # --- Aggregation 1: Total interactions per user per window ---
    # Groups by a 10-minute window (sliding every 5 minutes) and user_id, then counts interactions.
    user_agg_df = parsed.groupBy(
        F.window(F.col("timestamp"), "10 minutes", "5 minutes").alias("window"),
        F.col("user_id")
    ).agg(
        F.count("*").alias("total_interactions")
    ).select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("user_id"),
        F.col("total_interactions")
    )

    # --- Aggregation 2: Total interactions per item per window ---
    # Groups by a 10-minute window (sliding every 5 minutes) and item_id, then counts interactions.
    item_agg_df = parsed.groupBy(
        F.window(F.col("timestamp"), "10 minutes", "5 minutes").alias("window"),
        F.col("item_id")
    ).agg(
        F.count("*").alias("total_interactions")
    ).select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("item_id"),
        F.col("total_interactions")
    )

    # Define checkpoint locations for each streaming query
    user_checkpoint = os.path.join(args.checkpoint_base, "user_interactions_checkpoint")
    item_checkpoint = os.path.join(args.checkpoint_base, "item_interactions_checkpoint")

    # Start streaming query for user aggregations, writing to MongoDB
    user_query = user_agg_df.writeStream \
        .format("mongodb") \
        .queryName("user_interactions") \
        .option("checkpointLocation", user_checkpoint) \
        .option("connection.uri", args.mongo_uri) \
        .option("database", "kpi_db") \
        .option("collection", "user_interactions") \
        .option("operationType", "update") \
        .option("upsertDocument", "true") \
        .option("idFieldList", "window_start,window_end,user_id") \
        .option("batchSize", "1000") \
        .outputMode("append") \
        .start()

    # Start streaming query for item aggregations, writing to MongoDB
    item_query = item_agg_df.writeStream \
        .format("mongodb") \
        .queryName("item_interactions") \
        .option("checkpointLocation", item_checkpoint) \
        .option("connection.uri", args.mongo_uri) \
        .option("database", "kpi_db") \
        .option("collection", "item_interactions") \
        .option("operationType", "update") \
        .option("upsertDocument", "true") \
        .option("idFieldList", "window_start,window_end,item_id") \
        .option("batchSize", "1000") \
        .outputMode("append") \
        .start()

    logger.info("Spark consumer started -> brokers=%s, topic=%s, mongo_uri=%s", args.brokers, args.topic,
                args.mongo_uri)
    logger.info("User aggregations writing to collection: user_interactions")
    logger.info("Item aggregations writing to collection: item_interactions")

    # Track previous offsets and last logged batch ID
    previous_processed_offsets = None
    last_logged_batch_id = None

    try:
        # Await termination of any active streaming query to keep the application running
        # and periodically log progress/lag
        while True:
            # Use awaitTermination with a timeout to allow for periodic checks
            # This will block for 'timeout' seconds or until the query terminates
            if user_query.awaitTermination(30):  # Check every 30 seconds
                break  # Query terminated

            progress = user_query.lastProgress
            if progress:
                # Assuming a single Kafka source, access the first element in sources
                source_progress = progress.sources[0]
                current_batch_id = progress.batchId
                input_rows = progress.numInputRows
                
                # Only process if this is a new batch (batch ID changed)
                if current_batch_id != last_logged_batch_id:
                    # Offsets Spark has processed up to for this batch
                    processed_offsets_map = source_progress.endOffset
                    # Latest offsets available in Kafka when this batch started processing
                    latest_offsets_in_kafka_map = source_progress.latestOffset

                    total_lag = 0
                    processed_since_last = 0
                    
                    # Spark Structured Streaming stores offsets as nested dict: {"topic": {"partition": offset}}
                    # But sometimes they come as JSON strings or Python dict strings, so we need to parse them
                    if processed_offsets_map and latest_offsets_in_kafka_map:
                        try:
                            # Helper function to parse offset strings (handles JSON, Python dict strings, or already dicts)
                            def parse_offset(offset_value):
                                if isinstance(offset_value, dict):
                                    return offset_value
                                if not isinstance(offset_value, str):
                                    return None
                                
                                # Try JSON first (double quotes)
                                try:
                                    return json.loads(offset_value)
                                except (json.JSONDecodeError, ValueError):
                                    pass
                                
                                # Try ast.literal_eval (handles Python dict strings with single quotes)
                                try:
                                    parsed = ast.literal_eval(offset_value)
                                    if isinstance(parsed, dict):
                                        return parsed
                                except (ValueError, SyntaxError):
                                    pass
                                
                                return None
                            
                            # Parse both offset maps
                            latest_offsets_parsed = parse_offset(latest_offsets_in_kafka_map)
                            processed_offsets_parsed = parse_offset(processed_offsets_map)

                            # Handle nested structure: {"topic-name": {"0": offset, "1": offset, ...}}
                            if isinstance(latest_offsets_parsed, dict) and isinstance(processed_offsets_parsed, dict):
                                for topic_name, latest_partition_map in latest_offsets_parsed.items():
                                    if not isinstance(latest_partition_map, dict):
                                        continue

                                    processed_partition_map = {}
                                    if (topic_name in processed_offsets_parsed and
                                            isinstance(processed_offsets_parsed[topic_name], dict)):
                                        processed_partition_map = processed_offsets_parsed[topic_name]

                                    # Calculate lag per partition
                                    for partition_str, latest_offset in latest_partition_map.items():
                                        processed_offset = processed_partition_map.get(partition_str, 0)

                                        # Convert to int, handling None and string values
                                        try:
                                            latest = int(latest_offset) if latest_offset is not None else 0
                                            processed = int(processed_offset) if processed_offset is not None else 0
                                            partition_lag = max(0, latest - processed)
                                            total_lag += partition_lag
                                            
                                            # Calculate how much was processed since last batch
                                            if previous_processed_offsets:
                                                prev_processed = previous_processed_offsets.get(topic_name, {}).get(partition_str, processed)
                                                processed_since_last += max(0, processed - prev_processed)
                                        except (ValueError, TypeError) as e:
                                            logger.warning(
                                                f"Invalid offset for {topic_name}/{partition_str}: latest={latest_offset}, processed={processed_offset}, error={e}")
                                
                                # Store current processed offsets for next batch
                                previous_processed_offsets = processed_offsets_parsed.copy()
                        except Exception as e:
                            logger.error(f"Error calculating lag: {e}", exc_info=True)

                    # Log only when batch ID changes
                    if processed_since_last > 0:
                        logger.info(f"Batch ID: {current_batch_id}, Input Rows: {input_rows}, Total Lag: {total_lag}, Processed since last batch: {processed_since_last}")
                    else:
                        logger.info(f"Batch ID: {current_batch_id}, Input Rows: {input_rows}, Total Lag: {total_lag}")
                    
                    # Update last logged batch ID
                    last_logged_batch_id = current_batch_id
            else:
                logger.info("Waiting for first batch to process...")

    except KeyboardInterrupt:
        logger.info("Stopping streaming queries")
        # Stop all active queries gracefully on KeyboardInterrupt
        for s in spark.streams.active:
            s.stop()
        spark.stop()


if __name__ == "__main__":
    main()
