#!/usr/bin/env python3
# language: python
"""
Producer with connection retries and clearer errors.
Usage: python producer/producer.py --broker localhost:9092 --topic interactions --rate 1000 --batch 50
"""
import argparse
import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError, NoBrokersAvailable

INTERACTIONS = ['click', 'view', 'purchase', 'like', 'add_to_cart']

def gen_event(user_range=100000, item_range=10000):
    return {
        "user_id": f"user_{random.randint(1, user_range)}",
        "item_id": f"item_{random.randint(1, item_range)}",
        "interaction_type": random.choice(INTERACTIONS),
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }

def create_producer(brokers, retries=5, backoff=3):
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            p = KafkaProducer(
                bootstrap_servers=brokers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                linger_ms=5,
                retries=3,
                request_timeout_ms=20000
            )
            # allow some time to establish metadata
            for _ in range(10):
                if p.bootstrap_connected():
                    print(f"Connected to Kafka brokers: {brokers}")
                    return p
                time.sleep(0.5)
            # if not connected, close and retry
            p.close()
            last_err = NoBrokersAvailable("bootstrap_connected() returned False")
        except Exception as e:
            last_err = e
        print(f"Kafka connection attempt {attempt}/{retries} failed: {last_err}. retrying in {backoff}s...")
        time.sleep(backoff)
    raise RuntimeError(
        f"Failed to connect to Kafka brokers {brokers} after {retries} attempts. "
        "Check that Kafka is running, the port is correct, and `KAFKA_ADVERTISED_LISTENERS` "
        "is reachable from this host."
    )

def run(broker, topic, rate, batch):
    # broker can be comma-separated list
    brokers = [b.strip() for b in broker.split(",") if b.strip()]
    producer = create_producer(brokers, retries=6, backoff=5)
    print(f"Producer started -> brokers={brokers}, topic={topic}, rate={rate}/s, batch={batch}")
    try:
        while True:
            start = time.time()
            for _ in range(batch):
                evt = gen_event()
                try:
                    producer.send(topic, evt)
                except KafkaTimeoutError as kte:
                    print("KafkaTimeoutError while sending. This usually means metadata couldn't be retrieved.")
                    print("Verify broker address/port and that Kafka's advertised listeners are reachable from your host.")
                    # attempt to flush; if it fails recreate producer
                    try:
                        producer.flush(timeout=10)
                    except Exception:
                        print("Flush failed; attempting to recreate producer connection...")
                        producer.close()
                        producer = create_producer(brokers, retries=3, backoff=3)
                except NoBrokersAvailable as nba:
                    print("NoBrokersAvailable:", nba)
                    producer.close()
                    producer = create_producer(brokers, retries=3, backoff=3)
            producer.flush()
            elapsed = time.time() - start
            target = batch / float(rate) if rate > 0 else 0
            sleep_time = max(0, target - elapsed)
            time.sleep(sleep_time)
    except KeyboardInterrupt:
        print("Producer exiting")
        try:
            producer.flush(timeout=5)
        except Exception:
            pass
        producer.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # default port set to 9092 to match docker-compose-kafka.yml; override if needed
    parser.add_argument("--broker", default="localhost:9092",
                        help="comma-separated Kafka bootstrap brokers (e.g. localhost:9092,other:9092)")
    parser.add_argument("--topic", default="test-topic")
    parser.add_argument("--rate", type=int, default=1000, help="messages per second")
    parser.add_argument("--batch", type=int, default=10, help="messages per batch")
    args = parser.parse_args()
    run(args.broker, args.topic, args.rate, args.batch)
