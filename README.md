<div align="center">

# 🚀 Real-Time Interaction Analytics Pipeline

[![Python](https://img.shields.io/badge/Python-3.9+-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)](https://python.org)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-000?style=for-the-badge&logo=apachekafka)](https://kafka.apache.org)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-FDEE21?style=for-the-badge&logo=apachespark&logoColor=black)](https://spark.apache.org)
[![MongoDB](https://img.shields.io/badge/MongoDB-%234ea94b.svg?style=for-the-badge&logo=mongodb&logoColor=white)](https://mongodb.com)
[![Streamlit](https://img.shields.io/badge/Streamlit-%23FE4B4B.svg?style=for-the-badge&logo=streamlit&logoColor=white)](https://streamlit.io)
[![Docker](https://img.shields.io/badge/Docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)](https://docker.com)

**An end-to-end real-time data pipeline that ingests user interaction events, processes them with Spark Structured Streaming via Kafka, stores windowed aggregations in MongoDB, and visualizes live insights on a Streamlit dashboard.**

</div>

---

## 📐 Architecture

```
┌──────────────┐     ┌─────────────────┐     ┌──────────────────────────┐     ┌───────────────┐     ┌─────────────────────┐
│   Producer   │────▶│   Apache Kafka  │────▶│  Spark Structured        │────▶│    MongoDB    │────▶│  Streamlit          │
│              │     │                 │     │  Streaming Consumer       │     │   kpi_db      │     │  Dashboard          │
│ Generates    │     │ test-topic      │     │                          │     │               │     │                     │
│ interaction  │     │ Streaming       │     │ Windowed Aggregations    │     │ user_inter-   │     │ Real-time KPIs      │
│ events at    │     │ backbone        │     │ (10min window /          │     │ actions       │     │ Top-N Charts        │
│ configurable │     │                 │     │  5min slide)             │     │ item_inter-   │     │ Auto-refresh        │
│ rate         │     │                 │     │                          │     │ actions       │     │                     │
└──────────────┘     └─────────────────┘     └──────────────────────────┘     └───────────────┘     └─────────────────────┘
```

**Data Flow:**
1. **Producer** generates synthetic user interaction events at a configurable rate
2. **Kafka** acts as the fault-tolerant streaming backbone with decoupled pub/sub
3. **Spark Consumer** performs windowed aggregations with watermarking for late data
4. **MongoDB** stores pre-aggregated results for fast dashboard reads
5. **Streamlit Dashboard** auto-refreshes with live metrics and interactive charts

---

## 🖥️ Dashboard Preview

### KPI Metrics & Live Row Counts
![KPI Overview](screenshots/dashboard_3.png)

### Top Users & Top Items Charts
![Charts View](screenshots/dashboard_1.png)

### Raw Aggregated Data Tables
![Raw Data Tables](screenshots/dashboard_2.png)

<!--REMOVE_BELOW-->

---

## 📁 Project Structure

```
realtime_data_pipeline/
├── producer/
│   └── producer.py                        # Kafka producer — generates interaction events
├── consumer/
│   └── consumer.py                        # Spark Structured Streaming consumer
├── reporting/
│   └── reporting.py                       # Streamlit real-time dashboard
├── storage/
│   └── checkpoint/                        # Spark checkpoint data (auto-created)
│       ├── user_interactions_checkpoint/
│       └── item_interactions_checkpoint/
├── clean_checkpoint/
│   ├── cleanup_checkpoints.py             # Python checkpoint cleanup utility
│   └── cleanup_checkpoint.sh             # Shell checkpoint cleanup utility
├── Docker/
│   └── docker-compose-kafka.yml          # Kafka + Zookeeper Docker Compose
└── README.md
```

---

## 🔧 Tech Stack

| Layer | Technology |
|---|---|
| Event Generation | Python (custom producer) |
| Message Streaming | Apache Kafka |
| Stream Processing | Apache Spark 4.1.0+ (Structured Streaming) |
| Storage | MongoDB |
| Visualization | Streamlit + Plotly |
| Containerization | Docker / Docker Compose |

---

## 📦 Event Schema

Each interaction event published to Kafka:

```json
{
  "user_id":          "user_12345",
  "item_id":          "item_6789",
  "interaction_type": "click",
  "timestamp":        "2025-12-22T10:30:00.000Z"
}
```

**Interaction Types:** `click` · `view` · `purchase` · `like` · `add_to_cart`

---

## 🗄️ MongoDB Schema

### `user_interactions`
```json
{
  "window_start":       "2025-12-22T10:00:00Z",
  "window_end":         "2025-12-22T10:10:00Z",
  "user_id":            "user_12345",
  "total_interactions": 42
}
```

### `item_interactions`
```json
{
  "window_start":       "2025-12-22T10:00:00Z",
  "window_end":         "2025-12-22T10:10:00Z",
  "item_id":            "item_6789",
  "total_interactions": 156
}
```

> Pre-aggregated schema ensures fast reads for dashboards. Index `window_start`, `window_end`, `user_id`, and `item_id` for optimal performance.

---

## 🚀 Getting Started

### Prerequisites

| Tool | Version |
|---|---|
| Python | 3.9+ |
| Apache Kafka | Local or Docker |
| MongoDB | Local or Docker |
| Apache Spark | 4.1.0+ |
| Java | 8+ (required by Spark) |

### Step 1 — Install Python Dependencies

```bash
pip install streamlit pymongo plotly streamlit-autorefresh kafka-python pyspark
```

### Step 2 — Start MongoDB (Docker)

```bash
docker run -d --name local-mongo \
  -p 27017:27017 \
  -e MONGO_INITDB_ROOT_USERNAME=admin \
  -e MONGO_INITDB_ROOT_PASSWORD=password \
  mongo:latest
```

### Step 3 — Start Kafka (Docker Compose)

```bash
# Start Kafka + Zookeeper
docker-compose -f Docker/docker-compose-kafka.yml up -d

# Verify containers are running
docker ps

# Create the Kafka topic
docker exec -it <kafka_container_id> bash

kafka-topics --create \
  --topic test-topic \
  --partitions 3 \
  --replication-factor 2 \
  --add-config retention.ms=1000,cleanup.policy=delete \
  --bootstrap-server localhost:29092
```

---

## ▶️ Usage

### 1. Start the Producer

```bash
python producer/producer.py \
  --broker localhost:9092 \
  --topic test-topic \
  --rate 1000 \
  --batch 50
```

| Argument | Description | Default |
|---|---|---|
| `--broker` | Kafka bootstrap servers | `localhost:9092` |
| `--topic` | Kafka topic name | `test-topic` |
| `--rate` | Messages per second | `1000` |
| `--batch` | Messages per batch | `10` |

```bash
# Example: 500 events/sec in batches of 25
python producer/producer.py --broker localhost:9092 --topic test-topic --rate 500 --batch 25
```

---

### 2. Start the Consumer

```bash
python consumer/consumer.py \
  --brokers localhost:9092 \
  --topic test-topic \
  --checkpoint-base ./storage/checkpoint \
  --starting-offsets earliest \
  --mongo-uri "mongodb://admin:password@localhost:27017/?authSource=admin"
```

| Argument | Description | Default |
|---|---|---|
| `--brokers` | Kafka bootstrap servers | `localhost:9092` |
| `--topic` | Kafka topic | `test-topic` |
| `--checkpoint-base` | Spark checkpoint directory | `./storage/checkpoint` |
| `--starting-offsets` | `earliest` or `latest` | `earliest` |
| `--mongo-uri` | MongoDB connection URI | `mongodb://admin:password@localhost:27017/` |
| `--kafka-package` | Kafka connector | `org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0` |
| `--mongo-package` | MongoDB connector | `org.mongodb.spark:mongo-spark-connector_2.13:10.4.0` |

**Aggregation Configuration:**

| Parameter | Value |
|---|---|
| Window Size | 10 minutes |
| Slide Interval | 5 minutes |
| Watermark | 10 minutes (handles late-arriving data) |

---

### 3. Launch the Dashboard

```bash
streamlit run reporting/reporting.py
```

Dashboard available at **`http://localhost:8501`**

**Dashboard Features:**
- ⚡ Auto-refresh (2–30 second configurable interval)
- 📊 KPI cards: avg / max / min interactions
- 🏆 Top-N users by total interactions (interactive slider)
- 📦 Top-N items for the latest time window
- 🗂️ Expandable raw aggregated data tables

---

### 4. Clean Up Checkpoints (Optional)

```bash
# Recommended: Python script (safely preserves recent commits)
python clean_checkpoint/cleanup_checkpoints.py

# Basic: Shell script
bash clean_checkpoint/cleanup_checkpoint.sh
```

> ⚠️ **Always stop the Spark streaming consumer before running cleanup scripts** to avoid checkpoint corruption.

---

## 🔍 Kafka Useful Commands

```bash
# List topics
kafka-topics --list --bootstrap-server localhost:29092

# Describe topic
kafka-topics --describe --topic test-topic --bootstrap-server localhost:29092

# Produce test messages manually
kafka-console-producer --topic test-topic --bootstrap-server localhost:29092

# Consume messages from beginning
kafka-console-consumer --topic test-topic --from-beginning --bootstrap-server localhost:29092
```

---

## 🛠️ Troubleshooting

<details>
<summary><b>Producer — Cannot connect to Kafka broker</b></summary>

- Verify Kafka is running: `docker ps`
- Check `KAFKA_ADVERTISED_LISTENERS` in your Docker Compose file
- Ensure port `9092` is accessible
- For `KafkaTimeoutError`: verify topic exists or auto-creation is enabled
</details>

<details>
<summary><b>Consumer — Spark cannot find Kafka/MongoDB connector</b></summary>

- Verify `--kafka-package` and `--mongo-package` match your Spark version
- Check Spark version compatibility: Spark 4.1.0 requires `_2.13` suffix connectors
- Ensure `JAVA_HOME` and `SPARK_HOME` are set correctly
</details>

<details>
<summary><b>Consumer — Checkpoint errors</b></summary>

- Clear checkpoint directory if starting fresh (resets Kafka offsets)
- Ensure checkpoint directory has write permissions
- Run cleanup scripts to manage disk usage
- Always stop the consumer before cleanup
</details>

<details>
<summary><b>Consumer — Increasing lag</b></summary>

- Monitor lag in consumer logs
- Increase Spark executor memory/cores
- Consider increasing Kafka partitions for better parallelism
</details>

<details>
<summary><b>Dashboard — No data displayed</b></summary>

- Verify MongoDB has data: connect via MongoDB Compass or `mongosh`
- Check MongoDB URI in `reporting/reporting.py`
- Ensure consumer is running and actively writing to MongoDB
</details>

---

## ⚙️ Extending the Pipeline

### Add New Aggregations
1. Create a new aggregation DataFrame in `consumer/consumer.py`
2. Start a new streaming query writing to a new MongoDB collection
3. Add a new chart/metric to `reporting/reporting.py`

### Extend the Event Schema
1. Update `gen_event()` in `producer/producer.py`
2. Update the schema definition in `consumer/consumer.py`
3. Update MongoDB queries and dashboard as needed

---

## 📄 License

[Add your license information here]

---

<div align="center">

*Built with Apache Kafka · Spark Structured Streaming · MongoDB · Streamlit*

</div>
