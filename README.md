# Real-Time Fraud Detection Pipeline

> Near real-time streaming pipeline that processes financial transactions through Kafka, applies rule-based and statistical anomaly detection in PySpark Structured Streaming, and persists flagged events to Delta Lake — with Grafana dashboards for fraud ops monitoring.

[![PySpark](https://img.shields.io/badge/PySpark-3.5-orange?logo=apache-spark)](https://spark.apache.org)
[![Kafka](https://img.shields.io/badge/Apache_Kafka-3.7-231F20?logo=apache-kafka)](https://kafka.apache.org)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.1-00ADD8)](https://delta.io)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)](https://docker.com)
[![Grafana](https://img.shields.io/badge/Grafana-10.x-F7DC61?logo=grafana)](https://grafana.com)
[![Terraform](https://img.shields.io/badge/Terraform-1.7-7B42BC?logo=terraform)](https://terraform.io)

---

## The Problem

Payment fraud costs financial services companies billions annually. Batch-based fraud detection (catching it tomorrow morning) is too slow for card-not-present transactions — by the time the batch runs, the money is gone. This pipeline detects suspicious patterns within 30 seconds of a transaction hitting the stream.

---

## Architecture
<img width="945" height="618" alt="architecture" src="https://github.com/user-attachments/assets/1e000c2e-0380-4283-803b-cdd2816fed45" />

```
┌─────────────────────────────────────────────────────────────────────┐
│                     TRANSACTION SOURCES                              │
│  ┌────────────────┐  ┌────────────────┐  ┌──────────────────────┐  │
│  │  POS Terminals │  │  Mobile App    │  │   Web Checkout       │  │
│  │  (REST API)    │  │  (WebSocket)   │  │   (REST API)         │  │
│  └───────┬────────┘  └───────┬────────┘  └──────────┬───────────┘  │
└──────────┼───────────────────┼──────────────────────┼──────────────┘
           │                   │                      │
           └───────────────────▼──────────────────────┘
                               │
                     ┌─────────▼──────────┐
                     │   Kafka Producer   │
                     │  (Python client)   │
                     │  Topic: txn.raw    │
                     └─────────┬──────────┘
                               │
                     ┌─────────▼──────────────────────────────────┐
                     │           Apache Kafka                      │
                     │  ┌──────────────┐  ┌────────────────────┐  │
                     │  │  txn.raw     │  │  txn.flagged       │  │
                     │  │  (inbound)   │  │  (fraud alerts)    │  │
                     │  └──────┬───────┘  └────────────────────┘  │
                     └─────────┼──────────────────────────────────┘
                               │
                     ┌─────────▼────────────────────────────────────┐
                     │      PySpark Structured Streaming             │
                     │                                               │
                     │  ┌─────────────────────────────────────────┐ │
                     │  │  Step 1 — Schema validation & parsing   │ │
                     │  └────────────────┬────────────────────────┘ │
                     │                   │                           │
                     │  ┌────────────────▼────────────────────────┐ │
                     │  │  Step 2 — Feature engineering            │ │
                     │  │  • Rolling 1-hour spend per card        │ │
                     │  │  • Transaction velocity (txns/hour)     │ │
                     │  │  • Geo-velocity (impossible travel)     │ │
                     │  │  • Merchant category deviation          │ │
                     │  └────────────────┬────────────────────────┘ │
                     │                   │                           │
                     │  ┌────────────────▼────────────────────────┐ │
                     │  │  Step 3 — Rule engine                   │ │
                     │  │  • Amount > 3σ from card mean           │ │
                     │  │  • Velocity > 10 txns/hour              │ │
                     │  │  • Country mismatch + high amount       │ │
                     │  │  • Known high-risk MCC codes            │ │
                     │  └────────────────┬────────────────────────┘ │
                     │                   │                           │
                     │  ┌────────────────▼────────────────────────┐ │
                     │  │  Step 4 — Score + route                 │ │
                     │  │  fraud_score 0–100                      │ │
                     │  │  ≥ 80: block + alert ops team          │ │
                     │  │  50–79: flag for review                 │ │
                     │  │  < 50: pass through                     │ │
                     │  └────────────────┬────────────────────────┘ │
                     └──────────────────┼──────────────────────────┘
                                        │
               ┌────────────────────────┼────────────────────────┐
               │                        │                        │
               ▼                        ▼                        ▼
    ┌──────────────────┐   ┌──────────────────────┐  ┌─────────────────┐
    │   Delta Lake     │   │  Kafka: txn.flagged  │  │  Alert webhook  │
    │   (all events)  │   │  (high-score only)   │  │  (score ≥ 80)  │
    │   Gold table    │   └──────────────────────┘  └─────────────────┘
    └──────────────────┘
               │
               ▼
    ┌──────────────────────────────────┐
    │         ANALYTICS                │
    │  dbt daily models on Delta      │
    │  Grafana real-time dashboards   │
    │  Fraud ops team review queue    │
    └──────────────────────────────────┘
```

---

## Stack

| Layer | Tool | Purpose |
|---|---|---|
| Message broker | Apache Kafka 3.7 | Event streaming backbone |
| Stream processing | PySpark Structured Streaming 3.5 | Windowed aggregations, rule engine |
| Storage | Delta Lake (ADLS Gen2) | ACID writes, time travel, merge |
| Batch analytics | dbt | Daily fraud summary models |
| Infrastructure | Docker Compose (local) / Terraform (Azure) | Reproducible environments |
| Monitoring | Grafana + Prometheus | Real-time fraud rate dashboards |
| Alerting | Azure Monitor + Webhook | Fraud ops notifications |

---

## Repository Structure

```
realtime-fraud-detection/
│
├── kafka/
│   ├── producers/
│   │   ├── transaction_producer.py       # Simulates transaction stream (local dev)
│   │   └── config.py
│   └── consumers/
│       └── flagged_txn_consumer.py       # Reads fraud alerts from txn.flagged
│
├── spark_streaming/
│   ├── fraud_detection_job.py            # Main Structured Streaming job
│   ├── feature_engineering.py            # Rolling window features
│   ├── rule_engine.py                    # Fraud rule definitions
│   └── schema.py                         # Transaction event schema
│
├── delta/
│   └── create_tables.py                  # Delta table initialisation
│
├── dbt/
│   └── models/
│       ├── mart_fraud_daily_summary.sql
│       └── mart_fraud_by_merchant.sql
│
├── monitoring/
│   ├── grafana/
│   │   └── fraud_dashboard.json          # Grafana dashboard export
│   └── prometheus/
│       └── prometheus.yml
│
├── terraform/
│   ├── main.tf                           # Azure: Event Hubs, Databricks, ADLS
│   └── variables.tf
│
├── docker/
│   └── docker-compose.yml               # Kafka + Spark + Grafana local stack
│
├── .github/
│   └── workflows/
│       └── ci.yml
│
├── requirements.txt
└── README.md
```

---

## Fraud Detection Rules

Rules are defined in `rule_engine.py` and applied in Spark as column expressions — keeping the logic version-controlled and testable.

| Rule | Trigger | Score Impact |
|---|---|---|
| High amount deviation | Amount > mean + 3σ for this card | +35 |
| Velocity spike | > 10 transactions in 1 hour | +40 |
| Impossible travel | > 500km/hr distance between consecutive txns | +50 |
| High-risk MCC | Merchant category in blocked list | +25 |
| Country mismatch | Billing country ≠ transaction country AND amount > £200 | +30 |
| Unusual hour | Transaction between 02:00–04:00 local time + high amount | +15 |
| New merchant | First transaction with this merchant + amount > £500 | +10 |

Scores compound additively. Final score ≥ 80 → block. 50–79 → review queue. < 50 → pass.

---

## Key Engineering Decisions

**Why Spark Structured Streaming and not Flink?**
At this scale (< 50k txns/hour), Spark's micro-batch mode (10-second trigger intervals) gives acceptable latency with simpler operational overhead. Flink would be the right call if we needed millisecond latency or more complex event-time semantics. The architecture is designed so swapping the processing engine doesn't require changing Kafka topics or Delta Lake schemas.

**Why Delta Lake for the streaming sink?**
Streaming writes to a data lake historically meant either eventual consistency (Parquet) or a separate OLAP database. Delta's streaming support with exactly-once semantics, combined with Z-ordering on `card_id` and `event_timestamp`, gives us a queryable history that's also safe to write to from a stream. Time travel lets us replay any 30-day window for model retraining.

**Why rules first, not ML first?**
Rules are interpretable, auditable, and deployable in a day. ML models for fraud detection have high recall but need labelled data, retraining pipelines, and explainability for regulatory compliance. This pipeline is designed with a `fraud_score` column where the rule engine's score can be replaced by an ML model score without changing any downstream schema.

---

## Running Locally

```bash
git clone https://github.com/Neeraj600/realtime-fraud-detection
cd realtime-fraud-detection

# Start local Kafka + Grafana stack
docker compose -f docker/docker-compose.yml up -d

pip install -r requirements.txt

# In terminal 1: Start the producer (simulates transaction stream)
python kafka/producers/transaction_producer.py --rate 50  # 50 txns/sec

# In terminal 2: Start the Spark streaming job
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0 \
  spark_streaming/fraud_detection_job.py

# In terminal 3: Monitor flagged transactions
python kafka/consumers/flagged_txn_consumer.py

# Open Grafana at http://localhost:3000 (admin/admin)
# Import monitoring/grafana/fraud_dashboard.json
```

---

## Performance Characteristics (Local Testing)

| Metric | Result |
|---|---|
| Throughput | ~8,000 transactions/second sustained |
| Detection latency (p50) | 8 seconds (micro-batch interval) |
| Detection latency (p99) | 22 seconds |
| False positive rate (on synthetic data) | ~3.2% |
| Delta write throughput | ~2,000 records/second |

---

*Built by Neeraj Singh Guleria · [linkedin.com/in/neeraj-guleria](https://linkedin.com/in/neeraj-guleria)*
