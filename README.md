# Wikimedia Event Streaming — Lambda Architecture + Flink + Cassandra

![Apache Flink](https://img.shields.io/badge/Apache%20Flink-E6526F?style=flat&logo=apacheflink&logoColor=white)
![Apache Cassandra](https://img.shields.io/badge/Cassandra-1287B1?style=flat&logo=apachecassandra&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=flat&logo=apachekafka&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white)

Lambda Architecture implementation that consumes the public Wikimedia EventStreams (live Wikipedia edits, page views, and recent changes) via Server-Sent Events, processes in real-time with Flink, and persists to Cassandra for low-latency reads.

## Architecture

```mermaid
graph TD
    A[Wikimedia EventStreams<br/>SSE / recentchange] --> B[SSE Consumer<br/>Python]
    B --> C[Apache Kafka<br/>wiki-edits / wiki-pageviews]

    subgraph SL[Speed Layer]
        C --> D[Apache Flink<br/>Real-time Aggregations]
        D --> E[Cassandra<br/>Speed Views]
    end

    subgraph BL[Batch Layer]
        C --> F[S3 Parquet<br/>Raw Archive]
        F --> G[Spark Batch<br/>Historical Views]
        G --> H[Cassandra<br/>Batch Views]
    end

    E --> I[Serving Layer<br/>FastAPI]
    H --> I
```

## Features

- Live consumption of Wikimedia SSE (Server-Sent Events) stream
- Real-time edit rate and page view aggregations per language/project
- Cassandra data model optimized for time-range and user-based queries
- Bot vs. human editor classification
- Vandalism detection based on revert patterns
- Batch layer recalculates historical accuracy nightly

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Source | Wikimedia EventStreams (SSE) |
| Message Bus | Apache Kafka |
| Speed Processing | Apache Flink |
| Storage | Apache Cassandra |
| Batch | PySpark (S3 Parquet) |
| API | FastAPI |

## Prerequisites

- Docker & Docker Compose (8GB+ RAM)
- Python 3.10+
- No external API keys needed (Wikimedia is public)

## Quick Start

```bash
git clone https://github.com/zulham-tech/wikimedia-lambda-flink-cassandra.git
cd wikimedia-lambda-flink-cassandra
docker compose up -d
python consumers/sse_to_kafka.py  # starts consuming Wikipedia edits
# Flink UI: http://localhost:8081
```

## Project Structure

```
.
├── consumers/           # SSE → Kafka producers
├── flink_jobs/          # Speed layer Flink jobs
├── batch/               # PySpark batch recalculation
├── cassandra/           # CQL schemas for speed & batch views
├── serving/             # FastAPI serving layer
├── docker-compose.yml
└── requirements.txt
```

## Author

**Ahmad Zulham** — [LinkedIn](https://linkedin.com/in/ahmad-zulham-665170279) | [GitHub](https://github.com/zulham-tech)
