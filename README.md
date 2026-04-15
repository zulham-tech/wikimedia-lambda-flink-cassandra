# Wikimedia Trending Topics - Lambda Architecture | Flink, Cassandra, Snowflake

**Stack:** Wikimedia SSE + API -> Kafka -> Flink -> Cassandra + Snowflake -> Airflow

## Key Metrics
- True SSE streaming (Server-Sent Events), no polling
- 10 languages: EN, ID, DE, FR, ES, JA, ZH, AR, PT, RU
- Trending score: human_edits x 3 + bot_edits x 1 per 10-min window
- Cassandra TTL 24h auto-expires stale trending data

## Lambda Architecture
```
Wikimedia SSE Stream     Wikimedia Pageviews API
(live edits)             (top 50 articles/language)
    |                           |
Kafka [recentchanges]   Kafka [pageviews]
    |                           |
Apache Flink            PySpark Batch
10-min window           YoY change %
trending_score              |
    |                   Snowflake
Cassandra TTL 24h       ARTICLE_PAGEVIEWS
(speed layer)           (batch layer)
```

## Tech Stack
Python, Wikimedia SSE, Apache Kafka, Apache Flink, PySpark, Cassandra, Snowflake, Airflow, Docker

## Author
Ahmad Zulham Hamdan | https://linkedin.com/in/ahmad-zulham-665170279
