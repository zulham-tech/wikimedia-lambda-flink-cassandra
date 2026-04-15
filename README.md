# Wikimedia Trending Topics â€” Lambda Architecture | Flink Â· Cassandra Â· Snowflake

> **Type:** Lambda Architecture | **Stack:** Wikimedia SSE + API â†’ Kafka â†’ Flink â†’ Cassandra + Snowflake â†’ Airflow

## Key Metrics
- **True SSE streaming** (Server-Sent Events) â€” no polling
- **10 languages:** EN, ID, DE, FR, ES, JA, ZH, AR, PT, RU
- **Trending score:** human_editsÃ—3 + bot_editsÃ—1 per 10-min window
- **Cassandra TTL 24h** auto-expires stale trending data

## Lambda Architecture
```
Wikimedia SSE Stream     Wikimedia Pageviews API
(live edits)             (top 50 articles/language)
        â†“                           â†“
Kafka [recentchanges]    Kafka [pageviews]
        â†“                           â†“
Apache Flink             PySpark Batch
10-min window            YoY change %
trending_score           â†“
        â†“                Snowflake
Cassandra TTL 24h        ARTICLE_PAGEVIEWS
(speed layer)            (batch layer)
```

## Tech Stack
Python Â· Wikimedia SSE Â· Apache Kafka Â· Apache Flink Â· PySpark Â· Cassandra Â· Snowflake Â· Airflow Â· Docker

## Author
**Ahmad Zulham Hamdan** | [LinkedIn](https://linkedin.com/in/ahmad-zulham-hamdan-665170279) | [GitHub](https://github.com/zulham-tech)
