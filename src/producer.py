from __future__ import annotations

import json
import logging
import os
import time
import uuid

from kafka import KafkaProducer
from kafka.errors import KafkaError

from src.schemas import QueryRequest

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC_QUERIES = os.getenv("KAFKA_TOPIC_QUERIES", "queries")


class QueryProducer:
    """Publica consultas en el tópico principal de Kafka."""

    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",  # Garantía de persistencia
            retries=3,
        )

    def publish_query(self, query: QueryRequest, query_id: str, retry_count: int = 0) -> bool:
        """Publica una consulta en Kafka."""
        payload = {
            "query_id": query_id,
            "retry_count": retry_count,
            "query": query.model_dump(),
            "timestamp": time.time(),
        }

        try:
            future = self.producer.send(KAFKA_TOPIC_QUERIES, value=payload)
            record_metadata = future.get(timeout=10)
            logger.info("Consulta %s publicada en %s", query_id, record_metadata.topic)
            return True
        except KafkaError as exc:
            logger.error("Error publicando consulta %s: %s", query_id, exc)
            return False

    def close(self):
        self.producer.close()


def main() -> None:
    """CLI mínimo para publicar una consulta de ejemplo en Kafka."""
    query = QueryRequest(query_type="Q1", zone_id="Z1", confidence_min=0.5, bins=5)
    producer = QueryProducer()
    try:
        producer.publish_query(query, query_id=str(uuid.uuid4()))
    finally:
        producer.close()


if __name__ == "__main__":
    main()