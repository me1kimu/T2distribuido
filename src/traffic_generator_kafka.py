from __future__ import annotations

import argparse
import json
import logging
import os
import random
import time
import uuid

from kafka import KafkaProducer
from kafka.errors import KafkaError

from src.config import ZONES

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_QUERIES", "queries")


def _pick_zone(distribution: str, rng: random.Random) -> str:
    """Selecciona zona con distribución uniforme o Zipf."""
    zone_ids = list(ZONES.keys())
    if distribution == "uniform":
        return rng.choice(zone_ids)

    s = 1.2
    ranks = list(range(1, len(zone_ids) + 1))
    weights = [1 / (r**s) for r in ranks]
    total = sum(weights)
    probs = [w / total for w in weights]
    return rng.choices(zone_ids, weights=probs, k=1)[0]


def _generate_query(distribution: str, rng: random.Random) -> dict:
    """Genera una consulta sintética Q1-Q5."""
    query_type = rng.choice(["Q1", "Q2", "Q3", "Q4", "Q5"])

    if query_type == "Q4":
        zone_a, zone_b = rng.sample(list(ZONES.keys()), 2)
        return {
            "query_type": "Q4",
            "zone_id_a": zone_a,
            "zone_id_b": zone_b,
            "confidence_min": round(rng.uniform(0.0, 0.99), 2),
            "bins": rng.randint(5, 20),
        }
    if query_type == "Q5":
        return {
            "query_type": "Q5",
            "zone_id": _pick_zone(distribution, rng),
            "bins": rng.randint(5, 20),
            "confidence_min": round(rng.uniform(0.0, 0.99), 2),
        }
    return {
        "query_type": query_type,
        "zone_id": _pick_zone(distribution, rng),
        "confidence_min": round(rng.uniform(0.0, 0.99), 2),
        "bins": rng.randint(5, 20),
    }


def run(requests_n: int, distribution: str, sleep_ms: int, seed: int) -> None:
    """Publica consultas en Kafka."""
    rng = random.Random(seed)

    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3,
            request_timeout_ms=30000,
        )
    except Exception as exc:
        logger.error("No se pudo conectar a Kafka en %s: %s", KAFKA_BROKER, exc)
        return

    logger.info("Publicando %d consultas en tópico '%s'", requests_n, KAFKA_TOPIC)

    sent = 0
    errors = 0
    start = time.perf_counter()

    try:
        for i in range(requests_n):
            query = _generate_query(distribution, random.Random(seed + i))
            message = {
                "query_id": str(uuid.uuid4()),
                "retry_count": 0,
                "query": query,
                "timestamp": time.time(),
            }

            try:
                producer.send(KAFKA_TOPIC, value=message)
                sent += 1
            except KafkaError as exc:
                logger.error("Error enviando mensaje: %s", exc)
                errors += 1

            if sleep_ms > 0:
                time.sleep(sleep_ms / 1000.0)

    except KeyboardInterrupt:
        logger.info("Generador detenido por usuario")

    finally:
        producer.flush()
        producer.close()

    elapsed = time.perf_counter() - start
    logger.info("Consultas publicadas: %d", sent)
    logger.info("Errores: %d", errors)
    logger.info("Tiempo total: %.2fs", elapsed)
    logger.info("Throughput: %.2f msg/s", sent / elapsed)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generador de tráfico para Kafka")
    parser.add_argument("--requests", type=int, default=200)
    parser.add_argument("--distribution", choices=["uniform", "zipf"], default="uniform")
    parser.add_argument("--sleep-ms", type=int, default=0)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--base-url", default=None)
    parser.add_argument("--metrics-url", default=None)
    args = parser.parse_args()

    run(args.requests, args.distribution, args.sleep_ms, args.seed)


if __name__ == "__main__":
    main()