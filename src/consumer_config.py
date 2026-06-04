import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "queries")
RETRY_TOPIC = os.getenv("RETRY_TOPIC", "queries_retry")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "queries_dlq")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "query_consumers")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
RESPONSE_SERVICE_URL = os.getenv("RESPONSE_SERVICE_URL", "http://localhost:8002")
METRICS_SERVICE_URL = os.getenv("METRICS_SERVICE_URL", "http://localhost:8001")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
CACHE_TTL = int(os.getenv("CACHE_TTL", "120"))
