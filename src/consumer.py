import json
import asyncio
import time
import httpx
import logging
import sys
import redis.asyncio as aioredis  # type: ignore
from aiokafka import AIOKafkaConsumer  # type: ignore
from aiokafka.errors import KafkaConnectionError
from src.consumer_config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    RETRY_TOPIC,
    DLQ_TOPIC,
    CONSUMER_GROUP,
    REDIS_URL,
    RESPONSE_SERVICE_URL,
    METRICS_SERVICE_URL,
    MAX_RETRIES,
    CACHE_TTL,
)
from src.retry_handler import RetryHandler
from src.metrics_client import send_metric
from src.engine import build_cache_key
from src.schemas import QueryRequest

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("consumer")


def _decode_message(msg_value):
    if isinstance(msg_value, (bytes, bytearray)):
        return json.loads(msg_value.decode("utf-8"))
    if isinstance(msg_value, str):
        return json.loads(msg_value)
    if isinstance(msg_value, dict):
        return msg_value
    raise TypeError(f"Unsupported Kafka message type: {type(msg_value)!r}")


def _extract_payload(raw_msg: dict) -> dict:
    for key in ("payload", "query", "request", "data"):
        value = raw_msg.get(key)
        if isinstance(value, dict):
            return value
    return raw_msg


def _metric(event_type: str, query_type: str, latency_ms: float, retry_count: int = 0) -> dict:
    payload = {
        "event_type": event_type,
        "query_type": query_type,
        "latency_ms": latency_ms,
    }
    if retry_count:
        payload["retry_count"] = retry_count
    return payload

async def process_message(msg_value, redis_client, retry_handler, http_client):
    try:
        raw_msg = _decode_message(msg_value)
    except Exception:
        return

    msg_id = raw_msg.get("id") or raw_msg.get("message_id") or raw_msg.get("query_id") or "unknown"
    retry_count = int(raw_msg.get("retry_count", 0))
    created_at = raw_msg.get("created_at") or time.time()

    payload_data = _extract_payload(raw_msg)
    try:
        query = QueryRequest.model_validate(payload_data)
    except Exception:
        return

    key = build_cache_key(query)
    start_time = time.perf_counter()
    cached = await redis_client.get(key)
    
    if cached:
        latency_ms = (time.perf_counter() - start_time) * 1000
        await send_metric(_metric("hit", query.query_type, latency_ms))
        if retry_count > 0:
            await send_metric(_metric("recovery", query.query_type, latency_ms, retry_count))
            logger.info(json.dumps({"event_type": "recovery", "query_type": query.query_type, "id": msg_id}))
        return

    query_payload = query.model_dump()
    
    try:
        r = await http_client.post(f"{RESPONSE_SERVICE_URL}/compute", json=query_payload, timeout=5.0)
        r.raise_for_status()
        
        latency_ms = (time.perf_counter() - start_time) * 1000
        result = r.json().get("result", {})
        await redis_client.set(key, json.dumps(result), ex=CACHE_TTL)
        
        await send_metric(_metric("miss", query.query_type, latency_ms))
        if retry_count > 0:
            await send_metric(_metric("recovery", query.query_type, latency_ms, retry_count))
            logger.info(json.dumps({"event_type": "recovery", "query_type": query.query_type, "id": msg_id}))
            
    except Exception as e:
        logger.error(f"Error processing query {msg_id}: {e}", exc_info=True)
        failure_latency_ms = (time.perf_counter() - start_time) * 1000
        retry_count += 1
        await send_metric(_metric("retry", query.query_type, failure_latency_ms, retry_count))
        next_msg = {
            "id": msg_id,
            "retry_count": retry_count,
            "created_at": created_at,
            "payload": query_payload,
            "last_attempt_at": time.time()
        }
        
        if retry_count >= MAX_RETRIES:
            await send_metric(_metric("dlq", query.query_type, failure_latency_ms, retry_count))
            await retry_handler.forward(DLQ_TOPIC, next_msg)
            logger.info(json.dumps({"event_type": "dlq", "query_type": query.query_type, "id": msg_id}))
        else:
            await retry_handler.forward(RETRY_TOPIC, next_msg)
            logger.info(json.dumps({"event_type": "retry", "query_type": query.query_type, "id": msg_id, "retry_count": retry_count}))


async def run():
    redis_client = aioredis.from_url(REDIS_URL)
    retry_handler = RetryHandler()
    consumer = None
    retry_started = False
    consumer_started = False
    
    try:
        await retry_handler.start()
        retry_started = True

        consumer = AIOKafkaConsumer(
            KAFKA_TOPIC,
            RETRY_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=CONSUMER_GROUP,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
        retries = 10
        for i in range(retries):
            try:
                await consumer.start()
                consumer_started = True
                break
            except KafkaConnectionError as e:
                logger.warning(f"Failed to connect to Kafka (attempt {i+1}/{retries}): {e}. Retrying in 5 seconds...")
                if i < retries - 1:
                    await asyncio.sleep(5)
                else:
                    raise

        logger.info("Waiting for response-service and metrics-service to be ready...")
        async with httpx.AsyncClient() as init_client:
            for i in range(15):
                try:
                    r1 = await init_client.get(f"{RESPONSE_SERVICE_URL}/health", timeout=2.0)
                    r1.raise_for_status()
                    r2 = await init_client.get(f"{METRICS_SERVICE_URL}/health", timeout=2.0)
                    r2.raise_for_status()
                    logger.info("Dependent services are ready!")
                    break
                except Exception as exc:
                    logger.warning(f"Services not ready yet (attempt {i+1}/15): {exc}. Retrying in 5 seconds...")
                    if i < 14:
                        await asyncio.sleep(5)
                    else:
                        logger.error("Dependent services failed to become ready.")
                        raise

        async with httpx.AsyncClient(limits=httpx.Limits(max_connections=100, max_keepalive_connections=20)) as http_client:
            async for msg in consumer:
                await process_message(msg.value.decode("utf-8"), redis_client, retry_handler, http_client)
                await consumer.commit()
    except Exception as exc:
        logger.error("No se pudo iniciar el consumidor Kafka: %s", exc)
        raise
    finally:
        if consumer_started and consumer is not None:
            await consumer.stop()
        if retry_started:
            await retry_handler.stop()


def main() -> None:
    try:
        asyncio.run(run())
    except KafkaConnectionError:
        sys.exit(1)


if __name__ == "__main__":
    main()
