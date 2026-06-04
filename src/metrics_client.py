import logging
import httpx
from src.consumer_config import METRICS_SERVICE_URL

logger = logging.getLogger(__name__)

async def send_metric(event: dict) -> None:
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(f"{METRICS_SERVICE_URL}/events", json=event, timeout=2.0)
            if resp.status_code >= 400:
                logger.warning(
                    "Failed to send metric: status=%s url=%s body=%s",
                    resp.status_code,
                    f"{METRICS_SERVICE_URL}/events",
                    resp.text,
                )
    except httpx.TimeoutException as exc:
        logger.warning("Timeout sending metric to %s: %s", METRICS_SERVICE_URL, exc)
    except httpx.RequestError as exc:
        logger.warning("Request error sending metric to %s: %s", METRICS_SERVICE_URL, exc)
    except Exception:
        logger.exception("Unexpected error while sending metric")
