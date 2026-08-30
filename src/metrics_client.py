import logging
from urllib.parse import urlparse

import httpx
from src.consumer_config import METRICS_SERVICE_URL

logger = logging.getLogger(__name__)

_ALLOWED_METRICS_HOSTS = {"localhost", "127.0.0.1", "::1", "metrics-service"}


def _build_metrics_events_url() -> str | None:
    parsed = urlparse(METRICS_SERVICE_URL)
    hostname = parsed.hostname

    if parsed.scheme not in {"http", "https"} or not hostname:
        logger.warning("Invalid METRICS_SERVICE_URL configured: %s", METRICS_SERVICE_URL)
        return None

    if hostname not in _ALLOWED_METRICS_HOSTS:
        logger.warning("Disallowed METRICS_SERVICE_URL host configured: %s", hostname)
        return None

    netloc = hostname if parsed.port is None else f"{hostname}:{parsed.port}"
    return f"{parsed.scheme}://{netloc}/events"


async def send_metric(event: dict) -> None:
    metrics_events_url = _build_metrics_events_url()
    if metrics_events_url is None:
        return

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(metrics_events_url, json=event, timeout=2.0)
            if resp.status_code >= 400:
                logger.warning(
                    "Failed to send metric: status=%s url=%s body=%s",
                    resp.status_code,
                    metrics_events_url,
                    resp.text,
                )
    except httpx.TimeoutException as exc:
        logger.warning("Timeout sending metric to %s: %s", METRICS_SERVICE_URL, exc)
    except httpx.RequestError as exc:
        logger.warning("Request error sending metric to %s: %s", METRICS_SERVICE_URL, exc)
    except Exception:
        logger.exception("Unexpected error while sending metric")
