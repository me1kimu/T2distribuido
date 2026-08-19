import asyncio

from src import metrics_client


def test_send_metric_uses_sanitized_metrics_url(monkeypatch):
    called = {}

    class FakeResponse:
        status_code = 200
        text = ""

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, **kwargs):
            called["url"] = url
            called["kwargs"] = kwargs
            return FakeResponse()

    monkeypatch.setattr(metrics_client, "METRICS_SERVICE_URL", "http://metrics-service:8001/base?x=1")
    monkeypatch.setattr(metrics_client.httpx, "AsyncClient", lambda: FakeClient())

    asyncio.run(metrics_client.send_metric({"event_type": "hit"}))

    assert called["url"] == "http://metrics-service:8001/events"


def test_send_metric_skips_invalid_metrics_url(monkeypatch):
    called = {"post": False}

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, **kwargs):
            called["post"] = True
            return None

    monkeypatch.setattr(metrics_client, "METRICS_SERVICE_URL", "metrics-service:8001")
    monkeypatch.setattr(metrics_client.httpx, "AsyncClient", lambda: FakeClient())

    asyncio.run(metrics_client.send_metric({"event_type": "hit"}))

    assert called["post"] is False
