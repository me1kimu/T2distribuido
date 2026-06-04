import asyncio

import pytest

from src import consumer as consumer_module


class FakeRedis:
    async def get(self, key):
        return b'{"result": true}'


class FakeRetryHandler:
    def __init__(self):
        self.forward_calls = []

    async def forward(self, topic, message):
        self.forward_calls.append((topic, message))


def test_process_message_accepts_query_envelope_and_emits_recovery_metrics(monkeypatch):
    events = []

    async def fake_send_metric(event):
        events.append(event)

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"result": {"ok": True}}

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, *args, **kwargs):
            return FakeResponse()

    monkeypatch.setattr(consumer_module, "send_metric", fake_send_metric)
    monkeypatch.setattr(consumer_module.httpx, "AsyncClient", lambda: FakeClient())

    redis_client = FakeRedis()
    retry_handler = FakeRetryHandler()
    message = {
        "message_id": "abc-123",
        "retry_count": 1,
        "created_at": 1717000000.0,
        "query": {
            "query_type": "Q1",
            "zone_id": "Z1",
            "confidence_min": 0.4,
            "bins": 10,
        },
    }

    asyncio.run(consumer_module.process_message(message, redis_client, retry_handler))

    assert [event["event_type"] for event in events] == ["hit", "recovery"]
    assert events[1]["retry_count"] == 1
    assert retry_handler.forward_calls == []
