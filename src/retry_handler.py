from aiokafka import AIOKafkaProducer
import json
from src.consumer_config import KAFKA_BOOTSTRAP_SERVERS


class RetryHandler:
    def __init__(self):
        self._producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        self._started = False

    async def start(self):
        try:
            await self._producer.start()
        except Exception:
            try:
                await self._producer.stop()
            except Exception:
                pass
            raise
        self._started = True

    async def stop(self):
        if self._started:
            await self._producer.stop()
            self._started = False

    async def forward(self, topic: str, message: dict):
        await self._producer.send_and_wait(topic, json.dumps(message).encode("utf-8"))
