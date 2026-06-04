from aiokafka import AIOKafkaProducer
import json
from src.consumer_config import KAFKA_BOOTSTRAP_SERVERS


class RetryHandler:
    def __init__(self):
        self._producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

    async def start(self):
        await self._producer.start()

    async def stop(self):
        await self._producer.stop()

    async def forward(self, topic: str, message: dict):
        await self._producer.send_and_wait(topic, json.dumps(message).encode("utf-8"))
