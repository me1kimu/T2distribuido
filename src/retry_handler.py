from aiokafka import AIOKafkaProducer
import json
from src.consumer_config import KAFKA_BOOTSTRAP_SERVERS


class RetryHandler:
    def __init__(self):
        self._producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        self._started = False

    async def start(self):
        import asyncio
        from aiokafka.errors import KafkaConnectionError
        
        retries = 10
        for i in range(retries):
            try:
                await self._producer.start()
                self._started = True
                return
            except KafkaConnectionError as e:
                if i < retries - 1:
                    await asyncio.sleep(5)
                else:
                    try:
                        await self._producer.stop()
                    except Exception:
                        pass
                    raise
            except Exception:
                try:
                    await self._producer.stop()
                except Exception:
                    pass
                raise

    async def stop(self):
        if self._started:
            await self._producer.stop()
            self._started = False

    async def forward(self, topic: str, message: dict):
        await self._producer.send_and_wait(topic, json.dumps(message).encode("utf-8"))
