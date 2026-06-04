Instalar dependencias:
```bash
pip install -r requirements.txt
```

Entorno soportado:

- Python: 3.12 (probado)

Configurar variables de entorno y ejecutar:
```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 REDIS_URL=redis://localhost:6379/0 python -m src.consumer
```

Productor y generador Kafka:
```bash
python -m src.producer
python -m src.traffic_generator_kafka --requests 200 --distribution uniform
```

Stack completo con Docker:
```bash
docker compose up --build
```

Servicios expuestos:
- `cache-service`: `http://localhost:8000`
- `metrics-service`: `http://localhost:8001`
- `response-service`: `http://localhost:8002`
- Kafka: `localhost:9092`
- Redis: `localhost:6379`

Vídeo de demostración: https://drive.google.com/file/d/