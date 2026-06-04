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
