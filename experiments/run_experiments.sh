#!/usr/bin/env bash
set -euo pipefail

mkdir -p results

# Wait for healthy services
wait_services() {
    echo "Waiting for services to be ready..."
    for _ in $(seq 1 30); do
        if docker compose exec -T metrics-service python -c 'import urllib.request; urllib.request.urlopen("http://localhost:8001/health")' 2>/dev/null && \
           docker compose exec -T response-service python -c 'import urllib.request; urllib.request.urlopen("http://localhost:8002/health")' 2>/dev/null; then
            echo "Services are up!"
            return 0
        fi
        sleep 5
    done
    echo "Services did not come up in time."
    exit 1
}

# 1. Base System (Synchronous via Cache Service)
echo "=== 1. Base System (Sync) ==="
docker compose down -v
# Run only sync services
docker compose up -d redis metrics-service response-service cache-service
wait_services
# Run traffic generator against sync service
docker compose run --rm traffic-generator python -m src.traffic_generator --base-url http://cache-service:8000 --metrics-url http://metrics-service:8001/summary --requests 1000 --distribution zipf --sleep-ms 5
docker compose exec -T metrics-service python -c 'import urllib.request; print(urllib.request.urlopen("http://localhost:8001/summary").read().decode("utf-8"))' > results/sync_base.json

# 2. Kafka + 1 Consumer
echo "=== 2. Kafka + 1 Consumer ==="
docker compose down -v
docker compose up -d zookeeper kafka redis metrics-service response-service cache-service
# Start 1 consumer
docker compose up -d --scale consumer=1 consumer
wait_services
docker compose run --rm traffic-generator python -m src.traffic_generator_kafka --requests 1000 --distribution zipf --sleep-ms 5
# Wait for processing
sleep 45
docker compose exec -T metrics-service python -c 'import urllib.request; print(urllib.request.urlopen("http://localhost:8001/summary").read().decode("utf-8"))' > results/kafka_1_consumer.json

# 3. Kafka + Multiple Consumers
echo "=== 3. Kafka + 3 Consumers ==="
docker compose down -v
docker compose up -d zookeeper kafka redis metrics-service response-service cache-service
# Start 3 consumers
docker compose up -d --scale consumer=3 consumer
wait_services
docker compose run --rm traffic-generator python -m src.traffic_generator_kafka --requests 1000 --distribution zipf --sleep-ms 5
# Wait for processing
sleep 45
docker compose exec -T metrics-service python -c 'import urllib.request; print(urllib.request.urlopen("http://localhost:8001/summary").read().decode("utf-8"))' > results/kafka_3_consumers.json

# 4. Temporal Failure
echo "=== 4. Kafka Temporal Failure ==="
docker compose down -v
docker compose up -d zookeeper kafka redis metrics-service response-service cache-service consumer
wait_services
# Start sending traffic
docker compose run --rm -d traffic-generator python -m src.traffic_generator_kafka --requests 1000 --distribution zipf --sleep-ms 10
# Simulate failure by stopping response-service for a bit
sleep 2
echo "Stopping response-service..."
docker compose stop response-service
sleep 15
echo "Starting response-service..."
docker compose start response-service
wait_services
# Wait for processing
sleep 60
docker compose exec -T metrics-service python -c 'import urllib.request; print(urllib.request.urlopen("http://localhost:8001/summary").read().decode("utf-8"))' > results/kafka_temporal_failure.json

# 5. Traffic Spike
echo "=== 5. Kafka Traffic Spike ==="
docker compose down -v
docker compose up -d zookeeper kafka redis metrics-service response-service cache-service consumer
wait_services
# Send spike
docker compose run --rm traffic-generator python -m src.traffic_generator_kafka --requests 3000 --distribution zipf --sleep-ms 0
# Wait for processing
sleep 60
docker compose exec -T metrics-service python -c 'import urllib.request; print(urllib.request.urlopen("http://localhost:8001/summary").read().decode("utf-8"))' > results/kafka_spike.json

echo "Experiments completed."
