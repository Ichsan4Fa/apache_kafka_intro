import json
import time
import uuid

from typing import Any

from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

DEFAULT_BOOTSTRAP_SERVERS = 'localhost:9092'

def json_serializer(data: Any) -> bytes:
    return json.dumps(data).encode('utf-8')

def json_deserializer(data: bytes) -> Any:
    if data is None:
        return None
    return json.loads(data.decode('utf-8'))

def run_new_id() -> str:
    return uuid.uuid4().hex[:8]

def unique_topic_name(base_name: str) -> str:
    return f"{base_name}_{run_new_id()}"

def wait_for_kafkabroker(bootstrap_servers: str, timeout_s: int = 30) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        consumer = None
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=bootstrap_servers,
                consumer_timeout_ms=2000,
                api_version_auto_timeout_ms=2000
            )
            consumer.topics()
            return
        except NoBrokersAvailable:
            time.sleep(1)
        finally:
            if consumer is not None:
                consumer.close()
    raise TimeoutError(f"Kafka broker at {bootstrap_servers} did not become available within {timeout_s} seconds")