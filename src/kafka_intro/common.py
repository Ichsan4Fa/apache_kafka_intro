import json
import time
import uuid

from typing import Any

from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

DEFAULT_BOOTSTRAP_SERVERS = 'localhost:9092'

# JSON serialize 
def json_serializer(data: Any) -> bytes:
    return json.dumps(data).encode('utf-8')

# JSON deserialize
def json_deserializer(data: bytes) -> Any:
    if data is None:
        return None
    return json.loads(data.decode('utf-8'))

def run_new_id() -> str:
    return uuid.uuid4().hex[:8]

def unique_topic_name(base_name: str) -> str:
    return f"{base_name}_{run_new_id()}"

def unique_group_id(prefix: str) -> str:
    return f"{prefix}_{run_new_id()}"

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

def ensure_topic_exists(
        topic_spec: list[tuple[str, int, int]], 
        bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS
        ) -> None:

    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers,client_id='kafka_intro_admin')
    try:
        new_topic = [
            NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
            for topic_name, num_partitions, replication_factor in topic_spec
        ]
        try:
            admin_client.create_topics([NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor) for topic_name, num_partitions, replication_factor in topic_spec])
        except TopicAlreadyExistsError:
            pass
    finally:
        admin_client.close()

def build_producer(bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=json_serializer,
        acks='all',
        retries=5,
        linger_ms=10
    )

def build_consumer(
        topic:str,
        group_id: str,
        bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS
        ) -> KafkaConsumer:
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        value_deserializer=json_deserializer,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        consumer_timeout_ms=1000
    )

def drain_messages(consumer:KafkaConsumer, expected_count: int, timeout_s: int = 10) -> list[dict[str, Any]]:
    deadline = time.time() + timeout_s
    drained: list[dict[str, Any]] = []

    while time.time() < deadline and len(drained) < expected_count:
        batches = consumer.poll(timeout_ms = 800, max_records = expected_count - len(drained))
        for topic_partition, messages in batches.values():
            for message in messages:
                drained.append(
                    {
                        'topic': message.topic,
                        'partition': message.partition,
                        'offset': message.offset,
                        'key': message.key.decode('utf-8') if message.key else None,
                        'value': message.value
                    }
                )
                if len(drained) == expected_count:
                    return drained
                
                
    raise TimeoutError(f"Expected {expected_count} messages but only drained {len(drained)} within {timeout_s} seconds")