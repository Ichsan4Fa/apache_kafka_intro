import argparse

from kafka import KafkaConsumer

from kafka_intro.common import DEFAULT_BOOTSTRAP_SERVERS, json_deserializer, unique_group_id

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Continuosly consume demo order events")
    parser.add_argument("--bootstrap-servers", type=str, default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default="order-events-live")
    parser.add_argument(
        "--group-id", 
        default="",
        help="Optional, if empty there is a new group id generated for each run."
    )
    parser.add_argument(
        "--from-beginning",
        action="store_true",
        help="Read from the earliest offset instead of the latest"
    )
    return parser.parse_args()
def main() -> int:
    args = parse_args()
    group_id = args.group_id or unique_group_id("live-topic-consumer")
    offset_policy = "earliest" if args.from_beginning else "latest"

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_servers,
        group_id = group_id,
        auto_offset_reset = offset_policy,
        enable_auto_commit = True,
        value_deserializer = json_deserializer
    )

    print(f"Consumer Topic: {args.topic}")
    print(f"Consumer group : {group_id}")
    print(f"Bootstrap servers: {args.bootstrap_servers}")
    print(f"Offset policy: {offset_policy}")
    print("Consumer is running...")

    try:
        for message in consumer:
            payload = message.value or {}
            print(
                "Consumed Event"
                f"partition = {message.partition} offset = {message.offset} "
                f"order_id = {payload.get('order_id')} status = {payload.get('status')} at = {payload.get('event_time')}"
            )
    except KeyboardInterrupt:
        print("Shutting down consumer...")
    finally:
        consumer.close()
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
