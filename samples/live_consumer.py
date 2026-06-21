import argparse

from kafka import KafkaConsumer

from kafka_intro.common import DEFAULT_BOOTSTRAP_SERVERS, build_consumer, json_deserializer, unique_group_id


def parse_args()->argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kafka Live Demo Order Events Consumer")
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default="order-event-live")
    parser.add_argument(
        "--group-id",
        default="",
        help="Optional, if empty there is a unique group id generated for each run"
    )
    parser.add_argument(
        "--from-begining",
        action="store_true",
        help="Whether to consume from the beginning of the topic or only new messages"
    )
    return parser.parse_args()

def main()->int:
    args = parse_args()
    group_id = args.group_id if args.group_id else unique_group_id("live_consumer")
    offset_policy = "earliest" if args.from_begining else "latest"

    consumer = build_consumer(
        topic=args.topic,
        group_id=group_id,
        bootstrap_servers=args.bootstrap_servers,
        auto_offset_reset=offset_policy,
        enable_auto_commit=True,
        value_deserializer=json_deserializer
    )

    print(f"Consumer Topic: {args.topic}")
    print(f"Consumer Group ID: {group_id}")
    print(f"OFFSET POLICY: {offset_policy}")
    print(f"Consumer is running, waiting for events...")

    try:
        for message in consumer:
            payload = message.value or {}
            print(f"Consumed event:" 
                  f"Order id={payload.get('order_id')}, status={payload.get('status')}, step={payload.get('step')}, ")
    except KeyboardInterrupt:
        print("Consumer interrupted, shutting down...")
    finally:
        consumer.close()

    return 0

if __name__ == "__main__":
    raise SystemExit(main())