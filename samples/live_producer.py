import argparse
import time

from datetime import datetime, timezone

from kafka_intro.common import DEFAULT_BOOTSTRAP_SERVERS, build_producer

STATUSES =[
    "created",
    "accepted",
    "in_process",
    "completed",
    "on_delivery",
    "delivered",
]

def parse_args()->argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kafka Live Demo Order Events Producer")
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default="order-event-live")
    parser.add_argument("--interval", type=float, default=1.0, help="Interval between events in seconds")
    parser.add_argument("--max-events", type=int, default=0, help="Maximum number of events to produce (0 for unlimited)")
    return parser.parse_args()

def main()->int:
    args = parse_args()
    producer = build_producer(bootstrap_servers=args.bootstrap_servers)

    print(f"Producer Topic: {args.topic}")
    print(f"Producer is running")

    event_count = 0
    order_number = 1

    try:
        while True:
            order_id = f"live order {order_number}"
            for step, status in enumerate(STATUSES, start=1):
                event = {
                    "order_id": order_id,
                    "status": status,
                    "step": step,
                    "restaurant": f"mother eli house",
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
                producer.send(args.topic, value=event)
                producer.flush()

                print(f"Produced event:" 
                      f"{event}")
                
                if args.max_events > 0 and event_count >= args.max_events:
                    print(f"Reached maximum event count of {args.max_events}. Stopping producer.")
                    return 0
                
                time.sleep(args.interval)

            order_number += 1

    except KeyboardInterrupt:
        print("Producer interrupted. Exiting.")
        
    finally:
        producer.close()
    
    return 0

if __name__ == "__main__":
    raise SystemExit(main())