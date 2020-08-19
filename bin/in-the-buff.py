import argparse
import datetime
import os
import pprint
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from in_the_buff.consumer import Consumer
from in_the_buff.deserialiser import Deserialiser, UnknownSchemaException


def print_message(timestamp, message, schema="unknown", spacer=True):
    if spacer:
        print("=" * 80)
    print(f"Local time = {datetime.datetime.now()}")
    print(f"Schema = {schema}")
    readable_timestamp = datetime.datetime.fromtimestamp(timestamp // 1000)
    print(f"Message Timestamp = {timestamp} ({readable_timestamp})")
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(message)


def print_exception(message):
    print("=" * 80)
    print(message)


def print_missing_schema(error, timestamp, message):
    print("=" * 80)
    print(f"{error}, but printing anyway...\n")
    print_message(timestamp, message, spacer=False)


def handle_message(message):
    try:
        schema, deserialised_msg = Deserialiser.deserialise(message[1])
        print_message(message[0], deserialised_msg, schema)
    except UnknownSchemaException as error:
        print_missing_schema(error, message[0], message[1])
    except Exception as error:
        print_exception(error)


def main(broker, topic, start_from_oldest=False):
    with Consumer(broker, topic) as consumer:
        if start_from_oldest:
            consumer.move_to_oldest()
        else:
            # Always get last message, if available
            consumer.move_to_previous()

        while True:
            messages = consumer.check_for_messages()
            for msg in messages:
                handle_message(msg)
            time.sleep(0.5)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    required_args = parser.add_argument_group("required arguments")
    required_args.add_argument(
        "-b", "--broker", type=str, help="the broker address", required=True
    )

    required_args.add_argument(
        "-t", "--topic", type=str, help="the data topic", required=True
    )

    parser.add_argument(
        "-so",
        "--start-from-oldest",
        action="store_true",
        help="whether to start consuming from oldest message",
    )

    args = parser.parse_args()

    main(args.broker, args.topic, args.start_from_oldest)
