import argparse
import datetime
import os
import pprint
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from in_the_buff.consumer import Consumer
from in_the_buff.deserialiser import Deserialiser, UnknownSchemaException


def print_monitor_message(timestamp, message, schema="unknown", spacer=True):
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
    print_monitor_message(timestamp, message, spacer=False)


def handle_monitor_message(message):
    try:
        schema, deserialised_msg = Deserialiser.deserialise(message[1])
        print_monitor_message(message[0], deserialised_msg, schema)
    except UnknownSchemaException as error:
        print_missing_schema(error, message[0], message[1])
    except Exception as error:
        print_exception(error)


def monitor_topic(broker, start_from_oldest, topic):
    """
    Print any messages in the topic

    :param broker:
    :param start_from_oldest:
    :param topic:
    """
    with Consumer(broker, topic) as consumer:
        if start_from_oldest:
            consumer.move_to_oldest()
        else:
            # Always get last message, if available
            consumer.move_to_previous()

        while True:
            message = consumer.check_for_message()
            if message:
                handle_monitor_message(message)
            time.sleep(0.01)


def query_topic(broker, topic):
    """
    Print the sources and schema in the selected topic.

    :param broker:
    :param topic:
    """
    with Consumer(broker, topic) as consumer:
        consumer.move_to_oldest()

        sources = set()
        unrecognised = set()

        while True:
            message = consumer.check_for_message()
            if message:
                try:
                    schema, deserialised_msg = Deserialiser.deserialise(message[1])
                    source = extract_source(deserialised_msg)
                    if source:
                        if source not in sources:
                            print(f"source: {source} schema: {schema}")
                            sources.add(source)
                    else:
                        if schema not in unrecognised:
                            unrecognised.add(schema)
                            print(f"Could not determine source from schema {schema}")
                except UnknownSchemaException:
                    schema = Deserialiser.get_schema(message[1])
                    if schema not in unrecognised:
                        unrecognised.add(schema)
                        print(f"Did not recognise schema {schema}")
                except Exception as error:
                    print_exception(error)
            time.sleep(0.5)


def extract_source(message):
    # Currently can be a dict or a namedTuple
    if isinstance(message, dict):
        if "source" in message:
            return message["source"]
        elif "source_name" in message:
            return message["source_name"]
        elif "name" in message:
            return message["name"]
    else:
        if "source" in dir(message):
            return message.source
        if "source_name" in dir(message):
            return message.source_name
        elif "name" in dir(message):
            return message.name
    return None


def main(broker, topic, start_from_oldest=False, query=False):
    if query:
        query_topic(broker, topic)
    else:
        monitor_topic(broker, start_from_oldest, topic)


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

    parser.add_argument(
        "-q",
        "--query-mode",
        action="store_true",
        help="query the topic to see what schema and sources are present",
    )

    args = parser.parse_args()

    main(args.broker, args.topic, args.start_from_oldest, args.query_mode)
