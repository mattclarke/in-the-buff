import argparse
import datetime
import os
import pprint
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from in_the_buff.consumer import Consumer, create_sasl_config
from in_the_buff.deserialiser import Deserialiser, UnknownSchemaException


def print_monitor_message(timestamp, message, offset, partition, schema="unknown", spacer=True):
    if spacer:
        print("=" * 80)
    local_time = datetime.datetime.now()
    print(f"Local time = {local_time} {local_time.astimezone().tzname()}")
    print(f"Partition = {partition}")
    print(f"Offset = {offset}")
    print(f"Schema = {schema}")
    readable_timestamp = datetime.datetime.utcfromtimestamp(timestamp // 1000)
    print(f"Message Timestamp = {timestamp} ({readable_timestamp} UTC)")
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(message)


def print_exception(message):
    print("=" * 80)
    print(message)


def print_missing_schema(error, timestamp, message, offset):
    print("=" * 80)
    print(f"{error}, but printing anyway...\n")
    print_monitor_message(timestamp, message, offset, spacer=False)


def handle_monitor_message(message, schema_filter):
    try:
        schema, deserialised_msg = Deserialiser.deserialise(message[1])
        if schema_filter and schema_filter != schema:
            return
        print_monitor_message(message[0], deserialised_msg, message[2], message[3], schema)
    except UnknownSchemaException as error:
        print_missing_schema(error, message[0], message[1], message[2])
    except Exception as error:
        print_exception(error)


def monitor_topic(broker, topic, sasl_config, start_from_oldest, schema_filter):
    """
    Print any messages in the topic

    :param broker:
    :param topic:
    :param sasl_config:
    :param start_from_oldest:
    :param schema_filter:
    """
    with Consumer(broker, topic, sasl_config) as consumer:
        if start_from_oldest:
            consumer.move_to_oldest()
        else:
            # Always get last message, if available
            consumer.move_to_previous()

        while True:
            try:
                message = consumer.check_for_message()
                if message:
                    handle_monitor_message(message, schema_filter)
                time.sleep(0.01)
            except KeyboardInterrupt:
                break


def query_topic(broker, topic, sasl_config, start_from_oldest):
    """
    Print the sources and schema in the selected topic.

    :param broker:
    :param topic:
    :param sasl_config:
    :param start_from_oldest:
    """
    with Consumer(broker, topic, sasl_config) as consumer:
        if start_from_oldest:
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
                        if (source, schema) not in sources:
                            print(f"source: {source} schema: {schema}")
                            sources.add((source, schema))
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


def main(
    broker, topic, sasl_config, start_from_oldest=False, query=False, schema_filter=""
):
    if query:
        query_topic(broker, topic, sasl_config, start_from_oldest)
    else:
        monitor_topic(broker, topic, sasl_config, start_from_oldest, schema_filter)


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

    parser.add_argument(
        "-f",
        "--filter",
        type=str,
        default="",
        help="only show message with this schema.",
    )

    parser.add_argument(
        "-prot",
        "--sasl-protocol",
        type=str,
        default="",
        help="the SASL protocol to use.",
    )

    parser.add_argument(
        "-mech",
        "--sasl-mechanism",
        type=str,
        default="",
        help="the SASL mechanism to use.",
    )

    parser.add_argument(
        "-cert",
        "--cert-path",
        type=str,
        default="",
        help="the path to the certificate file.",
    )

    parser.add_argument(
        "-user",
        "--username",
        type=str,
        default="",
        help="the user name.",
    )

    parser.add_argument(
        "-password",
        "--password",
        type=str,
        default="",
        help="the password.",
    )

    args = parser.parse_args()

    sasl_config = create_sasl_config(
        args.sasl_protocol,
        args.sasl_mechanism,
        args.cert_path,
        args.username,
        args.password,
    )

    main(
        args.broker,
        args.topic,
        sasl_config,
        args.start_from_oldest,
        args.query_mode,
        args.filter,
    )
