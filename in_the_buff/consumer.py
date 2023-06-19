import time

from confluent_kafka import Consumer as KafkaConsumer
from confluent_kafka import TopicPartition
from confluent_kafka.cimpl import KafkaException


def create_sasl_config(
    protocol=None, mechanism=None, certificate_path=None, username=None, password=None
):
    """Create a SASL config for connecting to Kafka.

    Note that whereas some SASL mechanisms do not require user/password, the
    three we currently support do.

    :return: A dictionary of configuration parameters.
    """
    if not protocol:
        return {}

    supported_security_protocols = ["SASL_PLAINTEXT", "SASL_SSL"]
    supported_sasl_mechanisms = ["PLAIN", "SCRAM-SHA-512", "SCRAM-SHA-256"]

    if protocol not in supported_security_protocols:
        raise Exception(
            f"Security protocol {protocol} not supported, use one of "
            f"{supported_security_protocols}"
        )

    if not mechanism:
        raise Exception(
            f"SASL mechanism must be specified for security protocol {protocol}"
        )
    elif mechanism not in supported_sasl_mechanisms:
        raise Exception(
            f"SASL mechanism {mechanism} not supported, use one of "
            f"{supported_sasl_mechanisms}"
        )

    if not username or not password:
        raise Exception(
            f"Username and password must be provided to use SASL {mechanism}"
        )

    sasl_config = {
        "security.protocol": protocol,
        "sasl.mechanism": mechanism,
        "sasl.username": username,
        "sasl.password": password,
    }

    if certificate_path:
        sasl_config["ssl.ca.location"] = certificate_path

    return sasl_config


class Consumer:
    def __init__(self, brokers, topic, sasl_config):
        default_config = {
            "bootstrap.servers": brokers,
            "group.id": f"in-the-buff-{time.time_ns()}",
            "auto.offset.reset": "latest",
        }
        self.consumer = KafkaConsumer({**default_config, **sasl_config})
        metadata = self.consumer.list_topics(topic)
        if topic not in metadata.topics:
            raise Exception("Topic does not exist")

        self.topic_partitions = [
            TopicPartition(topic, p) for p in metadata.topics[topic].partitions
        ]

        self.consumer.assign(self.topic_partitions)

    def move_to_latest(self):
        """
        Move passed all the current messages and start from any new messages.
        """
        for tp in self.topic_partitions:
            _, high = self.consumer.get_watermark_offsets(tp, timeout=1, cached=False)
            tp.offset = high
            self._seek_position(tp)

    def move_to_oldest(self):
        """
        Move to the oldest message available.
        """
        for tp in self.topic_partitions:
            low, _ = self.consumer.get_watermark_offsets(tp, timeout=1, cached=False)
            tp.offset = low
            self._seek_position(tp)

    def move_to_previous(self):
        """
        Move to before the last message already in the topic, so that will be
        always read.
        """
        for tp in self.topic_partitions:
            _, high = self.consumer.get_watermark_offsets(tp, timeout=1, cached=False)
            if high > 0:
                tp.offset = high - 1
                self._seek_position(tp)

    def _seek_position(self, tp):
        # If the topic has just been assigned then seek might not be possible
        # if the assignment hasn't finished.
        # So just retry until it succeeds.
        seek_done = False
        while not seek_done:
            try:
                self.consumer.seek(tp)
                seek_done = True
            except KafkaException:
                time.sleep(0.5)

    def check_for_message(self):
        msg = self.consumer.poll(timeout=0.5)
        if msg is None:
            return None
        if msg.error():
            # TODO: raise this?
            return None
        else:
            return msg.timestamp()[1], msg.value()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.consumer.close()
