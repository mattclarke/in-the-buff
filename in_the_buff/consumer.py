import time

from confluent_kafka import Consumer as KafkaConsumer
from confluent_kafka import TopicPartition
from confluent_kafka.cimpl import KafkaException


class Consumer:
    def __init__(self, brokers, topic):
        self.consumer = KafkaConsumer(
            {
                "bootstrap.servers": brokers,
                "group.id": f"in-the-buff-{time.time_ns()}",
                "auto.offset.reset": "latest",
            }
        )
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
