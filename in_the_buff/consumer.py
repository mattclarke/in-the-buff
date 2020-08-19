import time

from confluent_kafka import Consumer as KafkaConsumer
from confluent_kafka import TopicPartition


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
            self.consumer.seek(tp)

    def move_to_oldest(self):
        """
        Move to the oldest message available.
        """
        for tp in self.topic_partitions:
            low, _ = self.consumer.get_watermark_offsets(tp, timeout=1, cached=False)
            tp.offset = low
            self.consumer.seek(tp)

    def move_to_previous(self):
        """
        Move to before the last message already in the topic, so that will be
        always read.
        """
        for tp in self.topic_partitions:
            _, high = self.consumer.get_watermark_offsets(tp, timeout=1, cached=False)
            if high > 0:
                tp.offset = high - 1
                self.consumer.seek(tp)

    def check_for_messages(self):
        msg = self.consumer.poll(0.0)
        if msg is None:
            return []
        if msg.error():
            # TODO: raise this?
            return []
        else:
            return [(msg.timestamp()[1], msg.value())]

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.consumer.close()
