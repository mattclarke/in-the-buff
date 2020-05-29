from kafka import KafkaConsumer, TopicPartition


class Consumer:
    def __init__(self, brokers, topic):
        self.consumer = KafkaConsumer(bootstrap_servers=brokers)

        if topic not in self.consumer.topics():
            raise Exception("Topic does not exist")

        self.topic_partitions = []

        partition_numbers = self.consumer.partitions_for_topic(topic)

        for pn in partition_numbers:
            self.topic_partitions.append(TopicPartition(topic, pn))

        self.consumer.assign(self.topic_partitions)
        self.consumer.seek_to_end()

    def move_to_latest(self):
        for tp in self.topic_partitions:
            self.consumer.seek_to_end(tp)

    def move_to_oldest(self):
        for tp in self.topic_partitions:
            self.consumer.seek_to_beginning(tp)

    def get_last_message(self):
        end_offsets = self.consumer.end_offsets(self.topic_partitions)

        move_to_new_position = False
        for tp in self.topic_partitions:
            offset = end_offsets[tp]
            if offset > 0:
                move_to_new_position = True
                self.consumer.seek(tp, offset - 1)

        data = None
        if move_to_new_position:
            while not data:
                data = self.check_for_messages()
            # Only want the last message
            data = data[-1]
        return data

    def check_for_messages(self):
        msgs = self.consumer.poll(5)
        data = []
        if msgs:
            for _, records in msgs.items():
                for i in records:
                    data.append((i.timestamp, i.value))
            data.sort(key=lambda x: x[0])
        return data
