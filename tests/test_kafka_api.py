from in_the_buff.consumer import Consumer


def test_can_create_consumer_and_get_topics():
    consumer = Consumer(["localhost:9092"], "hist_topic1")
    consumer.move_to_oldest()

    data = []
    while not data:
        data = consumer.check_for_messages()

    print(len(data))

    print(consumer.get_last_message())




def test_can_assign_to_partitions():
    consumer = KafkaConsumer(bootstrap_servers=["localhost:9092"])
    topic_partitions = []

    partition_numbers = consumer.partitions_for_topic("hist_topic1")

    for pn in partition_numbers:
        topic_partitions.append(TopicPartition("hist_topic1", pn))

    consumer.assign(topic_partitions)

    consumer.seek_to_end(topic_partitions[0])
    print(consumer.position(topic_partitions[0]))


def test_can_get_last_x_messages():
    consumer = KafkaConsumer(bootstrap_servers=["localhost:9092"])
    topic_partitions = []

    partition_numbers = consumer.partitions_for_topic("hist_topic1")

    for pn in partition_numbers:
        topic_partitions.append(TopicPartition("hist_topic1", pn))

    consumer.assign(topic_partitions)

    end_offsets = consumer.end_offsets(topic_partitions)
    print(end_offsets)

