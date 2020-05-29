from in_the_buff.consumer import Consumer


def test_can_create_consumer_and_get_topics():
    consumer = Consumer(["localhost:9092"], "hist_topic1")
    consumer.move_to_oldest()

    data = []
    while not data:
        data = consumer.check_for_messages()

    print(len(data))

    print(consumer.get_last_message())
