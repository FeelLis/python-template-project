from confluent_kafka import Message
from loguru import logger
from pydantic import BaseModel

from src.utils.kafka import Consumer, Producer

TOPIC = "template_topic"
USERNAME = "username"
PASSWORD = "password"
BOOTSTRAP_SERVERS = "localhost:9092"


class Item(BaseModel):
    id: str


# Use this arguments if Consumer.only_validated_messages=True and you don't need raw Kafka message.
def handler1(item: Item):
    logger.info(item.id)


# Set flag Consumer.only_validated_messages=False to get raw message even if they cannot be deserialized.
def handler2(raw: Message, item: Item):
    if not item:
        logger.info(raw.value())
    else:
        logger.info(item.id)


if __name__ == "__main__":
    # Producer
    producer = Producer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        username=USERNAME,
        password=PASSWORD,
        default_topic=TOPIC,
        logger=logger,
    )

    producer.produce(
        key="alice", message=Item(id="123"), headers={"is_important": True}
    )

    # Consumer
    consumer = Consumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        username=USERNAME,
        password=PASSWORD,
        logger=logger,
        topics=[TOPIC],
        handler=handler1,  # You can switch to handler2 to test raw message handling
        only_validated_messages=False,  # Set to True to only receive deserialized messages
    )
    # The .run() method should typically be awaited in an asyncio application
    # but based on the previous code, it's a blocking call or designed to be run
    # as an asyncio task.
    # If it's an async method, it should be called with asyncio.run(consumer.run())
    # or asyncio.run(consumer.run_in_background()).
    # Assuming it's meant to be blocking or will be handled by an event loop elsewhere:
    import asyncio

    asyncio.run(consumer.run())  # Assuming consumer.run() is an async method
