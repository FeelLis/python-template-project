from confluent_kafka import Producer as KafkaProducer
from loguru import Logger
from pydantic import BaseModel


class Producer:
    """Producer class to produce messages to kafka."""

    def __init__(
        self,
        bootstrap_servers: str,
        username: str,
        password: str,
        logger: Logger,
        default_topic: str | None = None,
    ) -> None:
        """Initialize Producer class.

        Args:
            bootstrap_servers (str): Kafka's brokers, can be found in America -> kafka topic description.
            username (str): service name, can be found in America -> kafka service description.
            password (str): service's password, can be found in America -> kafka service description.
            logger (Logger): Logger handler.
            default_topic (str, optional): topic to produce to. Defaults to None.
        """
        self.bootstrap_servers = bootstrap_servers
        self.username = username
        self.password = password
        self.default_topic = default_topic

        producer_config = {
            "bootstrap.servers": self.bootstrap_servers,
            "security.protocol": "SASL_PLAINTEXT",
            "sasl.mechanisms": "SCRAM-SHA-256",
            "sasl.username": self.username,
            "sasl.password": self.password,
        }
        self.producer = KafkaProducer(producer_config)
        self.logger = logger

    def _serialize(self, data: BaseModel) -> str:
        """Serialize data to json string.

        Args:
            data (BaseModel): data to serialize.

        Returns:
            str: json representation of data.
        """
        return data.model_dump_json()

    def _callback(self, err, msg) -> None:
        """Callback function to log message delivery status."""
        if err:
            self.logger.error(f"Message failed delivery: {err}.")
            raise RuntimeError(err)
        else:
            self.logger.success(
                f"Message delivered to {msg.topic()}:{msg.partition()} @{msg.offset()}.",
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
            )

    def produce(
        self,
        message: BaseModel,
        topic: str | None = None,
        key: str | None = None,
        headers: dict | None = None,
    ) -> None:
        """Produce message to Kafka.

        Args:
            message (BaseModel): message to produce (value).
            topic (str, optional): topic to produce to. If not defined the message will be produced to default_topic. Defaults to None.
            key (str, optional): Kafka message key. Defaults to None.
            headers (dict, optional): Kafka message headers. Defaults to None.

        Raises:
            RuntimeError: neither topic nor default_topic were provided.
        """
        if not topic and not self.default_topic:
            raise RuntimeError("Neither topic nor default_topic were provided.")

        target_topic = topic or self.default_topic

        self.logger.info(
            f"Sending message to {target_topic}.",
            key=key,
            headers=headers,
            topic=target_topic,
        )

        self.producer.produce(
            topic=target_topic,
            key=key,
            headers=headers,
            value=self._serialize(message),
            on_delivery=self._callback,
        )

        # Non-blocking poll to trigger callbacks if any messages are sent
        self.producer.poll(0)
