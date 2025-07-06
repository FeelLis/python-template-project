from __future__ import annotations

import asyncio
import traceback
from functools import lru_cache
from typing import (
    Any,
    Awaitable,
    Callable,
    GenericAlias,
    Iterable,
    TypeVar,
    get_type_hints,
)
from uuid import uuid4

from confluent_kafka import Consumer as KafkaConsumer
from confluent_kafka import KafkaException, TopicPartition
from confluent_kafka import Message as KafkaMessage
from loguru import Logger
from pydantic import BaseModel, PositiveInt, ValidationError

MaybeAwaitable = Awaitable[Any] | None
ConsumerHandler = Callable[[list[KafkaMessage]], MaybeAwaitable]
T = TypeVar("T", bound=ConsumerHandler)


class Consumer:
    """Simplified consumer class to consume messages from Kafka topics."""

    def __init__(
        self,
        bootstrap_servers: str,
        username: str,
        password: str,
        logger: Logger,
        topics: list[str],
        handler: Callable[[Any], MaybeAwaitable] | None = None,
        max_poll_interval_ms: PositiveInt = 10 * 60 * 1000,
        timeout_ms: PositiveInt = 5 * 60 * 1000,
        pre_consume_hook: Callable[[], MaybeAwaitable] | None = None,
        pre_consume_hook_frequency_s: PositiveInt = 10,
        only_validated_messages: bool = True,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.username = username
        self.password = password
        self.max_poll_interval_ms = max_poll_interval_ms
        self.timeout_ms = timeout_ms
        self.consumer: KafkaConsumer = None
        self.handler = handler
        self.topics = topics
        self.pre_consume_hook = pre_consume_hook
        self.pre_consume_hook_frequency_s = pre_consume_hook_frequency_s
        self.only_validated_messages = only_validated_messages
        self.logger = logger
        self._is_connected = False

    def __enter__(self):
        """Initialize connection to bootstrap servers and subscribe to topic."""
        if not self.handler:
            raise RuntimeError("Message handler was not provided.")

        if not self._is_connected:
            self.logger.info("Initializing Kafka consumer...")
            consumer_config = {
                "bootstrap.servers": self.bootstrap_servers,
                "security.protocol": "SASL_PLAINTEXT",
                "sasl.mechanisms": "SCRAM-SHA-256",
                "sasl.username": self.username,
                "sasl.password": self.password,
                "partition.assignment.strategy": "cooperative-sticky",
                "group.id": self.username,  # Using username as group_id for simplicity, adjust if needed
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "max.poll.interval.ms": self.max_poll_interval_ms,
            }
            self.consumer = KafkaConsumer(consumer_config)
            self.consumer.subscribe(
                self.topics,
                on_assign=self._on_assign,
                on_revoke=self._on_revoke,
                on_lost=self._on_lost,
            )
            self._is_connected = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close connection to bootstrap servers."""
        if self._is_connected:
            self.logger.info("Closing consumer service.")
            self.consumer.close()
            self._is_connected = False

    @staticmethod
    @lru_cache(maxsize=None)
    def _get_func_parsed_model_type(func: Callable) -> type[BaseModel]:
        """Identify derived class (child) of base class KafkaMessage (parent)."""
        # Args:
        #     func (Callable): consumer handler.
        # Raises:
        #     TypeError: type was not found / Invalid message schema.
        # Returns:
        #     type[BaseModel]: derived class that inherited from base class BaseModel.

        type_hints = get_type_hints(func, include_extras=True)
        for param_type in type_hints.values():
            if isinstance(param_type, (list, GenericAlias, Iterable)):
                real_type = param_type._origin  # for List[Type] get Type
                if real_type in (list, tuple, set):
                    if len(param_type.__args__) > 0:
                        param_type = param_type.__args__[0]
                    else:
                        continue  # Skip empty generics like List[]
                else:
                    continue  # Not a standard collection
            if issubclass(param_type, BaseModel):
                return param_type
        raise TypeError(
            f"Could not find an argument which is a subclass of {BaseModel.__name__} in function {func.__qualname__}."
        )

    def _deserialize(self, data: str) -> BaseModel | None:
        """
        Deserialize data to BaseModel class.

        Args:
            data (str): value from kafka message. Must be a json to be deserialized.

        Returns:
            type[BaseModel] | None: deserialized class.
        """
        model_type = Consumer._get_func_parsed_model_type(self.handler)
        try:
            return model_type.model_validate_json(data)
        except ValidationError as e:
            self.logger.error(
                f"Failed to deserialize Kafka message to model. "
                f"Message data: {data}, Model type: {model_type.__name__}, Error: {e}"
            )
            return None

    def _log_partition_state(
        self, partitions: list[TopicPartition], state: str
    ) -> None:
        """Log assign, revoke and lost topic partitions."""
        if partitions:
            self.logger.warning(
                "Partitions (state): {topic_partition}".format(
                    topic_partition=", ".join(
                        f"{i.topic}:{i.partition}" for i in partitions
                    )
                )
            )

    def _on_assign(
        self, consumer: KafkaConsumer, partitions: list[TopicPartition]
    ) -> None:
        self._log_partition_state(partitions, "assigned")

    def _on_revoke(
        self, consumer: KafkaConsumer, partitions: list[TopicPartition]
    ) -> None:
        self._log_partition_state(partitions, "revoked")

    def _on_lost(
        self, consumer: KafkaConsumer, partitions: list[TopicPartition]
    ) -> None:
        self._log_partition_state(partitions, "lost")

    async def _execute_pre_consume_hook(
        self, pre_consume_hook: Callable[[], MaybeAwaitable] | None
    ) -> bool:
        """
        Function to execute before consuming.

        Args:
            pre_consume_hook (Callable[[], MaybeAwaitable] | None): function to execute, can be async or sync.

        Returns:
            bool: True if function succeeded, else False
        """
        if pre_consume_hook is None:
            return True
        if asyncio.iscoroutinefunction(pre_consume_hook):
            await pre_consume_hook()
        else:
            pre_consume_hook()
        return True  # Assuming it always succeeds if executed

    async def _execute_handler(self, raw_message: KafkaMessage) -> None:
        """Execute handler that subscribed to topic."""
        # Args:
        #     raw_message (KafkaMessage): raw message from kafka.
        parsed_message = self._deserialize(
            raw_message.value().decode("utf-8")
        )  # Assuming UTF-8 decoding
        if self.only_validated_messages and parsed_message is None:
            return

        with self.logger.contextualize(bulk_id=uuid4()):
            self.logger.info("Starting to process message.")
            try:
                if asyncio.iscoroutinefunction(self.handler):
                    await self.handler(
                        raw_message, parsed_message
                    )  # Assuming handler can take raw and parsed
                else:
                    self.handler(
                        raw_message, parsed_message
                    )  # Assuming handler can take raw and parsed
                self.logger.success("Finished to process message.")
            except Exception:
                self.logger.error(
                    "Finished to process message with error.",
                    traceback=traceback.format_exc(),
                )
                raise

    async def _consume(self) -> None:
        """Execute pre consume hook, consume new message and store it's offset."""

        def blocking_poll() -> KafkaMessage:
            return self.consumer.poll(
                timeout=self.timeout_ms / 1000.0
            )  # Convert ms to s

        if not await self._execute_pre_consume_hook(self.pre_consume_hook):
            await asyncio.sleep(self.pre_consume_hook_frequency_s)
            return

        message: KafkaMessage = await asyncio.to_thread(blocking_poll)

        if message is None:
            return

        if message.error():
            raise KafkaException(message.error())
        else:
            await self._execute_handler(message)
            self.consumer.store_offsets(
                message
            )  # stored offset are auto committed by background thread.

    async def run(self) -> None:
        """Run consumer."""
        while self._is_connected:
            try:
                await self._consume()
            except KeyboardInterrupt:
                self.logger.warning("Aborted by user.")
                break
            except Exception:
                self.logger.exception(
                    "Failed to consume messages.", traceback=traceback.format_exc()
                )

    async def run_in_background(self) -> asyncio.Task:
        """
        Create and run consume function as async task.

        Returns:
            asyncio.Task: consume function task.
        """
        return asyncio.create_task(self.run())
