import json

from logstash_async.formatter import LogstashFormatter
from logstash_async.handler import AsynchronousLogstashHandler
from loguru import logger


class CustomLogstashFormatter(LogstashFormatter):
    def format(self, record):
        """Format log record as needed.

        Args:
            record (LogRecord): log record.
        """
        message = super().format(record)

        if record.levelno == 25:
            message = message.replace('"Level 25"', '"SUCCESS"')
        message = json.loads(message)
        extra = message.pop("extra", {})
        message.update(extra)

        return json.dumps(message)


def init_async_logstash_handler(
    logstash_host: str,
    logstash_port: str,
    elastic_index: str,
    logstash_file_path: str,
    **kwargs,
):
    logger.info("Initializing async logstash handler for logger.")

    if not logstash_file_path:
        logger.warning("'logstash_file_' was not provided, could lead to loosing logs.")
    elif not logstash_file_path.endswith(".db"):
        logger.error("Logstash file must end with '.db'. For example, 'logstash.db'.")
        return

    async_logstash_handler = AsynchronousLogstashHandler(
        host=logstash_host, port=logstash_port, database_path=logstash_file_path
    )
    async_logstash_handler.setFormatter(
        CustomLogstashFormatter(
            extra_prefix="", extra=kwargs, metadata={"index": elastic_index}
        )
    )

    logger.add(async_logstash_handler, format="{message}")
