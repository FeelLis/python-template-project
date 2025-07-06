import sys
from datetime import datetime
from typing import Literal

from loguru import logger

from .async_logstash_handler import init_async_logstash_handler

DEFAULT_LOG_FILE_PATH = f"./logs/{datetime.now().strftime('%Y-%m-%d')}.log"
DEFAULT_LOGSTASH_FILE_PATH = "./logs/logstash.db"


def init_logger_handlers(
    min_log_level: Literal[
        "TRACE", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"
    ] = "INFO",
    async_logstash_handler: bool = False,
    local_file_handler: bool = False,
    logstash_host: str | None = None,
    logstash_port: int | None = None,
    elastic_index: str | None = None,
    logstash_file_path: str = DEFAULT_LOGSTASH_FILE_PATH,
    local_file_path: str = DEFAULT_LOG_FILE_PATH,
    **kwargs,
) -> None:
    """
    Initialize logger based on Loguru with async Logstash handler.

    Args:
        min_log_level (str): minimal log level. Defaults to INFO.
        async_logstash_handler (bool, optional): flag to initialize logstash handler. Defaults to False.
        local_file_handler (bool, optional): flag to initialize local file handler. Defaults to False.
        logstash_host (str, optional): Logstash host. Defaults to None.
        logstash_port (int, optional): Logstash port. Defaults to None.
        elastic_index (str, optional): elastic index to write logs. Defaults to None.
        logstash_file_path (str, optional): Path to Logstash temp file of type '.db'.
            That logger will write logs into before sending to Logstash. Defaults to DEFAULT_LOGSTASH_FILE_PATH.
        local_file_path (str, optional): Path to local log files in debug mode. Defaults to DEFAULT_LOG_FILE_PATH.
        kwargs (Any): Other arguments to pass to handlers, for example default extra fields to logstash.
    """
    logger.info("Initializing handlers for logger.")
    logger.remove()

    # Colorized output to console.
    logger.add(sys.stderr, level=min_log_level)

    # Logs to local file.
    if local_file_handler and not local_file_path:
        logger.warning(
            "Local file path was not provided to initialize local file handler."
        )
    # elif local_file_handler:
    #     init_local_file_handler(local_file_path, min_log_level=min_log_level)

    # Logs to logstash.
    if async_logstash_handler and None in (logstash_host, logstash_port, elastic_index):
        logger.warning("Not enough params to initialize Logstash handler.")
    elif async_logstash_handler:
        init_async_logstash_handler(
            logstash_host=logstash_host,
            logstash_port=logstash_port,
            elastic_index=elastic_index,
            logstash_file_path=logstash_file_path,
            min_log_level=min_log_level,
            **kwargs,
        )

    logger.success("Finished logger's handlers initialization.")
