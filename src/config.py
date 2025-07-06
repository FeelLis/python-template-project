from pydantic_settings import BaseSettings

from src.utils.toml_settings import TomlSettings


class LogstashConfig(BaseSettings):
    host: str
    port: str
    index: str


class Config(TomlSettings):
    service_name: str
    debug_mode: bool
    logstash: LogstashConfig


config = Config()
