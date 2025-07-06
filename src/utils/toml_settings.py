import os

from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
)

IS_DOCKER_CONTAINER = bool(os.getenv("DOCKER_CONTAINER", False))
CONFIG_FILE = "/app/config/config.toml" if IS_DOCKER_CONTAINER else "devconfig.toml"


class TomlSettings(BaseSettings):
    model_config = SettingsConfigDict(toml_file=CONFIG_FILE)

    is_docker_container: bool = IS_DOCKER_CONTAINER

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (TomlConfigSettingsSource(settings_cls),)
