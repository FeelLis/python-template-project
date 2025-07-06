import time

from src.config import config
from src.utils.logger import logger


def main():
    while True:
        logger.info(f"Hello World! I'm {config.service_name}")
        time.sleep(10)


if __name__ == "__main__":
    main()
