import os
import logging
from datetime import datetime
from logging import Logger
from pathlib import Path
from typing import Union

from app.config import Config
from app.api_models import APIError


class Utils:
    STATUS_SUCCESS = "success"
    STATUS_FAIL = "fail"

    @staticmethod
    async def check_auth(token: str):
        if not token:
            raise APIError("Missing Authorization header", "AUTH_MISSING", 401)

        if not token.startswith("Bearer "):
            raise APIError("Invalid Authorization format", "AUTH_BAD_FORMAT", 400)

        if Config.REST_API_TOKEN != token.replace("Bearer ", ""):
            raise APIError("Invalid token", "AUTH_INVALID_TOKEN", 401)

    @staticmethod
    async def add_logging(process_id: int, datetime_of_start: Union[datetime, str]) -> Logger:
        if isinstance(datetime_of_start, str):
            file_dir = datetime_of_start

        elif isinstance(datetime_of_start, datetime):
            file_dir = datetime_of_start.strftime(Config.DATETIME_FORMAT)

        else:
            raise TypeError("datetime_of_start must be str or datetime")

        log_filepath = Path(os.path.abspath(f"{Config.LOGGING_DIR}/{file_dir}/{process_id}.txt"))
        log_filepath.parent.mkdir(parents=True, exist_ok=True)
        log_filepath.touch(exist_ok=True)

        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter(u'%(filename)s:%(lineno)d #%(levelname)-8s [%(asctime)s] - %(name)s - ' + str(
            process_id) + '| %(message)s')

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        file_handler = logging.FileHandler(log_filepath, mode="a", encoding="utf-8")
        file_handler.setFormatter(formatter)

        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

        return logger
