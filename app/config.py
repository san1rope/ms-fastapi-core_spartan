import os
from logging import Logger
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from fastapi import FastAPI
from httpx import AsyncClient

load_dotenv()


class Config:
    UVICORN_HOST: str = os.getenv("UVICORN_HOST").strip()
    UVICORN_PORT: int = int(os.getenv("UVICORN_PORT").strip())

    DATETIME_FORMAT = os.getenv("DATETIME_FORMAT").strip()
    LOGGING_DIR = Path(os.path.abspath("logs"))
    LOGGER: Optional[Logger] = None

    REST_APP: Optional[FastAPI] = None
    HTTP_CLIENT = AsyncClient(timeout=None)

    REST_API_TOKEN: str = os.getenv("REST_API_TOKEN").strip()
    KAFKA_BOOTSTRAP_IP: str = os.getenv("KAFKA_BOOTSTRAP_IP").strip()
    TG_GATEWAY_URL: str = os.getenv("TG_GATEWAY_URL").strip()
