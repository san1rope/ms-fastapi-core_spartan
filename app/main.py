import asyncio
from contextlib import asynccontextmanager
from datetime import datetime

import uvicorn
from fastapi import FastAPI

from app.config import Config
from app.kafka import KafkaInterface
from app.utils import Utils as Ut


@asynccontextmanager
async def lifespan(app: FastAPI):
    datetime_of_start = datetime.now().strftime(Config.DATETIME_FORMAT)
    logger = await Ut.add_logging(datetime_of_start=datetime_of_start, process_id=0)
    Config.LOGGER = logger

    if not await KafkaInterface().init_producer():
        return

    if not await KafkaInterface().init_consumer():
        return

    loop = asyncio.get_event_loop()
    loop.create_task(KafkaInterface().kafka_response_listener())

    yield

    await KafkaInterface.stop()


if __name__ == "__main__":
    Config.REST_APP = FastAPI(lifespan=lifespan)
    from app import endpoints

    uvicorn.run(Config.REST_APP, host=Config.UVICORN_HOST, port=Config.UVICORN_PORT)
