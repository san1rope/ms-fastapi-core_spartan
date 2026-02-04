import asyncio
import json
import os
import traceback
import uuid
from typing import Dict
from http import HTTPStatus

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError
from fastapi.responses import JSONResponse

from app.api_models import *
from app.config import Config
from app.utils import Utils as Ut


class KafkaInterface:
    BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", Config.KAFKA_BOOTSTRAP_IP)
    PRODUCER: Optional[AIOKafkaProducer] = None
    CONSUMER: Optional[AIOKafkaConsumer] = None

    @classmethod
    async def init_producer(cls) -> bool:
        if cls.PRODUCER is None:
            cls.PRODUCER = AIOKafkaProducer(
                bootstrap_servers=cls.BOOTSTRAP,
                enable_idempotence=True,
                acks="all",
                max_batch_size=16384,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8")
            )

            try:
                await cls.PRODUCER.start()
                Config.LOGGER.info("Kafka Producer has been init")

            except KafkaConnectionError as ex:
                Config.LOGGER.critical(f"KafkaInterface.init_producer | Kafka Connection Error! ex: {ex}")
                return False

        return True

    @classmethod
    async def init_consumer(cls) -> bool:
        if cls.CONSUMER is None:
            cls.CONSUMER = AIOKafkaConsumer(
                "tg-responses",
                bootstrap_servers=f"{Config.KAFKA_BOOTSTRAP_IP}:9092",
                group_id="demo-group",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
            try:
                await cls.CONSUMER.start()
                Config.LOGGER.info("Kafka Consumer has been init")

            except KafkaConnectionError as ex:
                Config.LOGGER.critical(f"KafkaInterface.init_consumer | Kafka Connection Error! ex: {ex}")
                return False

        return True

    @classmethod
    async def stop(cls):
        if cls.PRODUCER:
            await cls.PRODUCER.stop()
            Config.LOGGER.info("Kafka Producer has been stop")

        if cls.CONSUMER:
            await cls.CONSUMER.stop()
            Config.LOGGER.info("Kafka Consumer has been stop")

    @staticmethod
    async def get_request_type(payload) -> Optional[str]:
        mapping = {
            SendMessageRequest: "send_message",
            EditMessageRequest: "edit_message",
            DeleteMessageRequest: "delete_message",
            MessagePinRequest: "message_pin",
            MessageUnpinRequest: "message_unpin",
            SendPhotoRequest: "send_photo",
            SendVideoRequest: "send_video",
            SendAudioRequest: "send_audio",
            SendDocumentRequest: "send_document",
            SendStickerRequest: "send_sticker",
            SendVoiceRequest: "send_voice",
            SendGIFRequest: "send_gif",
            CreateTopicRequest: "create_topic",
            EditTopicRequest: "edit_topic",
            DeleteTopicRequest: "delete_topic",
            MediaFileInfoRequest: "media_file_info"
        }

        for model, req_type in mapping.items():
            if isinstance(payload, model):
                return req_type

        return None

    @classmethod
    async def send_message(cls, payload, topic: str) -> dict:
        if cls.PRODUCER is None:
            await cls.init_producer()

        request_type = await cls.get_request_type(payload)
        if request_type is None:
            Config.LOGGER.error("Could not send message, request_type is None!")
            raise Exception("KafkaInterface.send_message, request_type is None!")

        data = payload.model_dump()
        data["request_id"] = str(uuid.uuid4())
        data["request_type"] = request_type

        try:
            metadata = await cls.PRODUCER.send_and_wait(topic, key=data["request_id"], value=data)
            return {
                "message_id": data["request_id"],
                "topic": metadata.topic,
                "partition": metadata.partition,
                "offset": metadata.offset,
            }

        except KafkaConnectionError as ex:
            Config.LOGGER.critical(f"KafkaInterface.send_message | Kafka Connection Error! ex: {ex}")

        except asyncio.TimeoutError as ex:
            pass

        except Exception as ex:
            Config.LOGGER.error(f"Не удалось отправить сообщение! ex:\n{traceback.format_exc()}")
            return {"error": str(ex)}

    @staticmethod
    async def response_from_kafka_result(result: Dict) -> JSONResponse:
        if "error" in result:
            res_data = ResponseData(
                status=Ut.STATUS_FAIL,
                id=None,
                error=result.get("error"),
                code="KAFKA_ERROR"
            )
            status_code = HTTPStatus.INTERNAL_SERVER_ERROR

        else:
            res_data = ResponseData(
                status=Ut.STATUS_SUCCESS,
                id=result["message_id"],
                error=None,
                code=None
            )
            status_code = HTTPStatus.OK

        return JSONResponse(status_code=status_code, content=res_data.model_dump())

    @classmethod
    async def response_listener(cls):
        if not cls.CONSUMER:
            await cls.init_consumer()

        Config.LOGGER.info("Kafka response listener has been started!")

        try:
            async for msg in cls.CONSUMER:
                Config.LOGGER.info(
                    f"New topic message | {msg.topic}:{msg.partition}@{msg.offset} key={msg.key} value = {msg.value}")

                data = msg.value
                corr_id = data.get("request_id")

                if corr_id in Config.PENDING_REQUESTS:
                    future = Config.PENDING_REQUESTS.pop(corr_id)
                    if not future.done():
                        future.set_result(data)

                    data.pop("request_id")

        except KafkaConnectionError as ex:
            Config.LOGGER.critical(f"KafkaInterface().response_listener |  Kafka Connection Error! ex: {ex}")

        except asyncio.TimeoutError as ex:
            pass

        except Exception:
            print(traceback.format_exc())

            Config.LOGGER.warning("Trying to re-init Kafka Consumer...")
            await KafkaInterface().init_consumer()
