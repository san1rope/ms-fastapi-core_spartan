import json
import os
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
                value_serializer=lambda v: json.loads(v.decode("utf-8")),
                key_serializer=lambda k: k.encode("utf-8")
            )

            try:
                await cls.PRODUCER.start()
                return True

            except KafkaConnectionError as ex:
                Config.LOGGER.critical(f"Kafka Connection Error! ex: {ex}")
                return False

    @classmethod
    async def init_consumer(cls) -> bool:
        if cls.CONSUMER is None:
            cls.CONSUMER = AIOKafkaConsumer(
                Config.KAFKA_TOPIC_RESPONSES,
                bootstrap_servers=Config.KAFKA_BOOTSTRAP_IP,
                group_id="demo-group",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
            await cls.CONSUMER.start()

    @classmethod
    async def stop(cls):
        if cls.PRODUCER:
            await cls.PRODUCER.stop()

        if cls.CONSUMER:
            await cls.CONSUMER.stop()

    @staticmethod
    async def get_request_type(payload) -> str:
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
                print(f"req_type = {req_type}; payload = {payload}")
                return req_type

        return "None"

    @classmethod
    async def send_message(cls, payload, topic: str = "tg_commands") -> dict:
        if cls.PRODUCER is None:
            raise RuntimeError("Kafka Producer is not initialized. Call initialize() first.")

        request_type = await cls.get_request_type(payload)
        msg_id = str(uuid.uuid4())

        data = payload.model_dump()
        data["request_id"] = msg_id
        data["request_type"] = request_type

        try:
            metadata = await cls.PRODUCER.send_and_wait(
                topic,
                key=msg_id,
                value=data
            )

            return {
                "message_id": msg_id,
                "topic": metadata.topic,
                "partition": metadata.partition,
                "offset": metadata.offset,
            }

        except Exception as e:
            return {"error": str(e)}

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
    async def kafka_response_listener(cls):
        Config.LOGGER.info("Kafka response listener has been started!")

        try:
            async for msg in cls.CONSUMER:
                data = msg.value
                corr_id = data.get("request_id")

                if corr_id in Config.PENDING_REQUESTS:
                    future = Config.PENDING_REQUESTS.pop(corr_id)
                    future.set_result(data)
                    print(f"set result; corr_id = {corr_id}; data = {data}")

        finally:
            await cls.CONSUMER.stop()
