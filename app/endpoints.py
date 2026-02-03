import asyncio
import uuid
from http import HTTPStatus

from fastapi import Header, Request
from fastapi.responses import JSONResponse, StreamingResponse

from app.api_models import *
from app.config import Config
from app.kafka import KafkaInterface
from app.utils import Utils as Ut


@Config.REST_APP.exception_handler(APIError)
async def api_error_handler(request: Request, exc: APIError):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "status": Ut.STATUS_FAIL,
            "error": exc.error,
            "code": exc.code
        }
    )


@Config.REST_APP.post("/api/v1/messages/send", response_model=SendMessageRequest)
async def send_message(body: SendMessageRequest, authorization: str = Header(...)):
    await Ut.check_auth(authorization)

    result = await KafkaInterface().send_message(payload=body, topic=Config.KAFKA_TOPIC_COMMANDS)
    return await KafkaInterface.response_from_kafka_result(result=result)


@Config.REST_APP.put("/api/v1/messages/{message_id}/edit", response_model=EditMessageRequest)
async def edit_message(message_id: int, body: EditMessageRequest, authorization: str = Header(None)):
    await Ut.check_auth(authorization)

    if message_id != body.message_id:
        raise APIError("message_id in path and body do not match", "MESSAGE_ID_MISMATCH", status_code=400)

    result = await KafkaInterface().send_message(payload=body, topic=Config.KAFKA_TOPIC_COMMANDS)
    return await KafkaInterface.response_from_kafka_result(result=result)


@Config.REST_APP.delete("/api/v1/messages/{message_id}", response_model=DeleteMessageRequest)
async def delete_message(message_id: int, body: DeleteMessageRequest, authorization: str = Header(None)):
    await Ut.check_auth(authorization)

    if message_id != body.message_id:
        raise APIError("message_id in path and body do not match", "MESSAGE_ID_MISMATCH", status_code=400)

    result = await KafkaInterface().send_message(payload=body, topic=Config.KAFKA_TOPIC_COMMANDS)
    return await KafkaInterface.response_from_kafka_result(result=result)


@Config.REST_APP.post("/api/v1/messages/{message_id}/pin", response_model=MessagePinRequest)
async def message_pin(message_id: int, body: MessagePinRequest, authorization: str = Header(None)):
    await Ut.check_auth(authorization)

    if message_id != body.message_id:
        raise APIError("message_id in path and body do not match", "MESSAGE_ID_MISMATCH", status_code=400)

    result = await KafkaInterface().send_message(payload=body, topic=Config.KAFKA_TOPIC_COMMANDS)
    return await KafkaInterface.response_from_kafka_result(result=result)


@Config.REST_APP.post("/api/v1/messages/{message_id}/unpin", response_model=MessageUnpinRequest)
async def message_unpin(message_id: int, body: MessageUnpinRequest, authorization: str = Header(None)):
    await Ut.check_auth(authorization)

    if message_id != body.message_id:
        raise APIError("message_id in path and body do not match", "MESSAGE_ID_MISMATCH", status_code=400)

    result = await KafkaInterface().send_message(payload=body, topic=Config.KAFKA_TOPIC_COMMANDS)
    return await KafkaInterface.response_from_kafka_result(result=result)


@Config.REST_APP.post("/api/v1/messages/send_photo", response_model=SendPhotoRequest)
async def send_photo(body: SendPhotoRequest, authorization: str = Header(None)):
    await Ut.check_auth(authorization)

    result = await KafkaInterface().send_message(payload=body, topic=Config.KAFKA_TOPIC_COMMANDS)
    return await KafkaInterface.response_from_kafka_result(result=result)


@Config.REST_APP.post("/api/v1/messages/send_video", response_model=SendVideoRequest)
async def send_video(body: SendVideoRequest, authorization: str = Header(None)):
    await Ut.check_auth(authorization)

    result = await KafkaInterface().send_message(payload=body, topic=Config.KAFKA_TOPIC_COMMANDS)
    return await KafkaInterface.response_from_kafka_result(result=result)


@Config.REST_APP.post("/api/v1/messages/send_audio", response_model=SendAudioRequest)
async def send_audio(body: SendAudioRequest, authorization: str = Header(None)):
    await Ut.check_auth(authorization)

    result = await KafkaInterface().send_message(payload=body, topic=Config.KAFKA_TOPIC_COMMANDS)
    return await KafkaInterface.response_from_kafka_result(result=result)


@Config.REST_APP.post("/api/v1/messages/send_document", response_model=SendDocumentRequest)
async def send_document(body: SendDocumentRequest, authorization: str = Header(None)):
    await Ut.check_auth(authorization)

    result = await KafkaInterface().send_message(payload=body, topic=Config.KAFKA_TOPIC_COMMANDS)
    return await KafkaInterface.response_from_kafka_result(result=result)


@Config.REST_APP.post("/api/v1/messages/send_sticker", response_model=SendStickerRequest)
async def send_sticker(body: SendStickerRequest, authorization: str = Header(None)):
    await Ut.check_auth(authorization)

    result = await KafkaInterface().send_message(payload=body, topic=Config.KAFKA_TOPIC_COMMANDS)
    return await KafkaInterface.response_from_kafka_result(result=result)


@Config.REST_APP.post("/api/v1/messages/send_voice", response_model=SendVoiceRequest)
async def send_voice(body: SendVoiceRequest, authorization: str = Header(None)):
    await Ut.check_auth(authorization)

    result = await KafkaInterface().send_message(payload=body, topic=Config.KAFKA_TOPIC_COMMANDS)
    return await KafkaInterface.response_from_kafka_result(result=result)


@Config.REST_APP.post("/api/v1/messages/send_gif", response_model=SendGIFRequest)
async def send_gif(body: SendGIFRequest, authorization: str = Header(None)):
    await Ut.check_auth(authorization)

    result = await KafkaInterface().send_message(payload=body, topic=Config.KAFKA_TOPIC_COMMANDS)
    return await KafkaInterface.response_from_kafka_result(result=result)


@Config.REST_APP.post("/api/v1/topics/create", response_model=CreateTopicRequest)
async def create_topic(body: CreateTopicRequest, authorization: str = Header(None)):
    await Ut.check_auth(authorization)

    result = await KafkaInterface().send_message(payload=body, topic=Config.KAFKA_TOPIC_COMMANDS)
    return await KafkaInterface.response_from_kafka_result(result=result)


@Config.REST_APP.put("/api/v1/topics/{topic_id}/edit", response_model=EditTopicRequest)
async def edit_topic(topic_id: int, body: EditTopicRequest, authorization: str = Header(None)):
    await Ut.check_auth(authorization)

    if topic_id != body.topic_id:
        raise APIError("topic_id in path and body do not match", "TOPIC_ID_MISMATCH", status_code=400)

    result = await KafkaInterface().send_message(payload=body, topic=Config.KAFKA_TOPIC_COMMANDS)
    return await KafkaInterface.response_from_kafka_result(result=result)


@Config.REST_APP.delete("/api/v1/topics/{topic_id}", response_model=DeleteTopicRequest)
async def delete_topic(topic_id: int, body: DeleteTopicRequest, authorization: str = Header(None)):
    await Ut.check_auth(authorization)

    if topic_id != body.topic_id:
        raise APIError("topic_id in path and body do not match", "TOPIC_ID_MISMATCH", status_code=400)

    result = await KafkaInterface().send_message(payload=body, topic=Config.KAFKA_TOPIC_COMMANDS)
    return await KafkaInterface.response_from_kafka_result(result=result)


@Config.REST_APP.get("/api/v1/media/{chat_id}/{message_id}/info")
async def media_file_info(chat_id: int, message_id: int, authorization: str = Header(None)):
    await Ut.check_auth(authorization)

    result = await KafkaInterface.send_message(
        topic=Config.KAFKA_TOPIC_COMMANDS,
        payload=MediaFileInfoRequest(chat_id=chat_id, message_id=message_id)
    )

    loop = asyncio.get_running_loop()
    future = loop.create_future()
    print(f"result = {result}")
    request_id = result["message_id"]
    Config.PENDING_REQUESTS[request_id] = future

    try:
        result = await asyncio.wait_for(future, timeout=10)
        print(f"result media_file_info = {result}")

        return JSONResponse(
            status_code=HTTPStatus.OK,
            content=MediaFileInfoResponse(
                status=Ut.STATUS_SUCCESS,
                media_info=MediaInfo(
                    file_type="str",
                    file_name="asd",
                    mime_type="asd",
                    file_size=1000,
                    width=1,
                    height=1,
                    created_at="datetime"
                )
            ).model_dump()
        )

    except TimeoutError:
        Config.PENDING_REQUESTS.pop(request_id)
        return {"temperror": "err"}


@Config.REST_APP.get("/api/v1/media/{chat_id}/{msg_id}")
async def proxy_video_stream(chat_id: int, msg_id: int, range: str = Header(None)):
    offset = 0
    if range:
        try:
            offset = int(range.replace("bytes=", "").split("-")[0])
        except (ValueError, IndexError):
            offset = 0

    async def stream_generator():
        async with Config.HTTP_CLIENT.stream(
                "GET",
                f"{Config.TG_GATEWAY_URL}/internal/stream/{chat_id}/{msg_id}?offset={offset}"
        ) as response:
            if response.status_code != 200:
                return

            async for chunk in response.aiter_bytes(chunk_size=524288):
                yield chunk

    return StreamingResponse(
        stream_generator(),
        media_type="video/mp4",
        headers={
            "Accept-Ranges": "bytes",
            "Content-Type": "video/mp4"
        }
    )
