import asyncio

from aiohttp import ClientSession

from app.api_models import MediaFileInfoResponse, MediaInfo
from app.kafka import KafkaInterface


async def send_message():
    url = "http://127.0.0.1:8000/api/v1/messages/send"
    headers = {
        "Content-Type": "application/json",
        "authorization": "Bearer 21jk3h12kj3h"
    }
    data = {
        "chat_id": -1003514949333,
        "text": "new message text",
        "topic_id": 5,
    }

    async with ClientSession() as session:
        async with session.post(url=url, headers=headers, json=data, timeout=10) as response:
            answer = await response.text()
            print(answer)


async def edit_message():
    url = "http://127.0.0.1:8000/api/v1/messages/6/edit"
    headers = {
        "Content-Type": "application/json",
        "authorization": "Bearer 21jk3h12kj3h"
    }
    data = {
        "chat_id": -1003514949333,
        "message_id": 6,
        "text": "new edited text"
    }

    async with ClientSession() as session:
        async with session.put(url=url, headers=headers, json=data, timeout=10) as response:
            answer = await response.text()
            print(answer)


async def delete_message():
    url = "http://127.0.0.1:8000/api/v1/messages/6"
    headers = {
        "Content-Type": "application/json",
        "authorization": "Bearer 21jk3h12kj3h"
    }
    data = {
        "chat_id": -1003514949333,
        "message_id": 6
    }

    async with ClientSession() as session:
        async with session.delete(url=url, headers=headers, json=data, timeout=10) as response:
            answer = await response.text()
            print(answer)


async def message_pin():
    url = "http://127.0.0.1:8000/api/v1/messages/7/pin"
    headers = {
        "Content-Type": "application/json",
        "authorization": "Bearer 21jk3h12kj3h"
    }
    data = {
        "chat_id": -1003514949333,
        "message_id": 7
    }

    async with ClientSession() as session:
        async with session.post(url=url, headers=headers, json=data, timeout=10) as response:
            answer = await response.text()
            print(answer)


async def message_unpin():
    url = "http://127.0.0.1:8000/api/v1/messages/7/unpin"
    headers = {
        "Content-Type": "application/json",
        "authorization": "Bearer 21jk3h12kj3h"
    }
    data = {
        "chat_id": -1003514949333,
        "message_id": 7
    }

    async with ClientSession() as session:
        async with session.post(url=url, headers=headers, json=data, timeout=10) as response:
            answer = await response.text()
            print(answer)


async def create_topic():
    url = "http://127.0.0.1:8000/api/v1/topics/create"
    headers = {
        "Content-Type": "application/json",
        "authorization": "Bearer 21jk3h12kj3h"
    }
    data = {
        "chat_id": -1003514949333,
        "title": "newtopic2",
        "icon_color": 0x6FB9F0
    }

    async with ClientSession() as session:
        async with session.post(url=url, headers=headers, json=data, timeout=10) as response:
            answer = await response.text()
            print(answer)


async def edit_topic():
    url = "http://127.0.0.1:8000/api/v1/topics/10/edit"
    headers = {
        "Content-Type": "application/json",
        "authorization": "Bearer 21jk3h12kj3h"
    }
    data = {
        "chat_id": -1003514949333,
        "topic_id": 10,
        "title": "edittopicname"
    }

    async with ClientSession() as session:
        async with session.put(url=url, headers=headers, json=data, timeout=10) as response:
            answer = await response.text()
            print(answer)


async def delete_topic():
    url = "http://127.0.0.1:8000/api/v1/topics/10"
    headers = {
        "Content-Type": "application/json",
        "authorization": "Bearer 21jk3h12kj3h"
    }
    data = {
        "chat_id": -1003514949333,
        "topic_id": 10
    }

    async with ClientSession() as session:
        async with session.delete(url=url, headers=headers, json=data, timeout=10) as response:
            answer = await response.text()
            print(answer)


async def send_photo():
    url = "http://127.0.0.1:8000/api/v1/messages/send_photo"
    headers = {
        "Content-Type": "application/json",
        "authorization": "Bearer 21jk3h12kj3h"
    }
    data = {
        "chat_id": -1003514949333,
        "photo": "https://static-cse.canva.com/blob/847132/paulskorupskas7KLaxLbSXAunsplash2.jpg",
        "caption": "my_caption",
    }

    async with ClientSession() as session:
        async with session.post(url=url, headers=headers, json=data, timeout=10) as response:
            answer = await response.text()
            print(answer)


async def send_video():
    url = "http://127.0.0.1:8000/api/v1/messages/send_video"
    headers = {
        "Content-Type": "application/json",
        "authorization": "Bearer 21jk3h12kj3h"
    }
    data = {
        "chat_id": -1003514949333,
        "video": "C:/Users/valet/OneDrive/Рабочий стол/video_2026-01-19_14-49-54.mp4",
        "caption": "my_caption",
    }

    async with ClientSession() as session:
        async with session.post(url=url, headers=headers, json=data, timeout=10) as response:
            answer = await response.text()
            print(answer)


async def send_document():
    url = "http://127.0.0.1:8000/api/v1/messages/send_document"
    headers = {
        "Content-Type": "application/json",
        "authorization": "Bearer 21jk3h12kj3h"
    }
    data = {
        "chat_id": -1003514949333,
        "document": "C:/Users/valet/OneDrive/Рабочий стол/video_2026-01-19_14-49-54.mp4",
        "caption": "my_caption",
    }

    async with ClientSession() as session:
        async with session.post(url=url, headers=headers, json=data, timeout=10) as response:
            answer = await response.text()
            print(answer)


async def get_media_file_info():
    url = "http://127.0.0.1:8000/api/v1/media/-1003645046857/80/info"
    headers = {
        "Content-Type": "application/json",
        "authorization": "Bearer 1231424nelsa"
    }

    async with ClientSession() as session:
        async with session.get(url=url, headers=headers, timeout=30) as response:
            answer = await response.text()
            print(answer)


async def test():
    payload = MediaFileInfoResponse(
        status="success",
        media_info=MediaInfo(
            file_type="asd",
            file_name='asd',
            mime_type="asd",
            file_size=1,
            width=1,
            height=1,
            created_at="asd"
        )
    )
    result = await KafkaInterface().send_message(topic="tg-responses", payload=payload)
    print(result)
    await KafkaInterface().stop()


if __name__ == '__main__':
    asyncio.run(get_media_file_info())
