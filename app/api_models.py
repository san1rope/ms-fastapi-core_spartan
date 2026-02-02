from typing import Optional

from pydantic import BaseModel


class APIError(Exception):
    def __init__(self, error: str, code: str, status_code: int = 400):
        self.error = error
        self.code = code
        self.status_code = status_code


class PhotoData(BaseModel):
    width: int
    height: int
    file_size: int


class VideoData(BaseModel):
    width: int
    height: int
    duration: int
    file_size: int


class AudioData(BaseModel):
    duration: int
    title: str
    performer: str
    file_size: int


class DocumentData(BaseModel):
    file_name: str
    mime_type: str
    file_size: int


class StickerData(BaseModel):
    width: int
    height: int
    is_animated: bool
    file_size: int


class VoiceData(BaseModel):
    duration: int
    mime_type: str
    file_size: int


class GIFData(BaseModel):
    width: int
    height: int
    duration: int
    file_size: int


class MediaInfo(BaseModel):
    file_type: str
    file_name: str
    mime_type: str
    file_size: int
    width: int
    height: int
    created_at: str


class ResponseData(BaseModel):
    status: str
    id: Optional[str]
    error: Optional[str]
    code: Optional[str]


class SendMessageRequest(BaseModel):
    chat_id: int
    text: str
    topic_id: Optional[int] = None
    parse_mode: str = "markdown"
    disable_notification: bool = False
    reply_to_message_id: Optional[int] = None


class EditMessageRequest(BaseModel):
    chat_id: int
    message_id: int
    text: str
    parse_mode: str = "markdown"


class DeleteMessageRequest(BaseModel):
    chat_id: int
    message_id: int


class MessagePinRequest(BaseModel):
    chat_id: int
    message_id: int


class MessageUnpinRequest(BaseModel):
    chat_id: int
    message_id: int


class SendPhotoRequest(BaseModel):
    chat_id: int
    photo: str
    caption: Optional[str] = None
    topic_id: Optional[int] = None
    parse_mode: str = "markdown"


class SendVideoRequest(BaseModel):
    chat_id: int
    video: str
    caption: Optional[str] = None
    topic_id: Optional[int] = None
    parse_mode: str = "markdown"


class SendAudioRequest(BaseModel):
    chat_id: int
    audio: str
    caption: Optional[str] = None
    topic_id: Optional[int] = None
    parse_mode: str = "markdown"


class SendDocumentRequest(BaseModel):
    chat_id: int
    document: str
    caption: Optional[str] = None
    topic_id: Optional[int] = None
    parse_mode: str = "markdown"


class SendStickerRequest(BaseModel):
    chat_id: int
    sticker: str
    topic_id: Optional[int] = None


class SendVoiceRequest(BaseModel):
    chat_id: int
    voice: str
    caption: Optional[str] = None
    topic_id: Optional[int] = None


class SendGIFRequest(BaseModel):
    chat_id: int
    gif: str
    caption: Optional[str] = None
    topic_id: Optional[int] = None
    parse_mode: str = "markdown"


class CreateTopicRequest(BaseModel):
    chat_id: int
    title: str
    icon_color: int


class EditTopicRequest(BaseModel):
    chat_id: int
    topic_id: int
    title: str


class DeleteTopicRequest(BaseModel):
    chat_id: int
    topic_id: int


class MediaFileInfoRequest(BaseModel):
    chat_id: int
    message_id: int


class MediaFileInfoResponse(BaseModel):
    status: str
    media_info: MediaInfo
