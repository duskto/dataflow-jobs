"""Bound the multipart body before Starlette spools the entire upload to disk."""

from uuid import uuid4

from starlette.formparsers import MultiPartException
from starlette.responses import JSONResponse

from app.core.config import get_settings


class UploadLimitMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope["path"] != "/api/v1/files" or scope["method"] != "POST":
            return await self.app(scope, receive, send)
        limit = get_settings().max_upload_bytes + 1024 * 1024  # bounded multipart overhead
        received, exceeded, replaced = 0, False, False

        def rejection():
            return JSONResponse(
                status_code=413,
                content={
                    "code": "FILE_TOO_LARGE",
                    "message": "上传请求超过大小限制",
                    "request_id": scope.get("state", {}).get("request_id", str(uuid4())),
                },
            )

        headers = dict(scope.get("headers", []))
        try:
            length = int(headers.get(b"content-length", b"0"))
        except ValueError:
            length = 0
        if length > limit:
            return await rejection()(scope, receive, send)

        async def limited_receive():
            nonlocal received, exceeded
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > limit:
                    exceeded = True
                    # Starlette closes multipart temporary files for this exception.
                    raise MultiPartException("Upload exceeds configured size limit")
            return message

        async def limited_send(message):
            nonlocal replaced
            if exceeded:
                if not replaced:
                    replaced = True
                    await rejection()(scope, receive, send)
                return
            await send(message)

        await self.app(scope, limited_receive, limited_send)
