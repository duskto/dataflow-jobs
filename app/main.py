import logging
from pathlib import Path
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.exceptions import HTTPException

from app.api.routes import router
from app.core.errors import AppError
from app.core.upload_limit import UploadLimitMiddleware

app = FastAPI(title="DataFlow Jobs", version="0.1.0", description="可靠的 CSV 后台处理平台")
app.add_middleware(UploadLimitMiddleware)
app.include_router(router)
root = Path(__file__).parent
app.mount("/static", StaticFiles(directory=root / "static"), name="static")
templates = Jinja2Templates(directory=root / "templates")


@app.middleware("http")
async def request_id(request: Request, call_next):
    request.state.request_id = str(uuid4())
    response = await call_next(request)
    response.headers["X-Request-ID"] = request.state.request_id
    response.headers["X-Content-Type-Options"] = "nosniff"
    return response


def error(request, code, message, status):
    return JSONResponse(
        status_code=status,
        content={
            "code": code,
            "message": message,
            "request_id": getattr(request.state, "request_id", str(uuid4())),
        },
    )


@app.exception_handler(AppError)
async def app_error(request, exc):
    return error(request, exc.code, exc.message, exc.status)


@app.exception_handler(RequestValidationError)
async def validation_error(request, exc):
    messages = [f"{'.'.join(map(str, e['loc']))}: {e['msg']}" for e in exc.errors()[:8]]
    return error(request, "VALIDATION_ERROR", "; ".join(messages), 422)


@app.exception_handler(HTTPException)
async def http_error(request, exc):
    return error(request, "HTTP_ERROR", str(exc.detail), exc.status_code)


@app.exception_handler(Exception)
async def internal_error(request, exc):
    logging.getLogger(__name__).error("request_id=%s", request.state.request_id, exc_info=exc)
    return error(request, "INTERNAL_ERROR", "服务暂时不可用，请稍后重试", 500)


@app.get("/", include_in_schema=False)
def index(request: Request):
    return templates.TemplateResponse(request=request, name="index.html")
