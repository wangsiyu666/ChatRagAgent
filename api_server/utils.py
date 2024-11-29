import os.path

from fastapi import FastAPI, UploadFile
from pathlib import Path
from pydantic import BaseModel, Field
from concurrent.futures import ThreadPoolExecutor, as_completed

from typing import (
    Optional,
    Any,
    List,
    Callable,
    Dict,
    Generator
)


def MakeFastAPIOffline(
        app: FastAPI,
        static_dir=Path(__file__).parent / "api_server" / "static",
        static_url="/static-offline-docs",
        docs_url: Optional[str] = "/docs",
        redoc_url: Optional[str] = "/redoc",
) -> None:
    from fastapi import Request
    from fastapi.openapi.docs import (
        get_redoc_html,
        get_swagger_ui_html,
        get_swagger_ui_oauth2_redirect_html,
    )
    from fastapi.staticfiles import StaticFiles
    from starlette.responses import HTMLResponse

    openapi_url = app.openapi_url
    swagger_ui_oauth2_redirect_url = app.swagger_ui_oauth2_redirect_url

    def remove_route(url: str) -> None:
        index = None
        for i, r in enumerate(app.routes):
            if r.path.lower() == url.lower():
                index = i
                break
        if isinstance(index, int):
            app.routes.pop(index)

    app.mount(
        static_url,
        # StaticFiles(directory=Path(static_dir).as_posix()),
        StaticFiles(directory=r"E:\公司\copilotkit\api_server\static"),
        name="static-offline-docs",
    )
    if docs_url is not None:
        remove_route(docs_url)
        remove_route(swagger_ui_oauth2_redirect_url)

        # Define the doc and redoc pages, pointing at the right files
        @app.get(docs_url, include_in_schema=False)
        async def custom_swagger_ui_html(request: Request) -> HTMLResponse:
            root = request.scope.get("root_path")
            favicon = f"{root}{static_url}/favicon.png"
            return get_swagger_ui_html(
                openapi_url=f"{root}{openapi_url}",
                title=app.title + " - Swagger UI",
                oauth2_redirect_url=swagger_ui_oauth2_redirect_url,
                swagger_js_url=f"{root}{static_url}/swagger-ui-bundle.js",
                swagger_css_url=f"{root}{static_url}/swagger-ui.css",
                swagger_favicon_url=favicon,
            )

        @app.get(swagger_ui_oauth2_redirect_url, include_in_schema=False)
        async def swagger_ui_redirect() -> HTMLResponse:
            return get_swagger_ui_oauth2_redirect_html()

    if redoc_url is not None:
        remove_route(redoc_url)

        @app.get(redoc_url, include_in_schema=False)
        async def redoc_html(request: Request) -> HTMLResponse:
            root = request.scope.get("root_path")
            favicon = f"{root}{static_url}/favicon.png"

            return get_redoc_html(
                openapi_url=f"{root}{openapi_url}",
                title=app.title + " - ReDoc",
                redoc_js_url=f"{root}{static_url}/redoc.standalone.js",
                with_google_fonts=False,
                redoc_favicon_url=favicon,
            )


class BaseResponse(BaseModel):
    code: int = Field(200, description="API status code")
    msg: str = Field("success", description="API status message")
    data: Any = Field(None, description="APPI data")

    class Config:
        json_schema_extra = {
            "example": {
                "code": 200,
                "msg": "success"
            }
        }


class ListResponse(BaseResponse):
    data: List[Any] = Field(..., description="List of data")

    class Config:
        json_schema_extra = {
            "example": {
                "code": 200,
                "msg": "success",
                "data": ["doc1.docs", "doc2.pdf", "doc3.txt"]
            }
        }


def get_file_path(doc_name: str):
    return str(os.path.join("E:\公司\copilotkit\data", doc_name))


def run_in_thread_pool(
        func: Callable,
        params: List[Dict] = []
) -> Generator:
    tasks = []
    with ThreadPoolExecutor() as pool:
        for kwargs in params:
            tasks.append(pool.submit(func, **kwargs))

        for obj in as_completed(tasks):
            try:
                yield obj.result()
            except Exception as e:
                print(e)


def _save_files_in_thread(
        files: List[UploadFile]
):
    def save_file(file: UploadFile) -> dict:
        try:
            filename = file.filename
            file_path = get_file_path(
                doc_name=filename
            )
            data = {"file_name": filename}
            file_content = file.file.read()
            if (
                    os.path.isfile(file_path)
                    and os.path.getsize(file_path) == len(file_content)
            ):
                file_status = f"文件 {filename} 已存在"
                return dict(code=404, msg=file_status, data=data)
            with open(file_path, "wb") as f:
                f.write(file_content)
            return dict(code=200, msg=f"成功上传文件 {filename}", data=data)
        except Exception as e:
            msg = f"{filename} 文件上传失败， 报错信息为： {e}"
            return dict(code=500, msg=msg, data=data)

    params = [
        {"file": file} for file in files
    ]
    for result in run_in_thread_pool(save_file, params=params):
        yield result


if __name__ == '__main__':
    r = get_file_path("t.txt")
    print(r)
