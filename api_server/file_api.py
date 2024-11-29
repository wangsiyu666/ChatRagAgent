import os.path

from fastapi import APIRouter, Request, UploadFile, File, Form, Body, Query
from fastapi.responses import FileResponse
from typing import (
    Literal,
    List
)
from utils import BaseResponse, ListResponse, _save_files_in_thread


file_router = APIRouter(prefix="/file", tags=["file base"])


async def upload_files(
        files: List[UploadFile] = File(..., description="上传文件，支持多文件"),
        override: bool = Form(False, description="覆盖已有文件"),
) -> BaseResponse:

    failed_files = {}
    file_names = []
    for result in _save_files_in_thread(
        files
    ):
        filename = result["data"]["file_name"]
        if result["code"] != 200:
            failed_files[filename] = result["msg"]

        if filename not in file_names:
            file_names.append(filename)
    return BaseResponse(
        code=200, msg="文件上传成功", data={"failed_files": failed_files}
    )


async def download_doc(
        file_name: str = Query(..., description="文件名称"),
        preview: bool = Query(False, description="是： 浏览器内刘安， 否：下载")
):
    if preview:
        content_disposition_type = "inline"
    else:
        content_disposition_type = None
    file_path = os.path.join("E:\公司\copilotkit\data", file_name)
    try:
        if os.path.exists(file_path):
            return FileResponse(
                path=file_path,
                filename=file_name,
                media_type="multipart/form-data",
                content_disposition_type=content_disposition_type
            )
    except Exception as e:
        msg = f"{file_name} 读取文件失败，失败信息是： {e}"
        return BaseResponse(code=500, msg=msg)


file_router.post(
    "/upload_files", response_model=BaseResponse
)
file_router.post(
    "/download_files"
)
