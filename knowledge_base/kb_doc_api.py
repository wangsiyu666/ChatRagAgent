import asyncio.exceptions
import json
import asyncio
import os

from fastapi import Body, UploadFile, File, Form, Query
from fastapi.responses import FileResponse
from sse_starlette import EventSourceResponse
import urllib
from copilotkit.knowledge_base.kb_cache.faiss_cache import memo_faiss_pool
from typing import (
    List,
    Dict
)
from copilotkit.knowledge_base.kb_service.base import KBServiceFactory, get_kb_file_details
from langchain.docstore.document import Document
from copilotkit.utils import (
    ListResponse,
    run_in_thread_pool,
    get_default_embedding,
    BaseResponse
)

from copilotkit.knowledge_base.utils import (
    validate_kb_name,
    get_file_path,
    KnowledgeFile,
    files2docs_in_thread,
    list_files_from_folder,
)
from copilotkit.db.repository.knowledge_file_respository import get_file_detail


class DocumentWithVSId(Document):
    id: str = None
    score: float = 3.0


def search_temp_docs(
        knowledge_id: str = Body(...),
        query: str = Body(""),
        top_k: int = Body(...),
        score_threshold: float = Body(2)
) -> List[Dict]:
    with memo_faiss_pool.acquire(knowledge_id) as vs:
        docs = vs.similarity_search_with_score(
            query,
            k=top_k,
            score_threshold=score_threshold
        )
        docs = [x[0].dict() for x in docs]
        return docs


async def search_docs(
        query: str = Body(""),
        knowledge_base_name: str = Body(...),
        top_k: int = Body(3),
        score_threshold: float = Body(0.7, ge=0.0, le=2.0),
        file_name: str = Body(""),
        metadata: dict = Body({})
) -> List[Dict]:
    kb = await KBServiceFactory.get_service_by_name(knowledge_base_name)
    data = []
    if kb is not None:
        if query:
            docs = await kb.search_docs(query, top_k, score_threshold)
            # data = [DocumentWithVSId(**x[0].dict(), score=x[1], id=x[0].metadata.get("id")) for x in docs]
            if asyncio.iscoroutine(docs):
                docs = await docs
                data = [DocumentWithVSId(**x[0].dict(), score=x[1], id=x[0].metadata.get("id")) for x in docs]
            else:
                data = [DocumentWithVSId(**{"id": x.metadata.get("id"), **x.dict()}) for x in docs]
        elif file_name or metadata:
            data = await kb.list_docs(file_name=file_name, metadata=metadata)
            for d in data:
                if "vector" in d.metadata:
                    del d.metadata["vector"]
    return data


async def list_files(knowledge_base_name: str) -> ListResponse:
    if not validate_kb_name(knowledge_base_name):
        return ListResponse(code=403, msg="Dont attack me", data=[])

    knowledge_base_name = urllib.parse.unquote(knowledge_base_name)
    kb = await KBServiceFactory.get_service_by_name(knowledge_base_name)
    if kb is None:
        return ListResponse(
            code=404, msg=f"未找到知识库{knowledge_base_name}", data=[]
        )
    else:
        all_docs = await get_kb_file_details(knowledge_base_name)
        return ListResponse(data=all_docs)


def _save_files_in_thread(
        files: List[UploadFile], knowledge_base_name: str, override: bool
):
    def save_file(file: UploadFile, knowledge_base_name: str, override: bool) -> dict:
        try:
            filename = file.filename
            file_path = get_file_path(
                knowledge_base_name=knowledge_base_name,
                doc_name=filename
            )
            data = {"knowledge_base_name": knowledge_base_name, "file_name": filename}
            file_content = file.file.read()
            if (
                os.path.isfile(file_path)
                and not override
                and os.path.getsize(file_path) == len(file_content)
            ):
                file_status = f"文件 {filename} 已存在"
                return dict(code=404, msg=file_status, data=data)
            if not os.path.isdir(os.path.dirname(file_path)):
                os.makedirs(os.path.dirname(file_path))
            with open(file_path, "wb") as f:
                f.write(file_content)
            return dict(code=200, msg=f"成功上传文件{filename}", data=data)
        except Exception as e:
            msg = f"{filename} 文件上传失败，报错信息为{e}"
            return dict(code=500, msg=msg, data=data)
    params = [
        {"file": file, "knowledge_base_name": knowledge_base_name, "override": override}
        for file in files
    ]
    for result in run_in_thread_pool(save_file, params=params):
        yield result


async def update_docs(
        knowledge_base_name: str = Body(...),
        file_names: List[str] = Body(...),
        chunk_size: int = Body(750),
        chunk_overlap: int = Body(150),
        zh_title_enhance: bool = Body(False),
        override_custom_docs: bool = Body(False),
        docs: str = Body(""),
        not_refresh_vs_cache: bool = Body(False)
) -> BaseResponse:
    if not validate_kb_name(knowledge_base_name):
        return BaseResponse(code=403, msg="Dont attack me")

    kb = await KBServiceFactory.get_service_by_name(knowledge_base_name)
    if kb is None:
        return BaseResponse(code=404, msg=f"未找到知识库 {knowledge_base_name}")
    failed_files = {}
    kb_files = []
    docs = json.load(docs) if docs else {}
    for file_name in file_names:
        file_detail = await get_file_detail(
            kb_name=knowledge_base_name,
            filename=file_name
        )
        if file_detail.get("custom_docs") and not override_custom_docs:
            continue
        if file_name not in docs:
            try:
                kb_files.append(
                    KnowledgeFile(
                        filename=file_name,
                        knowledge_base_name=knowledge_base_name
                    )
                )
            except Exception as e:
                msg = f"加载文档 {file_name} 时出错: {e}"
                failed_files[file_name] = msg
    for status, result in files2docs_in_thread(
        kb_files,
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        zh_title_enhance=zh_title_enhance
    ):
        if status:
            kb_name, file_name, new_docs = result
            kb_file = KnowledgeFile(
                filename=file_name,
                knowledge_base_name=knowledge_base_name
            )
            kb_file.splited_docs = new_docs
            await kb.update_doc(kb_file, not_refresh_vs_cache=True)
        else:
            kb_name, file_name, error = result
            failed_files[file_name] = error
    for file_name, v in docs.items():
        try:
            v = [x if isinstance(x, Document) else Document(**x) for x in v]
            kb_file = KnowledgeFile(
                filename=kb_file,
                knowledge_base_name=knowledge_base_name
            )
            kb.update_doc(kb_file, docs=v, not_refresh_vs_cache=True)
        except Exception as e:
            msg = f"为 {file_name} 添加自定义docs时出错: {e}"
            failed_files[file_name] = msg
    if not not_refresh_vs_cache:
        kb.save_vector_store()

    return BaseResponse(
        code=200,
        msg=f"更新文档完成",
        data={"failed_files": failed_files}
    )



async def upload_docs(
        files: List[UploadFile] = File(...),
        knowledge_base_name: str = Form(...),
        override: bool = Form(False),
        to_vector_store: bool = Form(True),
        chunk_size: int = Form(750),
        chunk_overlap: int = Form(150),
        zh_title_enhance: bool = Form(False),
        docs: str = Form(""),
        not_refresh_vs_cache: bool = Form(False)
) -> BaseResponse:
    if not validate_kb_name(knowledge_base_name):
        return BaseResponse(code=403, msg="Dont attack me")

    kb = await KBServiceFactory.get_service_by_name(knowledge_base_name)
    if kb is None:
        return BaseResponse(code=404, msg=f"未找到数据库 {knowledge_base_name}")

    docs = json.loads(docs) if docs else {}
    failed_files = {}
    file_names = list(docs.keys())

    for result in _save_files_in_thread(
        files,
        knowledge_base_name=knowledge_base_name,
        override=override
    ):
        filename = result["data"]["file_name"]
        if result["code"] != 200:
            failed_files[filename] = result["msg"]

        if filename not in file_names:
            file_names.append(filename)

    if to_vector_store:
        result = await update_docs(
            knowledge_base_name=knowledge_base_name,
            file_names=file_names,
            override_custom_docs=True,
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            zh_title_enhance=zh_title_enhance,
            docs=docs,
            not_refresh_vs_cache=not_refresh_vs_cache
        )
        failed_files.update(result.data["failed_files"])
        if not not_refresh_vs_cache:
            kb.save_vector_store()
    return BaseResponse(
        code=200, msg="文件上传向量化完成", data={"failed_files": failed_files}
    )


async def delete_docs(
        knowledge_base_name: str = Body(...),
        file_names: List[str] = Body(...),
        delete_content: bool = Body(False),
        not_refresh_vs_cache: bool = Body(False)
) -> BaseResponse:
    if not validate_kb_name(knowledge_base_name):
        return BaseResponse(code=403, msg="Dont attack me")

    knowledge_base_name = urllib.parse.unquote(knowledge_base_name)
    kb = await KBServiceFactory.get_service_by_name(knowledge_base_name)
    if kb is None:
        return BaseResponse(code=404, msg=f"未找到知识库 {knowledge_base_name}")

    failed_files = {}
    for file_name in file_names:
        if not await kb.exist_doc(file_name):
            failed_files[file_name] = f"未找到文件 {file_name}"
        try:
            kb_file = KnowledgeFile(
                filename=file_name,
                knowledge_base_name=knowledge_base_name
            )
            await kb.delete_doc(kb_file, delete_content, not_refresh_vs_cache=True)
        except Exception as e:
            msg = f"{file_name} 文件删除失败，错误信息{e}"
            failed_files[file_name] = msg
    if not not_refresh_vs_cache:
        kb.save_vector_store()

    return BaseResponse(
        code=200, msg=f"文件删除成功",data={"failed_files": failed_files}
    )


async def update_info(
        knowledge_base_name: str = Body(...),
        kb_info: str = Body(...)
):
    if not validate_kb_name(knowledge_base_name):
        return BaseResponse(code=403, msg="Dont attack me")

    kb = await KBServiceFactory.get_service_by_name(knowledge_base_name)
    if kb is None:
        return BaseResponse(code=404, msg=f"未找到知识库 {knowledge_base_name}")
    await kb.update_info(kb_info)

    return BaseResponse(code=200, msg=f"知识库介绍修改完成", data={"kb_info": kb_info})


async def download_doc(
        knowledge_base_name: str = Query(...),
        file_name: str = Query(...),
        preview: bool = Query(False)
):
    if not validate_kb_name(knowledge_base_name):
        return BaseResponse(code=403, msg="Dont attack me")
    kb = await KBServiceFactory.get_service_by_name(knowledge_base_name)
    if kb is None:
        return BaseResponse(code=404, msg=f"未找到知识库 {knowledge_base_name}")

    if preview:
        content_disposition_type = "inline"
    else:
        content_disposition_type = None

    try:
        kb_file = KnowledgeFile(
            filename=file_name,
            knowledge_base_name=knowledge_base_name
        )
        if os.path.exists(kb_file.filepath):
            return FileResponse(
                path=kb_file.filepath,
                filename=kb_file.filename,
                media_type="multipart/form-data",
                content_disposition_type=content_disposition_type
            )
    except Exception as e:
        msg = f"{kb_file.filename} 读取文件失败，错误信息是：{e}"
        return BaseResponse(code=500, msg=f"{kb_file.filename} 读取失败")


def recreate_vector_store(
        knowledge_base_name: str = Body(...),
        allow_empty_kb: bool = Body(True),
        vs_type: str = Body("faiss"),
        embed_model: str = Body(get_default_embedding()),
        chunk_size: int = Body(750),
        chunk_overlap: int = Body(150),
        zh_title_enhance: bool = Body(False),
        not_refresh_vs_cache: bool = Body(False)
):
    def output():
        try:
            kb = KBServiceFactory.get_service(knowledge_base_name, vs_type, embed_model)
            if not kb.exists() and not allow_empty_kb:
                yield {"code": 404, "msg": f"未找到知识库 {knowledge_base_name}"}
            else:
                ok, msg = kb.check_embed_model()
                if not ok:
                    yield {"code": 404, "msg": msg}
                else:
                    if kb.exists():
                        kb.clear_vs()
                    kb.create_kb()
                    files = list_files_from_folder(knowledge_base_name)
                    kb_files = [(file, knowledge_base_name) for file in files]
                    i = 0
                    for status, result in files2docs_in_thread(
                        kb_files,
                        chunk_size=chunk_size,
                        chunk_overlap=chunk_overlap,
                        zh_title_enhance=zh_title_enhance
                    ):
                        if status:
                            kb_name, file_name, docs = result
                            kb_file = KnowledgeFile(
                                filename=file_name,
                                knowledge_base_name=kb_name
                            )
                            kb_file.splited_docs = docs
                            yield json.dumps(
                                {
                                    "code": 200,
                                    "msg": f"({i + 1} / {len(files)}) : {file_name}",
                                    "total": len(files),
                                    "finished": i + 1,
                                    "docs": file_name
                                },
                                ensure_ascii=False
                            )
                            kb.add_doc(kb_file, not_refresh_vs_cache=True)
                        else:
                            kb_name, file_name, error = result
                            msg = f"添加文件 {file_name} 到知识库 {knowledge_base_name} 时出错 {error}"
                            yield json.dumps(
                                {
                                    "code": 500,
                                    "msg": msg
                                }
                            )
                        i += 1
                    if not not_refresh_vs_cache:
                        kb.save_vector_store()
        except asyncio.exceptions.CancelledError:
            return
    # yield+EventSourceResponse -> streaming
    return EventSourceResponse(output())


