from fastapi import APIRouter, Request
from typing import (
    Literal,
    List
)
from copilotkit.knowledge_base.kb_doc_api import (
    list_files,
    search_docs,
    upload_docs,
    delete_docs,
    update_docs,
    update_info,
    download_doc,
    recreate_vector_store,
    search_temp_docs
)
from copilotkit.knowledge_base.kb_api import (
    list_kbs,
    delete_kb,
    create_db,
)
from copilotkit.chat.kb_chat import kb_chat
from copilotkit.utils import (
    BaseResponse,
    ListResponse
)
from api_schema import OpenAIChatInput
kb_router = APIRouter(prefix="/knowledge_base", tags=["knowledge base"])


@kb_router.post(
    "/{mode}/{param}/chat/completions", summary="知识库对话"
)
async def kb_chat_endpoint(
        mode: Literal["local_kb", "temp_kb", "search_engine"],
        param: str,
        body: OpenAIChatInput,
        request: Request
):
    if body.max_tokens in [None, 0]:
        body.max_tokens = None

    extra = body.model_extra
    ret = await kb_chat(
        query=body.messages[-1]["content"],
        conversation_id=extra.get("conversation_id", "admin"),
        conversation_name=extra.get("conversation_name", "admin"),
        user_id=extra.get("user_id", "admin"),
        mode=mode,
        kb_name=param,
        top_k=extra.get("top_k", 3),
        score_threshold=extra.get("score_threshold", 0.7),
        history=body.messages[:-1],
        stream=body.stream,
        model=body.model,
        temperature=body.temperature,
        max_tokens=body.max_tokens,
        prompt_name=extra.get("prompt", "default"),
        return_direct=extra.get("return_direct", False),
        request=request
    )
    return ret


kb_router.get(
    "/list_knowledge_bases", response_model=ListResponse
)(list_kbs)

kb_router.post(
    "/create_knowledge_base", response_model=BaseResponse
)(create_db)

kb_router.post(
    "/delete_knowledge_base", response_model=BaseResponse
)(delete_kb)

kb_router.get(
    "/list_files", response_model=ListResponse
)(list_files)

kb_router.post(
    "/search_docs", response_model=List[dict]
)(search_docs)

kb_router.post(
    "/upload_docs",
    response_model=BaseResponse
)(upload_docs)

kb_router.post(
    "/delete_docs",
    response_model=BaseResponse
)(delete_docs)

kb_router.post(
    "/update_info",
    response_model=BaseResponse
)(update_info)

kb_router.post(
    "/update_docs"
)(update_docs)

kb_router.get(
    "/download_doc"
)(download_doc)

# 目前用不到，暂不对外开放接口
# kb_router.post(
#     "recreate_vector_store",
# )(recreate_vector_store)


kb_router.post(
    "/search_temp_docs"
)(search_temp_docs)
