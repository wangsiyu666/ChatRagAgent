import urllib.parse
from copilotkit.db.repository.knowledge_base_respository import list_kbs_from_db
from copilotkit.utils import (
    BaseResponse,
    ListResponse,
    get_default_embedding
)
from copilotkit.knowledge_base.utils import validate_kb_name
from copilotkit.knowledge_base.kb_service.base import KBServiceFactory

from fastapi import Body

async def list_kbs():
    data = await list_kbs_from_db()
    return ListResponse(data=data)


async def create_db(
        knowledge_base_name: str = Body(...),
        vector_store_type: str = Body("faiss"),
        kb_info: str = Body(""),
        embed_model: str = Body(get_default_embedding())
) -> BaseResponse:
    if not validate_kb_name(knowledge_base_name):
        return BaseResponse(code=403, msg="Dont attack me")

    if knowledge_base_name is None or knowledge_base_name.strip() == "":
        return BaseResponse(code=404, msg=f"知识库名称不能为空，请重新填写")

    kb = KBServiceFactory.get_service(
        knowledge_base_name,
        vector_store_type,
        embed_model,
        kb_info=kb_info
    )
    try:
        await kb.create_kb()
    except Exception as e:
        msg = f"创建知识库出错： {e}"
        return BaseResponse(code=500, msg=msg)
    return BaseResponse(code=200, msg=f"已新增知识库 {knowledge_base_name}")


async def delete_kb(
        knowledge_base_name: str = Body(...)
) -> BaseResponse:
    if not validate_kb_name(knowledge_base_name):
        return BaseResponse(code=403, msg="Dont attack me")
    knowledge_base_name = urllib.parse.unquote(knowledge_base_name)
    kb = await KBServiceFactory.get_service_by_name(knowledge_base_name)

    if kb is None:
        return BaseResponse(code=404, msg=f"未找到知识库 {knowledge_base_name}")
    try:
        status = await kb.clear_vs()
        status = await kb.drop_kb()
        if status:
            return BaseResponse(
                code=200,
                msg=f"成功删除知识库 {knowledge_base_name}"
            )
    except Exception as e:
        msg = f"删除知识库失败： {e}"
        return BaseResponse(code=500, msg=msg)

    return BaseResponse(code=500, msg=f"删除知识库失败 {knowledge_base_name}")


