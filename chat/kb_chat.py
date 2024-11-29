import asyncio
import json

from fastapi import Body, Request
from typing import (
    Literal,
    List,
    Optional,
    AsyncIterable
)
from fastapi.concurrency import run_in_threadpool
from langchain.callbacks import AsyncIteratorCallbackHandler
from langchain.prompts.chat import ChatPromptTemplate
from copilotkit.utils import History, api_address
from copilotkit.knowledge_base.utils import format_reference

from copilotkit.knowledge_base.kb_service.base import KBServiceFactory
from copilotkit.knowledge_base.kb_doc_api import search_docs, search_temp_docs
from copilotkit.utils import (
    wrap_done,
    get_ChatOpenAI,
    BaseResponse,
    check_embed_model
)
from copilotkit.callback_handler.conversation_callback_handler import ConversationCallbackHandler
from copilotkit.db.repository.message_repository import add_message_to_db, update_message
from copilotkit.memory.conversation_db_buffer_memory import ConversationBufferDBMemory
from copilotkit.reranker.reranker import LangchainReranker

default_prompt = {"default":
                      "【指令】根据已知信息，简洁和专业的来回答问题，不允许在答案中添加编造成分。"
                      "如果无法从中得到答案，请你直接回复已知信息的内容。\n\n"
                      "【已知信息】答案是：{{context}}\n"
                      "【问题】{{question}}\n\n",
                  "empty":
                      "请你回答我的问题:\n"
                      "{{question}}"
                  }


async def kb_chat(
        user_id: str = Body("admin"),
        conversation_id: str = Body(""),
        conversation_name: str = Body("admin"),
        query: str = Body(...),
        mode: Literal["local_kb", "temp_kb", "search_engine"] = Body("local_kb"),
        kb_name: str = Body(""),
        top_k: int = Body(3),
        score_threshold: float = Body(0.7, ge=0, le=2),
        history: List[History] = Body([]),
        stream: bool = Body(False),
        model: str = Body("custom-qwen2-chat"),
        temperature: float = Body(1, ge=0.0, le=2.0),
        max_tokens: Optional[int] = Body(4096),
        prompt_name: str = Body("default")
):
    if mode == "local_kb":
        kb = await KBServiceFactory.get_service_by_name(kb_name)
        if kb is None:
            return BaseResponse(code=404, msg=f"未找到知识库 {kb_name}")

    async def knowledge_base_chat_iterator() -> AsyncIterable[str]:
        try:
            nonlocal history, prompt_name, max_tokens
            message_id = await add_message_to_db(
                user_id=user_id,
                conversation_id=conversation_id,
                conversation_name=conversation_name,
                prompt_name=prompt_name,
                query=query
            )

            conversation_callback = ConversationCallbackHandler(
                conversation_id=conversation_id,
                message_id=message_id,
                chat_type=prompt_name,
                query=query
            )
            if mode == "local_kb":
                kb = await KBServiceFactory.get_service_by_name(kb_name)
                ok, msg = kb.check_embed_model()
                if not ok:
                    raise ValueError(msg)

                docs = await search_docs(
                    query=query,
                    knowledge_base_name=kb_name,
                    top_k=top_k,
                    score_threshold=score_threshold
                )
                rerank_path = "/home/northinfo_99/PyProject/Xorbits/bge-reranker-large"
                rerank_model = LangchainReranker(top_n=1,
                                                 device="cpu",
                                                 max_length=1024,
                                                 model_name_or_path=rerank_path
                                                 )
                docs = rerank_model.compress_documents(documents=docs,query=query)
                print("----------after reranker------------")
                # float(docs[0].metadata['relevance_score'])
                print(docs)
                if docs:
                    data, context, code = format_reference(message_id, mode, kb_name, docs, api_address(is_public=True))
                else:
                    data, context, code = "", "", ""

            elif mode == "temp_kb":
                ok, msg = check_embed_model()
                if not ok:
                    raise ValueError(msg)
                docs = await run_in_threadpool(search_temp_docs,
                                               kb_name,
                                               query=query,
                                               top_k=top_k,
                                               score_threshold=score_threshold)
                data, context, code = format_reference(message_id, mode, kb_name, docs, api_address(is_public=True))

            callback = AsyncIteratorCallbackHandler()
            callbacks = [callback]
            callbacks.append(conversation_callback)

            if max_tokens in [None, 0]:
                max_tokens = 2048

            llm = get_ChatOpenAI(
                model_name=model,
                temperature=temperature,
                max_tokens=max_tokens,
                callbacks=callbacks
            )
            if len(docs) == 0:
                prompt_name = 'empty'
                await update_message(chat_type=prompt_name,
                                     message_id=message_id)
                code = 204
            else:
                prompt_name = "default"
            memory = ConversationBufferDBMemory(conversation_id=conversation_id,
                                                llm=llm,
                                                chat_type=prompt_name,
                                                message_limit=10)
            history = await memory.buffer()
            import numpy as np

            prompt_template = default_prompt.get(prompt_name)
            system_msg = History(role="system",
                                 content="你是一位善于结合历史对话信息，以及相关文档回答问题的高智商助手").to_msg_template(
                is_raw=False)
            input_msg = History(role="user", content=prompt_template).to_msg_template(False)

            history = [History.from_data(h) for h in history]
            chat_prompt = ChatPromptTemplate.from_messages(
                [i.to_msg_template() for i in history] + [system_msg, input_msg]  # [input_msg]
            )
            chain = chat_prompt | llm

            task = asyncio.create_task(wrap_done(
                chain.ainvoke({"context": context, "question": query}),
                callback.done),
            )

            if stream:
                # todo 流式输出
                ...
            else:
                answer = ""
                async for token in callback.aiter():
                    answer += token
                yield json.dumps({"answer": answer,
                                  "data": data,
                                  "code": code},
                                 ensure_ascii=False)
            await task

        except asyncio.exceptions.CancelledError:
            print("streaming progress has been interrupted by user")
            return
        except Exception as e:
            print(f"error in knowledge chat: {e}")
            yield {"data": json.dumps({"error": str(e)})}
            return
    if stream:
        ...
    else:
        return await knowledge_base_chat_iterator().__anext__()