from copilotkit.db.session import with_async_session, async_session_scope
from copilotkit.db.models.message_model import MessageModel
from copilotkit.db.models.conversation_model import ConversationModel

from typing import (
    Dict,
    List
)
import uuid
from sqlalchemy.future import select
from fastapi import HTTPException

@with_async_session
async def add_message_to_db(
        session,
        query: str,
        conversation_id: str,
        prompt_name: str,
        response="",
        metadata="",
        message_id=None
):
    conversation = await session.get(ConversationModel, conversation_id)

    if conversation.name == "新对话":
        conversation.name = query

    await session.commit()

    if not message_id:
        message_id = str(uuid.uuid4())

    m = MessageModel(
        id=message_id,
        conversation_id=conversation_id,
        chat_type=prompt_name,
        query=query,
        response=response,
        metadata=metadata
    )

    session.add(m)

    await session.commit()
    return m.id

@with_async_session
async def filter_message(
        session,
        conversation_id: str,
        chat_type: str,
        limit: int = 10
):
    result = await session.execute(
        select(MessageModel)
        .filter_by(conversation_id=conversation_id, chat_type=chat_type)
        .filter(MessageModel.response != '')
        .order_by(MessageModel.create_time.desc())
        .limit(limit)
    )

    return result.scalars().all()


@with_async_session
async def get_message_by_id(session, message_id) -> MessageModel:

    result = await session.execute(
        select(MessageModel).filter_by(id=message_id)
    )

    return result.scalars().first()


@with_async_session
async def update_message(
        session,
        message_id,
        response: str = None,
        metadata: str = None
):
    m = await get_message_by_id(message_id)

    if m is not None:
        if response is not None:
            m.response = response

        if isinstance(metadata, str):
            m.metadata = metadata

        session.add(m)

        await session.commit()
        return m.id

    else:
        raise HTTPException(status_code=404, detail="Message not found")



