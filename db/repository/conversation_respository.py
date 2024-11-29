from fastapi import HTTPException, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel, Field
from datetime import datetime
from copilotkit.db.session import with_async_session, get_async_db
from copilotkit.db.models.conversation_model import ConversationModel
from copilotkit.db.models.message_model import MessageModel
from fastapi import Body
from sqlalchemy import delete
import uuid
from typing import List
from sqlalchemy.future import select
from fastapi.responses import JSONResponse
from fastapi import Query
from sqlalchemy import desc


class CreateConversationRequest(BaseModel):
    user_id: str
    name: str = Field(default="new chat", example="new_chat")
    chat_type: str


class ConversationResponse(BaseModel):
    id: str
    name: str
    chat_type: str
    create_time: datetime


class MessageResponse(BaseModel):
    id: str = Field(...)
    conversation_id: str = Field(...)
    chat_type: str = Field(...)
    query: str = Field(...)
    response: str = Field(...)
    meta_data: dict = Field(...)
    create_time: datetime = Field(...)


class UpdateConversationRequest(BaseModel):
    name: str = Field(...)


async def create_conversation(
        request: CreateConversationRequest = Body(...),
        session: AsyncSession = Depends(get_async_db)
):
    try:
        new_conversation = ConversationModel(
            id=str(uuid.uuid4()),
            user_id=request.user_id,
            name=request.name,
            chat_type=request.chat_type,
            create_time=datetime.now()
        )
        session.add(new_conversation)
        await session.commit()
        await session.refresh(new_conversation)

        return JSONResponse(
            status_code=200,
            content={"status": 200, "id": new_conversation.id}
        )
    except Exception as e:
        await session.rollback()
        raise HTTPException(status_code=400, detail=f"Error creating conversation: {str(e)}")


async def get_user_conversations(
        user_id: str,
        chat_type: str = Query(...),
        session: AsyncSession = Depends(get_async_db)
):
    async with session as async_session:
        result = await async_session.execute(
            select(ConversationModel)
            .where(ConversationModel.user_id == user_id,
                   ConversationModel.chat_type == chat_type
                   ).order_by(desc(ConversationModel.create_time))
        )
        conversations = result.scalars().all()

        if conversations == []:
            return {"status": 200, "msg": "success", "data": []}

        else:
            data = [ConversationResponse(
                id=conv.id,
                name=conv.id,
                chat_type=conv.chat_type,
                create_time=conv.create_time
            ) for conv in conversations]

            return {"status": 200, "msg": "success", "data": data}


async def get_conversation_messages(
        conversation_id: str,
        chat_type: List[str] = Query(None),
        session: AsyncSession = Depends(get_async_db)
):
    async with (session as async_session):
        query = select(MessageModel).where(MessageModel.conversation_id == conversation_id)
        if chat_type:
            query = query.where(MessageModel.chat_type)

        result = await async_session.execute(query)
        messages = result.scalars().all()
        if not messages:
            return {"status": 200, "msg": "success", "data": []}
        else:
            data = [MessageResponse(
                id=msg.id,
                conversation_id=msg.conversation_id,
                chat_type=msg.chat_type,
                query=msg.query,
                response=msg.response,
                meta_data=msg.meta_data,
                create_time=msg.create_time
            ) for msg in messages]

            return {"status": 200, "msg": "success", "data": data}


async def delete_conversation_and_messages(
        conversation_id: str,
        session: AsyncSession = Depends(get_async_db)
):
    async with session.begin():
        conversation = await session.get(ConversationModel, conversation_id)
        if not conversation:
            raise HTTPException(status_code=404, detail="Conversation not found")

        await session.execute(
            delete(MessageModel).where(MessageModel.conversation_id == conversation_id)
        )
        await session.delete(conversation)
        await session.commit()

    return JSONResponse(
        status_code=200,
        content={"status": 200}
    )

async def update_conversation_name(
        conversation_id: str,
        request: UpdateConversationRequest,
        session: AsyncSession = Depends(get_async_db)
):
    async with session.begin():
        conversation = await session.get(ConversationModel, conversation_id)
        if not conversation:
            raise HTTPException(status_code=404, detail="Conversation not found")

        conversation.name = request.name
        session.add(conversation)
        await session.commit()

    return JSONResponse(
        status_code=200,
        content={"status": 200, "message": "Conversation name updated successfully"}
    )


