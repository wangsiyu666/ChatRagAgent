import uuid

from pydantic import BaseModel, Field
from fastapi import Depends, Body, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from copilotkit.db.session import get_async_db
from passlib.hash import bcrypt
from fastapi.responses import JSONResponse
from sqlalchemy.exc import IntegrityError
from sqlalchemy.future import select

from copilotkit.db.models.user_model import UserModel
from copilotkit.db.session import with_async_session


class UserRegistrationRequest(BaseModel):
    username: str = Field(..., example="user123")
    password: str = Field(..., example="123456")


class UserLoginRequest(BaseModel):
    username: str = Field(..., example="user123")
    password: str = Field(..., example="123456")


async def register_user(
        request: UserRegistrationRequest = Body(...),
        session: AsyncSession = Depends(get_async_db)
):
    print(f"request: {request}")
    hashed_password = bcrypt.hash(request.password)

    new_user = UserModel(
        id=str(uuid.uuid4()),
        username=request.username,
        password_hash=hashed_password
    )
    try:
        session.add(new_user)
        await session.commit()
        await session.refresh(new_user)

        return JSONResponse(
            status_code=201,
            content={"statis": 200, "id": new_user, "username": new_user.username}
        )
    except IntegrityError:
        await session.rollback()
        raise HTTPException(status_code=400, detail="Username is already taken")


async def login_user(
        request: UserLoginRequest = Body(...),
        session: AsyncSession = Depends(get_async_db)
):
    user = await session.execute(select(UserModel).where(UserModel.username == request.username))
    user = user.scalar_one_or_none()

    if user and bcrypt.verify(request.password, user.password_hash):
        return JSONResponse(
            status_code=200,
            content={
                "status": 200,
                "id": user.id,
                "username": user.username,
                "message": "Login successful"
            }
        )
    else:
        return {"status": 401, "message": "用户名或密码错误"}


@with_async_session
async def check_user(session, user_id: str):
    result = await session.get(UserModel, user_id)
    if not result:
        raise HTTPException(status_code=401, detail="User ID not found")
    return {"message": "User ID exists"}
