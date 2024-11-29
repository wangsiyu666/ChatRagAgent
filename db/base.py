from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base, DeclarativeMeta
from sqlalchemy.orm import sessionmaker

username = ''
hostname = ''
database_name = ''
password = ""

SQLALCHEMY_DATABASE_URI = f"mysql+asyncmy://{username}:{password}@{hostname}/{database_name}?charset=utf8mb4"


async_engine = create_async_engine(
    SQLALCHEMY_DATABASE_URI,
    echo=True
)

AsyncSessionLocal = sessionmaker(
    bind=async_engine,
    class_=AsyncSession,
    expire_on_commit=False
)

Base: DeclarativeMeta = declarative_base()
