from sqlalchemy import Column, String, CHAR
from sqlalchemy.orm import relationship
from copilotkit.db.base import Base

class UserModel(Base):
    __tablename__ = "user"
    id = Column(CHAR(36), primary_key=True, comment="用户ID")
    username = Column(String(255), unique=True, comment="用户名")
    password_hash = Column(String(255), comment="密码的哈希值")

    conversations = relationship("ConversationModel", back_populates='user')
    knowledge_bases = relationship('KnowledgeBaseModel', back_populates='user', cascade='all, delete-orphan')

    def __repr__(self):
        return f"<User(id='{self.id}', username='{self.username}')>"


