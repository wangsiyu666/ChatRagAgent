from sqlalchemy import Column, Integer, String, DateTime, JSON, ForeignKey, func, CHAR
from sqlalchemy.orm import relationship
from copilotkit.db.base import Base


class MessageModel(Base):
    __tablename__ = "message"
    id = Column(CHAR(36), primary_key=True, comment="聊天记录ID")
    conversation_id = Column(CHAR(36), ForeignKey('conversation.id'), comment='会话ID')

    chat_type = Column(String(50), comment="聊天类型")
    query = Column(String(4096), comment='用户问题')
    response = Column(String(4096), comment="模型回答")

    metadata = Column(String(4096), default="")

    feedback_score = Column(Integer, default=-1, comment='用户评分')
    feedback_reason = Column(String(255), default="", comment='用户评分理由')
    create_time = Column(DateTime, default=func.now(), comment='创建时间')


    def __repr__(self):
        return f"<Message(id='{self.id}', chat_type='{self.chat_type}', query='{self.query}', response='{self.response}', meta_data='{self.meta_data}', feedback_score='{self.feedback_score}', feedback_reason='{self.feedback_reason}', create_time='{self.create_time}')>"
