from sqlalchemy import Column, Integer, String, DateTime, func
from .base import Base

class KeywordSearch(Base):
    __tablename__ = 'youtube_keywords'
    id = Column(Integer, primary_key=True)
    keyword = Column(String(255), unique=True, nullable=False, index=True)
    status = Column(String(50), nullable=False, index=True)
    type = Column(String(50), nullable=False, index=True)
    last_update = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)

    def __repr__(self):
        return f"<KeywordSearch(id={self.id}, keyword='{self.keyword}', status={self.status}, type={self.type})>"

    def to_dict(self):
        return {
            "id": self.id,
            "keyword": self.keyword,
            "status": self.status,
            "type": self.type,
            "last_update": self.last_update.isoformat() if self.last_update else None
        } 