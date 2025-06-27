from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

# Import Base from channel.py để sử dụng cùng Base
from .base import Base

class YouTubeApiKey(Base):
    """SQLAlchemy model cho YouTube API keys với cấu trúc đơn giản."""
    __tablename__ = 'youtube_api_keys'
    
    id = Column(Integer, primary_key=True)
    email = Column(String(255), nullable=True)  # Email liên kết với key
    api_key = Column(String(100), unique=True, nullable=False, index=True)
    remaining_quota = Column(Integer, default=10000, nullable=False)
    status = Column(String(20), default='active', nullable=False, index=True)  # 'active' hoặc 'unactive'
    last_updated = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    
    def __repr__(self):
        return f"<YouTubeApiKey(id={self.id}, api_key='{self.api_key[:10]}...', status={self.status})>"
    
    @property
    def is_active(self) -> bool:
        """Kiểm tra xem API key có active không."""
        return self.status == 'active'
    
    @property
    def is_exhausted(self) -> bool:
        """Kiểm tra xem API key có hết quota không."""
        return self.remaining_quota <= 0
    
    def use_quota(self, amount: int) -> bool:
        """
        Sử dụng quota và cập nhật key.
        
        Args:
            amount (int): Số lượng quota cần sử dụng
            
        Returns:
            bool: True nếu sử dụng thành công, False nếu không đủ quota
        """
        if self.remaining_quota < amount:
            return False
        
        self.remaining_quota -= amount
        self.last_updated = datetime.utcnow()
        
        # Cập nhật status nếu hết quota
        if self.remaining_quota <= 0:
            self.status = 'unactive'
            
        return True
    
    def reset_quota(self, new_quota: int = 10000) -> None:
        """
        Reset quota và kích hoạt key.
        
        Args:
            new_quota (int): Số lượng quota mới (mặc định: 10000)
        """
        self.remaining_quota = new_quota
        self.status = 'active'
        self.last_updated = datetime.utcnow()
    
    def to_dict(self) -> dict:
        """Chuyển đổi thành dictionary."""
        return {
            "id": self.id,
            "email": self.email,
            "api_key": self.api_key,
            "remaining_quota": self.remaining_quota,
            "status": self.status,
            "last_updated": self.last_updated.isoformat() if self.last_updated else None
        } 