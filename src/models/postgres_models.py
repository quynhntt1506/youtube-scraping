from sqlalchemy import Column, String, Integer, DateTime, Boolean, Text, ForeignKey, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime

# Import Base from channel.py to use the same Base
from .channel import Base

class ApiKey(Base):
    __tablename__ = 'api_keys'
    
    id = Column(Integer, primary_key=True)
    api_key = Column(String(100), unique=True, nullable=False, index=True)
    quota_limit = Column(Integer, default=10000)
    quota_used = Column(Integer, default=0)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class KeywordUsage(Base):
    __tablename__ = 'keyword_usage'
    
    id = Column(Integer, primary_key=True)
    api_key_id = Column(Integer, ForeignKey('api_keys.id'), nullable=False)
    keyword = Column(String(255), nullable=False)
    used_quota = Column(Integer, default=0)
    crawl_date = Column(DateTime, default=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)

class YouTubeKeyword(Base):
    __tablename__ = 'youtube_keywords'
    
    id = Column(Integer, primary_key=True)
    keyword = Column(String(255), unique=True, nullable=False, index=True)
    status = Column(String(50), default='to_crawl')
    last_updated = Column(DateTime, default=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

# Create indexes for better performance
Index('idx_api_keys_api_key', ApiKey.api_key)
Index('idx_keywords_keyword', YouTubeKeyword.keyword)
Index('idx_keyword_usage_api_key_id', KeywordUsage.api_key_id)
Index('idx_keyword_usage_keyword', KeywordUsage.keyword) 