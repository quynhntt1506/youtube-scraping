import re
from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field, ConfigDict
from sqlalchemy import Column, String, Integer, DateTime, Text, Index
from sqlalchemy.ext.declarative import declarative_base
from src.utils.common import convert_to_datetime, format_datetime_to_iso, convert_datetime_to_timestamp
from src.config.config import STATUS_ENTITY
import json

Base = declarative_base()

class ChannelSQL(Base):
    """SQLAlchemy Channel model for PostgreSQL."""
    __tablename__ = 'channels'
    
    id = Column(Integer, primary_key=True)
    channel_id = Column(String(50), unique=True, nullable=False, index=True)
    title = Column(String(500), nullable=False)
    description = Column(Text)
    custom_url = Column(String(100))
    published_at = Column(DateTime)
    country = Column(String(10))
    subscriber_count = Column(Integer, default=0)
    video_count = Column(Integer, default=0)
    view_count = Column(Integer, default=0)
    topics = Column(Text)  # JSON string
    email = Column(String(255))
    avatar_url = Column(String(500))
    banner_url = Column(String(500))
    playlist_id = Column(String(50))
    status = Column(String(50), default='to_crawl')
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class Channel(BaseModel):
    """Channel model for PostgreSQL."""
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        populate_by_name=True,
        json_encoders={
            datetime: lambda dt: dt.isoformat()
        }
    )
    
    id: Optional[int] = Field(default=None, alias="_id")
    channelId: str
    title: str
    description: Optional[str] = None
    customUrl: Optional[str] = None
    publishedAt: Optional[int] = None  # Unix timestamp
    country: Optional[str] = None
    subscriberCount: Optional[int] = None
    videoCount: Optional[int] = None
    viewCount: Optional[int] = None
    topics: Optional[List[str]] = None
    email: Optional[str] = None
    avatarUrl: Optional[str] = None
    bannerUrl: Optional[str] = None
    playlistId: Optional[str] = None
    status: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @staticmethod
    def extract_email(text: str) -> Optional[str]:
        """Extract email from text."""
        if not text:
            return None
        email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        match = re.search(email_pattern, text)
        return match.group(0) if match else None

    @classmethod
    def from_youtube_response(cls, item: dict) -> "Channel":
        """Create Channel instance from YouTube API response."""
        snippet = item.get("snippet", {})
        statistics = item.get("statistics", {})
        topic_details = item.get("topicDetails", {})
        branding = item.get("brandingSettings", {})
        content_details = item.get("contentDetails", {})
        
        description = snippet.get("description", "")
        
        return cls(
            channelId=item.get("id", ""),
            title=snippet.get("title", ""),
            description=description,
            customUrl=snippet.get("customUrl"),
            publishedAt=convert_datetime_to_timestamp(format_datetime_to_iso(snippet.get("publishedAt")).replace("Z", "+00:00")),
            country=snippet.get("country"),
            subscriberCount=int(statistics.get("subscriberCount", 0)),
            videoCount=int(statistics.get("videoCount", 0)),
            viewCount=int(statistics.get("viewCount", 0)),
            topics=topic_details.get("topicCategories", []),
            email=cls.extract_email(description),
            avatarUrl=snippet.get("thumbnails", {}).get("default", {}).get("url"),
            bannerUrl=branding.get("image", {}).get("bannerExternalUrl"),
            playlistId=content_details.get("relatedPlaylists", {}).get("uploads"),
            status=STATUS_ENTITY["crawled_channel"]
        )

    def to_sql_dict(self) -> dict:
        """Convert to dictionary format for SQLAlchemy."""
        return {
            "channel_id": self.channelId,
            "title": self.title,
            "description": self.description,
            "custom_url": self.customUrl,
            "published_at": datetime.fromtimestamp(self.publishedAt) if self.publishedAt else None,
            "country": self.country,
            "subscriber_count": self.subscriberCount,
            "video_count": self.videoCount,
            "view_count": self.viewCount,
            "topics": json.dumps(self.topics) if self.topics else None,
            "email": self.email,
            "avatar_url": self.avatarUrl,
            "banner_url": self.bannerUrl,
            "playlist_id": self.playlistId,
            "status": self.status
        }
