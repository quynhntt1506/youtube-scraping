from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field, ConfigDict
from sqlalchemy import Column, String, Integer, DateTime, Text, Boolean, Index, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from src.utils.common import convert_to_datetime, format_datetime_to_iso, convert_datetime_to_timestamp
from src.config.config import STATUS_ENTITY
import json

Base = declarative_base()

class VideoSQL(Base):
    """SQLAlchemy Video model for PostgreSQL."""
    __tablename__ = 'videos'
    
    id = Column(Integer, primary_key=True)
    video_id = Column(String(50), unique=True, nullable=False, index=True)
    title = Column(String(500), nullable=False)
    description = Column(Text)
    published_at = Column(DateTime)
    channel_id = Column(String(50), ForeignKey('channels.channel_id'), nullable=False, index=True)
    channel_title = Column(String(500), nullable=False)
    thumbnail_url = Column(String(500))
    view_count = Column(Integer, default=0)
    like_count = Column(Integer, default=0)
    comment_count = Column(Integer, default=0)
    duration = Column(String(20))
    dimension = Column(String(20))
    definition = Column(String(10))
    projection = Column(String(20))
    caption = Column(Boolean)
    licensed_content = Column(Boolean)
    live_broadcast_content = Column(String(20))
    topics = Column(Text)  # JSON string
    category_id = Column(String(20))
    tags = Column(Text)  # JSON string
    playlist_id = Column(String(50))
    position = Column(Integer)
    update_status = Column(String(50))
    privacy_status = Column(String(20))
    public_stats_viewable = Column(Boolean)
    embeddable = Column(Boolean)
    made_for_kids = Column(Boolean)
    license = Column(String(50))
    status = Column(String(50), default='to_crawl')
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class Video(BaseModel):
    """Video model for PostgreSQL."""
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        populate_by_name=True,
        json_encoders={
            datetime: lambda dt: dt.isoformat()
        }
    )
    
    id: Optional[int] = Field(default=None, alias="_id")
    videoId: str
    title: str
    description: Optional[str] = None
    publishedAt: Optional[int] = None  # Unix timestamp
    channelId: str
    channelTitle: str
    thumbnailUrl: Optional[str] = None
    viewCount: Optional[int] = None
    likeCount: Optional[int] = None
    commentCount: Optional[int] = None
    duration: Optional[str] = None
    dimension: Optional[str] = None
    definition: Optional[str] = None
    projection: Optional[str] = None
    caption: Optional[bool] = None
    licensedContent: Optional[bool] = None
    liveBroadcastContent: Optional[str] = None
    topics: Optional[List[str]] = None
    categoryId: Optional[str] = None
    tags: Optional[List[str]] = None
    playlistId: Optional[str] = None
    position: Optional[int] = None
    updateStatus: Optional[str] = None
    privacyStatus: Optional[str] = None
    publicStatsViewable: Optional[bool] = None
    embeddable: Optional[bool] = None
    madeForKids: Optional[bool] = None
    license: Optional[str] = None
    status: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    @classmethod
    def from_youtube_response_playlist(cls, item: dict, playlist_id: str) -> "Video":
        """Create Video instance from YouTube API response."""
        snippet = item.get("snippet", {})
        content_details = item.get("contentDetails", {})
        
        return cls(
            videoId=content_details.get("videoId", ""),
            title=snippet.get("title", ""),
            description=snippet.get("description"),
            publishedAt=convert_datetime_to_timestamp(format_datetime_to_iso(snippet.get("publishedAt")).replace("Z", "+00:00")),
            channelId=snippet.get("channelId", ""),
            channelTitle=snippet.get("channelTitle", ""),
            thumbnailUrl=snippet.get("thumbnails", {}).get("high", {}).get("url"),
            playlistId=playlist_id,
            position=snippet.get("position", ""),
            status=STATUS_ENTITY["to_crawl"]
        )
    
    @classmethod
    def from_youtube_response_detail(cls, item: dict) -> "Video":
        """Create Video instance from YouTube API video details response."""
        snippet = item.get("snippet", {})
        statistics = item.get("statistics", {})
        content_details = item.get("contentDetails", {})
        status = item.get("status", {})
        topic_details = item.get("topicDetails", {})
        
        return cls(
            videoId=item.get("id", ""),
            title=snippet.get("title", ""),
            description=snippet.get("description"),
            publishedAt=convert_datetime_to_timestamp(format_datetime_to_iso(snippet.get("publishedAt")).replace("Z", "+00:00")),
            channelId=snippet.get("channelId", ""),
            channelTitle=snippet.get("channelTitle", ""),
            thumbnailUrl=snippet.get("thumbnails", {}).get("high", {}).get("url"),
            viewCount=int(statistics.get("viewCount", 0)),
            likeCount=int(statistics.get("likeCount", 0)),
            commentCount=int(statistics.get("commentCount", 0)),
            duration=content_details.get("duration"),
            dimension=content_details.get("dimension"),
            definition=content_details.get("definition"),
            projection=content_details.get("projection"),
            caption=content_details.get("caption"),
            licensedContent=content_details.get("licensedContent"),
            liveBroadcastContent=snippet.get("liveBroadcastContent"),
            tags=snippet.get("tags", []),
            categoryId=snippet.get("categoryId"),
            topics=topic_details.get("topicCategories", []),
            updateStatus=status.get("updateStatus"),
            privacyStatus=status.get("privacyStatus"),
            license=status.get("license"),
            publicStatsViewable=status.get("publicStatsViewable"),
            embeddable=status.get("embeddable"),
            madeForKids=status.get("madeForKids"),
            status=STATUS_ENTITY["crawled_video"]
        )

    def to_sql_dict(self) -> dict:
        """Convert to dictionary format for SQLAlchemy."""
        return {
            "video_id": self.videoId,
            "title": self.title,
            "description": self.description,
            "published_at": datetime.fromtimestamp(self.publishedAt) if self.publishedAt else None,
            "channel_id": self.channelId,
            "channel_title": self.channelTitle,
            "thumbnail_url": self.thumbnailUrl,
            "view_count": self.viewCount,
            "like_count": self.likeCount,
            "comment_count": self.commentCount,
            "duration": self.duration,
            "dimension": self.dimension,
            "definition": self.definition,
            "projection": self.projection,
            "caption": self.caption,
            "licensed_content": self.licensedContent,
            "live_broadcast_content": self.liveBroadcastContent,
            "topics": json.dumps(self.topics) if self.topics else None,
            "category_id": self.categoryId,
            "tags": json.dumps(self.tags) if self.tags else None,
            "playlist_id": self.playlistId,
            "position": self.position,
            "update_status": self.updateStatus,
            "privacy_status": self.privacyStatus,
            "public_stats_viewable": self.publicStatsViewable,
            "embeddable": self.embeddable,
            "made_for_kids": self.madeForKids,
            "license": self.license,
            "status": self.status
        }

