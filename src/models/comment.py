from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field, ConfigDict
from sqlalchemy import Column, String, Integer, DateTime, Text, Boolean, Index, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from src.utils.common import convert_to_datetime, format_datetime_to_iso, convert_datetime_to_timestamp

Base = declarative_base()

class CommentSQL(Base):
    """SQLAlchemy Comment model for PostgreSQL."""
    __tablename__ = 'comments'
    
    id = Column(Integer, primary_key=True)
    comment_id = Column(String(50), unique=True, nullable=False, index=True)
    video_id = Column(String(50), ForeignKey('videos.video_id'), nullable=False, index=True)
    author_display_name = Column(String(255), nullable=False)
    author_profile_image_url = Column(String(500))
    author_channel_id = Column(String(50))
    author_channel_url = Column(String(500))
    text_display = Column(Text, nullable=False)
    like_count = Column(Integer, default=0)
    is_public = Column(Boolean, default=True)
    can_reply = Column(Boolean, default=True)
    published_at = Column(DateTime)
    updated_at = Column(DateTime)
    total_reply_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at_meta = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class ReplySQL(Base):
    """SQLAlchemy Reply model for PostgreSQL."""
    __tablename__ = 'replies'
    
    id = Column(Integer, primary_key=True)
    comment_id = Column(String(50), unique=True, nullable=False, index=True)
    author_display_name = Column(String(255), nullable=False)
    author_profile_image_url = Column(String(500))
    author_channel_id = Column(String(50))
    text_display = Column(Text, nullable=False)
    like_count = Column(Integer, default=0)
    published_at = Column(DateTime)
    updated_at = Column(DateTime)
    parent_id = Column(String(50), ForeignKey('comments.comment_id'), nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at_meta = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class Comment(BaseModel):
    """Comment model for PostgreSQL."""
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        populate_by_name=True,
        json_encoders={
            datetime: lambda dt: dt.isoformat()
        }
    )
    
    id: Optional[int] = Field(default=None, alias="_id")
    commentId: str
    videoId: str
    authorDisplayName: str
    authorProfileImageUrl: Optional[str] = None
    authorChannelId: Optional[str] = None
    authorChannelUrl: Optional[str] = None
    textDisplay: str
    likeCount: Optional[int] = None
    isPublic: Optional[bool] = None
    canReply: Optional[bool] = None
    publishedAt: Optional[int] = None  # Unix timestamp
    updatedAt: Optional[int] = None  # Unix timestamp
    totalReplyCount: Optional[int] = None
    replies: Optional[List['Reply']] = None
    created_at: Optional[datetime] = None
    updated_at_meta: Optional[datetime] = None

    @classmethod
    def from_youtube_response(cls, item: dict) -> 'Comment':
        """Create a Comment instance from YouTube API response."""
        snippet = item.get("snippet", {}).get("topLevelComment", {}).get("snippet", {})
        replies = item.get("replies", {}).get("comments", [])
        
        comment = cls(
            commentId=item.get("id", ""),
            videoId=snippet.get("videoId", ""),
            authorDisplayName=snippet.get("authorDisplayName", ""),
            authorProfileImageUrl=snippet.get("authorProfileImageUrl"),
            authorChannelId=snippet.get("authorChannelId", {}).get("value"),
            authorChannelUrl=snippet.get("authorChannelUrl", ""),
            textDisplay=snippet.get("textDisplay", ""),
            likeCount=int(snippet.get("likeCount", 0)),
            isPublic=item.get("isPublic", True),
            canReply=item.get("canReply", True),
            publishedAt=convert_datetime_to_timestamp(format_datetime_to_iso(snippet.get("publishedAt")).replace("Z", "+00:00")),
            updatedAt=convert_datetime_to_timestamp(format_datetime_to_iso(snippet.get("updatedAt")).replace("Z", "+00:00")),
            totalReplyCount=int(item.get("snippet", {}).get("totalReplyCount", 0)),
        )
        
        if replies:
            comment.replies = [
                Reply.from_youtube_response(reply, comment.commentId)
                for reply in replies
            ]
            
        return comment

    def to_sql_dict(self) -> dict:
        """Convert to dictionary format for SQLAlchemy."""
        return {
            "comment_id": self.commentId,
            "video_id": self.videoId,
            "author_display_name": self.authorDisplayName,
            "author_profile_image_url": self.authorProfileImageUrl,
            "author_channel_id": self.authorChannelId,
            "author_channel_url": self.authorChannelUrl,
            "text_display": self.textDisplay,
            "like_count": self.likeCount,
            "is_public": self.isPublic,
            "can_reply": self.canReply,
            "published_at": datetime.fromtimestamp(self.publishedAt) if self.publishedAt else None,
            "updated_at": datetime.fromtimestamp(self.updatedAt) if self.updatedAt else None,
            "total_reply_count": self.totalReplyCount
        }

class Reply(BaseModel):
    """Reply model for PostgreSQL."""
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        populate_by_name=True,
        json_encoders={
            datetime: lambda dt: dt.isoformat()
        }
    )
    
    id: Optional[int] = Field(default=None, alias="_id")
    commentId: str
    authorDisplayName: str
    authorProfileImageUrl: Optional[str] = None
    authorChannelId: Optional[str] = None
    textDisplay: str
    likeCount: Optional[int] = None
    publishedAt: Optional[int] = None  # Unix timestamp
    updatedAt: Optional[int] = None  # Unix timestamp
    parentId: str
    created_at: Optional[datetime] = None
    updated_at_meta: Optional[datetime] = None

    @classmethod
    def from_youtube_response(cls, item: dict, parent_id: str) -> 'Reply':
        """Create a Reply instance from YouTube API response."""
        snippet = item.get("snippet", {})
        
        return cls(
            commentId=item.get("id", ""),
            authorDisplayName=snippet.get("authorDisplayName", ""),
            authorProfileImageUrl=snippet.get("authorProfileImageUrl"),
            authorChannelId=snippet.get("authorChannelId", {}).get("value"),
            textDisplay=snippet.get("textDisplay", ""),
            likeCount=int(snippet.get("likeCount", 0)),
            publishedAt=convert_datetime_to_timestamp(format_datetime_to_iso(snippet.get("publishedAt")).replace("Z", "+00:00")),
            updatedAt=convert_datetime_to_timestamp(format_datetime_to_iso(snippet.get("updatedAt")).replace("Z", "+00:00")),
            parentId=parent_id,
        )

    def to_sql_dict(self) -> dict:
        """Convert to dictionary format for SQLAlchemy."""
        return {
            "comment_id": self.commentId,
            "author_display_name": self.authorDisplayName,
            "author_profile_image_url": self.authorProfileImageUrl,
            "author_channel_id": self.authorChannelId,
            "text_display": self.textDisplay,
            "like_count": self.likeCount,
            "published_at": datetime.fromtimestamp(self.publishedAt) if self.publishedAt else None,
            "updated_at": datetime.fromtimestamp(self.updatedAt) if self.updatedAt else None,
            "parent_id": self.parentId
        }