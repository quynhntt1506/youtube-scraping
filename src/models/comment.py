from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field, ConfigDict
from bson import ObjectId
from src.utils.common import convert_to_datetime, format_datetime_to_iso, convert_datetime_to_timestamp

class Comment(BaseModel):
    """Comment model for MongoDB."""
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        populate_by_name=True,
        json_encoders={
            ObjectId: str,
            datetime: lambda dt: dt.isoformat()
        }
    )
    
    id: Optional[ObjectId] = Field(default=None, alias="_id")
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
    # publishedAt: Optional[datetime] = None
    publishedAt: Optional[int] = None
    # updatedAt: Optional[datetime] = None
    updatedAt: Optional[int] = None
    totalReplyCount: Optional[int] = None
    replies: Optional[List['Reply']] = None
    # crawlDate: datetime = Field(default_factory=datetime.now)

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
            # publishedAt=datetime.fromisoformat(format_datetime_to_iso(snippet.get("publishedAt")).replace("Z", "+00:00")),
            publishedAt=convert_datetime_to_timestamp(format_datetime_to_iso(snippet.get("publishedAt")).replace("Z", "+00:00")),
            # updatedAt=convert_to_datetime(snippet.get("updatedAt")),
            updatedAt=convert_datetime_to_timestamp(format_datetime_to_iso(snippet.get("updatedAt")).replace("Z", "+00:00")),
            totalReplyCount=int(item.get("snippet", {}).get("totalReplyCount", 0)),
            # crawlDate=datetime.now()
        )
        
        if replies:
            comment.replies = [
                Reply.from_youtube_response(reply, comment.commentId)
                for reply in replies
            ]
            
        return comment

class Reply(BaseModel):
    """Reply model for MongoDB."""
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        populate_by_name=True,
        json_encoders={
            ObjectId: str,
            datetime: lambda dt: dt.isoformat()
        }
    )
    
    id: Optional[ObjectId] = Field(default=None, alias="_id")
    commentId: str
    authorDisplayName: str
    authorProfileImageUrl: Optional[str] = None
    authorChannelId: Optional[str] = None
    textDisplay: str
    likeCount: Optional[int] = None
    # publishedAt: Optional[datetime] = None
    # updatedAt: Optional[datetime] = None
    publishedAt: Optional[int] = None
    updatedAt: Optional[int] = None
    parentId: str
    # crawlDate: datetime = Field(default_factory=datetime.now)

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
            # publishedAt=datetime.fromisoformat(format_datetime_to_iso(snippet.get("publishedAt")).replace("Z", "+00:00")),
            # updatedAt=convert_to_datetime(snippet.get("updatedAt")),
            publishedAt=convert_datetime_to_timestamp(format_datetime_to_iso(snippet.get("publishedAt")).replace("Z", "+00:00")),
            updatedAt=convert_datetime_to_timestamp(format_datetime_to_iso(snippet.get("updatedAt")).replace("Z", "+00:00")),
            parentId=parent_id,
            # crawlDate=datetime.now()
        )