import re
from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field, ConfigDict
from bson import ObjectId
from src.utils.common import convert_to_datetime, format_datetime_to_iso, convert_datetime_to_timestamp
from src.config.config import STATUS_ENTITY

class Channel(BaseModel):
    """Channel model for MongoDB."""
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        populate_by_name=True,
        json_encoders={
            ObjectId: str,
            datetime: lambda dt: dt.isoformat()
        }
    )
    
    id: Optional[ObjectId] = Field(default=None, alias="_id")
    channelId: str
    title: str
    description: Optional[str] = None
    customUrl: Optional[str] = None
    # publishedAt: Optional[datetime] = None
    publishedAt: Optional[int] = None
    country: Optional[str] = None
    subscriberCount: Optional[int] = None
    videoCount: Optional[int] = None
    viewCount: Optional[int] = None
    topics: Optional[List[str]] = None
    email: Optional[str] = None
    avatarUrl: Optional[str] = None
    bannerUrl: Optional[str] = None
    playlistId: Optional[str] = None
    # crawlDate: datetime = Field(default_factory=datetime.now)
    status: Optional[str] = None

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
            # publishedAt=datetime.fromisoformat(format_datetime_to_iso(snippet.get("publishedAt")).replace("Z", "+00:00")),
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
            # crawlDate=datetime.now(),
            status=STATUS_ENTITY["crawled_channel"]
        )
