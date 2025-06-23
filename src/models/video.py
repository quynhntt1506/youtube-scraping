from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field, ConfigDict
from bson import ObjectId
from src.utils.common import convert_to_datetime, format_datetime_to_iso, convert_datetime_to_timestamp
from src.config.config import STATUS_ENTITY

class Video(BaseModel):
    """Video model for MongoDB."""
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        populate_by_name=True,
        json_encoders={
            ObjectId: str,
            datetime: lambda dt: dt.isoformat()
        }
    )
    
    id: Optional[ObjectId] = Field(default=None, alias="_id")
    videoId: str
    title: str
    description: Optional[str] = None
    # publishedAt: Optional[datetime] = None
    publishedAt: Optional[int] = None
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
    # crawlDate: datetime = Field(default_factory=datetime.now)
    updateStatus: Optional[str] = None
    privacyStatus: Optional[str] = None
    publicStatsViewable: Optional[bool] = None
    embeddable: Optional[bool] = None
    madeForKids: Optional[bool] = None
    license: Optional[str] = None
    status: Optional[str] = None
    
    
    @classmethod
    def from_youtube_response_playlist(cls, item: dict, playlist_id: str) -> "Video":
        """Create Video instance from YouTube API response."""
        snippet = item.get("snippet", {})
        content_details = item.get("contentDetails", {})
        
        return cls(
            videoId=content_details.get("videoId", ""),
            title=snippet.get("title", ""),
            description=snippet.get("description"),
            # publishedAt=datetime.fromisoformat(format_datetime_to_iso(snippet.get("publishedAt")).replace("Z", "+00:00")),
            publishedAt=convert_datetime_to_timestamp(format_datetime_to_iso(snippet.get("publishedAt")).replace("Z", "+00:00")),
            channelId=snippet.get("channelId", ""),
            channelTitle=snippet.get("channelTitle", ""),
            thumbnailUrl=snippet.get("thumbnails", {}).get("high", {}).get("url"),
            playlistId=playlist_id,
            position=snippet.get("position", ""),
            # crawlDate=datetime.now(),
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
            # publishedAt=datetime.fromisoformat(format_datetime_to_iso(snippet.get("publishedAt")).replace("Z", "+00:00")),
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
            # crawlDate=datetime.now(),
            status=STATUS_ENTITY["crawled_video"]
        )

