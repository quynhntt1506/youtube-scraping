from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field, ConfigDict
from bson import ObjectId
from src.utils.common import convert_to_datetime, format_datetime_to_iso

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
    publishedAt: Optional[datetime] = None
    updatedAt: Optional[datetime] = None
    totalReplyCount: Optional[int] = None
    replies: Optional[List['Reply']] = None
    crawlDate: datetime = Field(default_factory=datetime.now)

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
            publishedAt=datetime.fromisoformat(format_datetime_to_iso(snippet.get("publishedAt")).replace("Z", "+00:00")),
            updatedAt=convert_to_datetime(snippet.get("updatedAt")),
            totalReplyCount=int(item.get("snippet", {}).get("totalReplyCount", 0)),
            crawlDate=datetime.now()
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
    publishedAt: Optional[datetime] = None
    updatedAt: Optional[datetime] = None
    parentId: str
    crawlDate: datetime = Field(default_factory=datetime.now)

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
            publishedAt=datetime.fromisoformat(format_datetime_to_iso(snippet.get("publishedAt")).replace("Z", "+00:00")),
            updatedAt=convert_to_datetime(snippet.get("updatedAt")),
            parentId=parent_id,
            crawlDate=datetime.now()
        )

    # def _process_comment_item(self, item: dict) -> dict:
    #     """Process a single comment item from the API response."""
    #     comment = item["snippet"]["topLevelComment"]["snippet"]
        
    #     # Handle datetime parsing
    #     published_at = comment["publishedAt"]
    #     if "." in published_at:
    #         if "Z" in published_at:
    #             base = published_at.split(".")[0]
    #             published_at = f"{base}Z"
    #         elif "+" in published_at:
    #             base, tz = published_at.split("+")
    #             base = base.split(".")[0]
    #             published_at = f"{base}+{tz}"
        
    #     return {
    #         "commentId": item["id"],
    #         "videoId": comment["videoId"],
    #         "authorDisplayName": comment["authorDisplayName"],
    #         "authorChannelId": comment["authorChannelId"]["value"],
    #         "textDisplay": comment["textDisplay"],
    #         "textOriginal": comment["textOriginal"],
    #         "likeCount": comment["likeCount"],
    #         "publishedAt": datetime.fromisoformat(published_at.replace("Z", "+00:00")),
    #         "updatedAt": datetime.fromisoformat(comment["updatedAt"].replace("Z", "+00:00")),
    #         "totalReplyCount": item["snippet"]["totalReplyCount"],
    #         "replies": [],
    #         "crawlDate": datetime.now()
    #     }
        
    # def _process_reply_item(self, reply: dict) -> dict:
    #     """Process a single reply item from the API response."""
    #     reply_snippet = reply["snippet"]
        
    #     # Handle datetime parsing
    #     published_at = reply_snippet["publishedAt"]
    #     if "." in published_at:
    #         if "Z" in published_at:
    #             base = published_at.split(".")[0]
    #             published_at = f"{base}Z"
    #         elif "+" in published_at:
    #             base, tz = published_at.split("+")
    #             base = base.split(".")[0]
    #             published_at = f"{base}+{tz}"
        
    #     return {
    #         "replyId": reply["id"],
    #         "parentId": reply_snippet["parentId"],
    #         "authorDisplayName": reply_snippet["authorDisplayName"],
    #         "authorChannelId": reply_snippet["authorChannelId"]["value"],
    #         "textDisplay": reply_snippet["textDisplay"],
    #         "textOriginal": reply_snippet["textOriginal"],
    #         "likeCount": reply_snippet["likeCount"],
    #         "publishedAt": datetime.fromisoformat(published_at.replace("Z", "+00:00")),
    #         "updatedAt": datetime.fromisoformat(reply_snippet["updatedAt"].replace("Z", "+00:00")),
    #         "crawlDate": datetime.now()
    #     }