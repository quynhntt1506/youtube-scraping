from pymongo import MongoClient
from typing import Dict, Any
from datetime import datetime
from config.config import MONGODB_URI, MONGODB_DB, MONGODB_COLLECTIONS

class Database:
    def __init__(self):
        self.client = MongoClient(MONGODB_URI)
        self.db = self.client[MONGODB_DB]
        self.collections = {
            name: self.db[collection_name]
            for name, collection_name in MONGODB_COLLECTIONS.items()
        }

    def channel_exists(self, channel_id: str) -> bool:
        """Check if a channel exists in the database."""
        return bool(self.collections["channels"].find_one({"channelId": channel_id}))

    def video_exists(self, video_id: str) -> bool:
        """Check if a video exists in the database."""
        return bool(self.collections["videos"].find_one({"videoId": video_id}))

    def insert_channel(self, channel_data: Dict[str, Any]) -> None:
        """Insert a channel document if it doesn't exist."""
        if not self.channel_exists(channel_data["channelId"]):
            self.collections["channels"].insert_one(channel_data)

    def insert_video(self, video_data: Dict[str, Any]) -> None:
        """Insert a video document if it doesn't exist."""
        if not self.video_exists(video_data["videoId"]):
            self.collections["videos"].insert_one(video_data)

    def update_keyword_data(self, keyword: str, channels: list, videos: list, count_channels_from_api: int, count_videos_from_api: int) -> None:
        """Update or insert keyword data."""
        existing_doc = self.collections["keywords"].find_one({"keyword": keyword})
        
        if existing_doc:
            existing_channels = existing_doc.get("channels", [])
            existing_videos = existing_doc.get("videos", [])
            
            # Filter out duplicates
            new_channels = [ch for ch in channels if ch["channelId"] not in 
                          {ch["channelId"] for ch in existing_channels}]
            new_videos = [v for v in videos if v["videoId"] not in 
                         {v["videoId"] for v in existing_videos}]
            
            # Update document
            self.collections["keywords"].update_one(
                {"keyword": keyword},
                {
                    "$set": {
                        "channels": existing_channels + new_channels,
                        "videos": existing_videos + new_videos,
                        "count_channels": len(existing_channels + new_channels),
                        "count_videos": len(existing_videos + new_videos),
                        "count_channels_from_api": count_channels_from_api,
                        "count_videos_from_api": count_videos_from_api,
                        "last_updated": datetime.now(),
                    }
                }
            )
        else:
            # Insert new document
            self.collections["keywords"].insert_one({
                "keyword": keyword,
                "channels": channels,
                "videos": videos,
                "count_channels": len(channels),
                "count_videos": len(videos),
                "count_channels_from_api": count_channels_from_api,
                "count_videos_from_api": count_videos_from_api,
                "last_updated": datetime.now(),
            })

    def close(self):
        """Close the MongoDB connection."""
        self.client.close() 
        self.client.close() 