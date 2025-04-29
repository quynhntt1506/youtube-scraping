from pymongo import MongoClient
from typing import Dict, Any, List
from datetime import datetime
from config.config import MONGODB_URI, MONGODB_DB, MONGODB_COLLECTIONS
import pymongo

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

    def update_keyword_data(self, keyword: str, channels: list, videos: list, count_channels_from_api: int, count_videos_from_api: int) -> Dict[str, Any]:
        """Update or insert keyword data.
        
        Args:
            keyword (str): Keyword to update/insert
            channels (list): List of new channels
            videos (list): List of new videos
            count_channels_from_api (int): Total number of channels from API
            count_videos_from_api (int): Total number of videos from API
            
        Returns:
            Dict[str, Any]: Dictionary containing keyword data and statistics
        """
        current_time = datetime.now()
        existing_doc = self.collections["keywords"].find_one({"keyword": keyword})
        
        if existing_doc:
            # Get all existing channels and videos from crawl_history
            existing_channels = []
            existing_videos = []
            for result in existing_doc.get("crawl_history", []):
                existing_channels.extend(result.get("channels", []))
                existing_videos.extend(result.get("videos", []))
            
            # Filter out duplicates
            new_channels = [ch for ch in channels if ch["channelId"] not in 
                          {ch["channelId"] for ch in existing_channels}]
            new_videos = [v for v in videos if v["videoId"] not in 
                         {v["videoId"] for v in existing_videos}]
            
            # Create crawl result object with new data only
            crawl_result = {
                "channels": new_channels,
                "videos": new_videos,
                "count_channels": len(new_channels),
                "count_videos": len(new_videos),
                "count_channels_from_api": count_channels_from_api,
                "count_videos_from_api": count_videos_from_api,
                "crawlDate": current_time
            }
            
            # Get existing crawl results
            existing_crawl_history = existing_doc.get("crawl_history", [])
            
            # Add new crawl result
            existing_crawl_history.append(crawl_result)
            
            # Update document
            self.collections["keywords"].update_one(
                {"keyword": keyword},
                {
                    "$set": {
                        "crawl_history": existing_crawl_history,
                        "last_updated": current_time
                    }
                }
            )
            
            # Return the requested information
            return {
                "count_channels": len(new_channels),
                "count_videos": len(new_videos),
                "count_channels_from_api": count_channels_from_api,
                "count_videos_from_api": count_videos_from_api,
                "keyword": keyword,
                "_id": str(existing_doc["_id"]),
                "crawlDate": current_time
            }
        else:
            # Create crawl result object with all data for new document
            crawl_result = {
                "channels": channels,
                "videos": videos,
                "count_channels": len(channels),
                "count_videos": len(videos),
                "count_channels_from_api": count_channels_from_api,
                "count_videos_from_api": count_videos_from_api,
                "crawlDate": current_time
            }
            
            # Insert new document
            result = self.collections["keywords"].insert_one({
                "keyword": keyword,
                "crawl_history": [crawl_result],
                "last_updated": current_time
            })
            
            # Return the requested information
            return {
                "count_channels": len(channels),
                "count_videos": len(videos),
                "count_channels_from_api": count_channels_from_api,
                "count_videos_from_api": count_videos_from_api,
                "keyword": keyword,
                "_id": str(result.inserted_id),
                "crawlDate": current_time
            }

    def update_many_keywords(self, keywords_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Update or insert multiple keywords data in a single operation.
        
        Args:
            keywords_data (List[Dict[str, Any]]): List of keyword data to update/insert
                Each item should contain:
                - keyword (str): Keyword to update/insert
                - channels (list): List of new channels
                - videos (list): List of new videos
                - count_channels_from_api (int): Total number of channels from API
                - count_videos_from_api (int): Total number of videos from API
                
        Returns:
            List[Dict[str, Any]]: List of results for each keyword
        """
        if not keywords_data:
            return []
            
        try:
            current_time = datetime.now()
            results = []
            operations = []
            
            for data in keywords_data:
                keyword = data.get("keyword")
                channels = data.get("channels", [])
                videos = data.get("videos", [])
                count_channels_from_api = data.get("count_channels_from_api", 0)
                count_videos_from_api = data.get("count_videos_from_api", 0)
                
                if not keyword:
                    continue
                    
                # Create crawl result object
                crawl_result = {
                    "channels": channels,
                    "videos": videos,
                    "count_channels": len(channels),
                    "count_videos": len(videos),
                    "count_channels_from_api": count_channels_from_api,
                    "count_videos_from_api": count_videos_from_api,
                    "crawlDate": current_time
                }
                
                # Create update operation
                operations.append(
                    pymongo.UpdateOne(
                        {"keyword": keyword},
                        {
                            "$set": {
                                "last_updated": current_time
                            },
                            "$push": {
                                "crawl_history": crawl_result
                            }
                        },
                        upsert=True
                    )
                )
                
                # Add to results
                results.append({
                    "count_channels": len(channels),
                    "count_videos": len(videos),
                    "count_channels_from_api": count_channels_from_api,
                    "count_videos_from_api": count_videos_from_api,
                    "keyword": keyword,
                    "crawlDate": current_time
                })
            
            if operations:
                # Execute bulk write
                result = self.collections["keywords"].bulk_write(operations, ordered=False)
                print(f"✅ Processed {len(operations)} keywords successfully")
                print(f"ℹ️ Inserted {result.upserted_count} new keywords")
                print(f"ℹ️ Updated {result.modified_count} existing keywords")
                
                # Get inserted ids and update results
                if result.upserted_ids:
                    for idx, doc_id in result.upserted_ids.items():
                        results[idx]["_id"] = str(doc_id)
                
                return results
            else:
                print("ℹ️ No keywords to process")
                return []
                
        except Exception as e:
            print(f"❌ Error processing keywords: {str(e)}")
            raise

    def close(self):
        """Close the MongoDB connection."""
        self.client.close()

    def insert_many_videos(self, videos: List[dict]) -> Dict[str, Any]:
        """Insert multiple videos into database using update_many with upsert.
        Duplicate videos will be automatically handled by MongoDB.
        
        Args:
            videos (List[dict]): List of video documents to insert
            
        Returns:
            Dict[str, Any]: Dictionary containing count of new/updated videos and list of new video ids
        """
        if not videos:
            return {
                "new_videos_count": 0,
                "updated_videos_count": 0,
                "new_video_ids": []
            }
            
        try:
            # Create bulk operations
            operations = []
            
            for video in videos:
                video_id = video.get("videoId")
                if not video_id:
                    continue
                    
                operations.append(
                    pymongo.UpdateOne(
                        {"videoId": video_id},  # Filter by videoId
                        {"$set": video},        # Update with full document
                        upsert=True             # Insert if not exists
                    )
                )
            
            if operations:
                # Execute bulk write
                result = self.collections["videos"].bulk_write(operations, ordered=False)
                print(f"✅ Inserted {result.upserted_count} new videos successfully")
                print(f"ℹ️ Updated {result.modified_count} existing videos")
                
                # Get list of new video ids from upserted_ids
                new_video_ids = []
                if result.upserted_ids:
                    new_video_ids = list(result.upserted_ids.values())
                
                return {
                    "new_videos_count": result.upserted_count,
                    "updated_videos_count": result.modified_count,
                    "new_video_ids": new_video_ids
                }
            else:
                print("ℹ️ No videos to process")
                return {
                    "new_videos_count": 0,
                    "updated_videos_count": 0,
                    "new_video_ids": []
                }
                
        except Exception as e:
            print(f"❌ Error processing videos: {str(e)}")
            raise

    def add_many_keyword_usage(self, api_key: str, keywords_data: List[Dict[str, Any]]) -> None:
        """Add multiple keyword usage history to api_key's used_history array.
        
        Args:
            api_key (str): API key to update
            keywords_data (List[Dict[str, Any]]): List of keyword usage data
                Each item should contain:
                - keyword (str): The keyword that was crawled
                - used_quota (int): Quota used for this keyword
                - crawl_date (datetime): Date of crawling
        """
        if not api_key or not keywords_data:
            return
            
        try:
            # Create update operations
            operations = []
            for data in keywords_data:
                keyword = data.get("keyword")
                used_quota = data.get("used_quota", 0)
                crawl_date = data.get("crawl_date")
                
                if not all([keyword, used_quota, crawl_date]):
                    continue
                    
                # Convert datetime to string if needed
                if isinstance(crawl_date, datetime):
                    crawl_date = crawl_date.isoformat()
                    
                operations.append(
                    pymongo.UpdateOne(
                        {"api_key": api_key},
                        {
                            "$push": {
                                "used_history": {
                                    "keyword": keyword,
                                    "used_quota": used_quota,
                                    "crawl_date": crawl_date
                                }
                            }
                        }
                    )
                )
            
            if operations:
                # Execute bulk write
                result = self.collections["api_keys"].bulk_write(operations, ordered=False)
                print(f"✅ Added {len(operations)} keyword usage records successfully")
                print(f"ℹ️ Updated {result.modified_count} API key documents")
                
        except Exception as e:
            print(f"❌ Error adding keyword usage history: {str(e)}")
            raise

    def insert_many_channels(self, channels: List[dict]) -> Dict[str, Any]:
        """Insert multiple channels into database using update_many with upsert.
        Duplicate channels will be automatically handled by MongoDB.
        
        Args:
            channels (List[dict]): List of channel documents to insert
            
        Returns:
            Dict[str, Any]: Dictionary containing count of new/updated channels and list of new channel ids
        """
        if not channels:
            return {
                "new_channels_count": 0,
                "updated_channels_count": 0,
                "new_channel_ids": []
            }
            
        try:
            # Create bulk operations
            operations = []
            
            for channel in channels:
                channel_id = channel.get("channelId")
                if not channel_id:
                    continue
                    
                operations.append(
                    pymongo.UpdateOne(
                        {"channelId": channel_id},  # Filter by channelId
                        {"$set": channel},          # Update with full document
                        upsert=True                 # Insert if not exists
                    )
                )
            
            if operations:
                # Execute bulk write
                result = self.collections["channels"].bulk_write(operations, ordered=False)
                print(f"✅ Inserted {result.upserted_count} new channels successfully")
                print(f"ℹ️ Updated {result.modified_count} existing channels")
                
                # Get list of new channel ids from upserted_ids
                new_channel_ids = []
                if result.upserted_ids:
                    new_channel_ids = list(result.upserted_ids.values())
                
                return {
                    "new_channels_count": result.upserted_count,
                    "updated_channels_count": result.modified_count,
                    "new_channel_ids": new_channel_ids
                }
            else:
                print("ℹ️ No channels to process")
                return {
                    "new_channels_count": 0,
                    "updated_channels_count": 0,
                    "new_channel_ids": []
                }
                
        except Exception as e:
            print(f"❌ Error processing channels: {str(e)}")
            raise 