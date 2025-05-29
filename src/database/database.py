from pymongo import MongoClient
from typing import Dict, Any, List, Callable
from datetime import datetime
import time
from functools import wraps
from src.config.config import MONGODB_URI, MONGODB_DB, MONGODB_COLLECTIONS, STATUS_ENTITY
import pymongo

def retry_mongodb_operation(max_retries: int = 3, delay: float = 1.0):
    """Decorator to retry MongoDB operations on failure.
    
    Args:
        max_retries (int): Maximum number of retry attempts
        delay (float): Delay between retries in seconds
    """
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (pymongo.errors.ConnectionFailure, 
                        pymongo.errors.ServerSelectionTimeoutError,
                        pymongo.errors.OperationFailure) as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        time.sleep(delay * (attempt + 1))  # Exponential backoff
                        continue
                    raise last_exception
            return None
        return wrapper
    return decorator

class Database:
    def __init__(self):
        self.client = MongoClient(MONGODB_URI)
        self.db = self.client[MONGODB_DB]
        self.collections = {
            name: self.db[collection]
            for name, collection in MONGODB_COLLECTIONS.items()
        }

    @retry_mongodb_operation()
    def channel_exists(self, channel_id: str) -> bool:
        """Check if a channel exists in the database."""
        return bool(self.collections["channels"].find_one({"channelId": channel_id}))

    @retry_mongodb_operation()
    def video_exists(self, video_id: str) -> bool:
        """Check if a video exists in the database."""
        return bool(self.collections["videos"].find_one({"videoId": video_id}))

    @retry_mongodb_operation()
    def insert_channel(self, channel_data: Dict[str, Any]) -> None:
        """Insert a channel document if it doesn't exist."""
        if not self.channel_exists(channel_data["channelId"]):
            self.collections["channels"].insert_one(channel_data)

    @retry_mongodb_operation()
    def insert_video(self, video_data: Dict[str, Any]) -> None:
        """Insert a video document if it doesn't exist."""
        if not self.video_exists(video_data["videoId"]):
            self.collections["videos"].insert_one(video_data)

    @retry_mongodb_operation()
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
                    "countNewChannels": len(channels),
                    "countNewVideos": len(videos),
                    "countChannelsFromApi": count_channels_from_api,
                    "countVideosFromApi": count_videos_from_api,
                    "crawlDate": current_time
                }
                
                # Create update operation
                operations.append(
                    pymongo.UpdateOne(
                        {"keyword": keyword},
                        {
                            "$set": {
                                "lastUpdated": current_time
                            },
                            "$push": {
                                "crawlHistory": crawl_result
                            }
                        },
                        upsert=True
                    )
                )
                
                # Add to results
                results.append({
                    "countNewChannels": len(channels),
                    "countNewVideos": len(videos),
                    "countChannelsFromApi": count_channels_from_api,
                    "countVideosFromApi": count_videos_from_api,
                    "keyword": keyword,
                    "crawlDate": current_time
                })
            
            if operations:
                # Execute bulk write
                result = self.collections["youtube_crawl_history"].bulk_write(operations, ordered=False)
                
                # Get inserted ids and update results
                if result.upserted_ids:
                    for idx, doc_id in result.upserted_ids.items():
                        results[idx]["_id"] = str(doc_id)
                
                return {
                    "count_operations": len(operations),
                    "new_keywords_count": result.upserted_count,
                    "updated_keywords_count": result.modified_count,
                }
            else:
                print("ℹ️ No keywords to process")
                return {
                    "count_operations": 0,
                    "new_keywords_count": 0,
                    "updated_keywords_count": 0,
                }
                
        except Exception as e:
            print(f"❌ Error processing keywords: {str(e)}")
            raise

    def close(self):
        """Close MongoDB connection."""
        self.client.close()

    @retry_mongodb_operation()
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
                    
                # Create update document that preserves existing fields
                update_doc = {}
                for key, value in video.items():
                    if value is not None:  # Only update non-null values
                        update_doc[key] = value
                        
                operations.append(
                    pymongo.UpdateOne(
                        {"videoId": video_id},  # Filter by videoId
                        {"$set": update_doc},   # Update only non-null fields
                        upsert=True             # Insert if not exists
                    )
                )
            
            if operations:
                # Execute bulk write
                result = self.collections["videos"].bulk_write(operations, ordered=False)
                
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
                return {
                    "new_videos_count": 0,
                    "updated_videos_count": 0,
                    "new_video_ids": []
                }
                
        except Exception as e:
            print(f"❌ Error processing videos: {str(e)}")
            raise

    @retry_mongodb_operation()
    def insert_many_comments(self, comments: List[dict]) -> Dict[str, Any]:
        """Insert multiple comments into database using update_many with upsert.
        Duplicate comments will be automatically handled by MongoDB.
        
        Args:
            comments (List[dict]): List of comment documents to insert
            
        Returns:
            Dict[str, Any]: Dictionary containing:
                - new_comments_count: Number of new comments inserted
                - updated_comments_count: Number of existing comments updated
                - new_comment_ids: List of IDs for newly inserted comments
                - error_count: Number of comments that failed to insert
                - errors: List of error messages
        """
        if not comments:
            return {
                "new_comments_count": 0,
                "updated_comments_count": 0,
                "new_comment_ids": [],
                "error_count": 0,
                "errors": []
            }
            
        try:
            # Create bulk operations
            operations = []
            error_count = 0
            errors = []
            
            for comment in comments:
                comment_id = comment.get("commentId")
                if not comment_id:
                    error_count += 1
                    errors.append("Comment missing commentId")
                    continue
                    
                operations.append(
                    pymongo.UpdateOne(
                        {"commentId": comment_id},  # Filter by commentId
                        {"$set": comment},          # Update with full document
                        upsert=True                 # Insert if not exists
                    )
                )
            
            if operations:
                # Execute bulk write
                result = self.collections["comments"].bulk_write(operations, ordered=False)
                
                # Get list of new comment ids from upserted_ids
                new_comment_ids = []
                if result.upserted_ids:
                    new_comment_ids = list(result.upserted_ids.values())
                
                return {
                    "new_comments_count": result.upserted_count,
                    "updated_comments_count": result.modified_count,
                    "new_comment_ids": new_comment_ids,
                    "error_count": error_count,
                    "errors": errors
                }
            else:
                return {
                    "new_comments_count": 0,
                    "updated_comments_count": 0,
                    "new_comment_ids": [],
                    "error_count": error_count,
                    "errors": errors
                }
                
        except Exception as e:
            error_msg = f"❌ Error processing comments: {str(e)}"
            print(error_msg)
            return {
                "new_comments_count": 0,
                "updated_comments_count": 0,
                "new_comment_ids": [],
                "error_count": len(comments),
                "errors": [error_msg]
            }

    @retry_mongodb_operation()
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
                used_quota = data.get("usedQuota", 0)
                crawl_date = datetime.fromisoformat(data.get("crawlDate", datetime.now()))
                
                if not all([keyword, used_quota, crawl_date]):
                    continue
                    
                # Convert datetime to string if needed
                if isinstance(crawl_date, datetime):
                    crawl_date = crawl_date.isoformat()
                    
                operations.append(
                    pymongo.UpdateOne(
                        {"apiKey": api_key},
                        {
                            "$push": {
                                "usedHistory": {
                                    "keyword": keyword,
                                    "usedQuota": used_quota,
                                    "crawlDate": crawl_date
                                }
                            }
                        }
                    )
                )
            
            if operations:
                # Execute bulk write
                result = self.collections["api_keys"].bulk_write(operations, ordered=False)
                return {
                    "new_keyword_usage_count": result.upserted_count,
                    "updated_api_key_count": result.modified_count
                }
                # print(f"✅ Added {len(operations)} keyword usage records successfully")
                # print(f"ℹ️ Updated {result.modified_count} API key documents")
            else: 
                return {
                    "new_keyword_usage_count": 0,
                    "updated_api_key_count": 0
                }
        except Exception as e:
            print(f"❌ Error adding keyword usage history: {str(e)}")
            raise

    @retry_mongodb_operation()
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

    @retry_mongodb_operation()
    def get_keyword_by_keyword(self, keyword: str) -> Dict[str, Any]:
        """Get keyword document from youtube_keywords collection.
        
        Args:
            keyword (str): Keyword to find
            
        Returns:
            Dict[str, Any]: Keyword document if found, None otherwise
        """
        return self.collections["youtube_keywords"].find_one({"keyword": keyword})

    @retry_mongodb_operation()
    def update_keyword_status(self, keyword: str, status: str) -> bool:
        """Update status of a keyword in youtube_keywords collection.
        
        Args:
            keyword (str): Keyword to update
            status (str): New status ("to crawl", "crawling", "crawled")
            
        Returns:
            bool: True if update successful, False otherwise
        """
        if status not in ["to crawl", "crawling", "crawled"]:
            return False
        
        result = self.collections["youtube_keywords"].update_one(
            {"keyword": keyword},
            {
                "$set": {
                    "status": status,
                    "lastUpdated": datetime.now()
                }
            }
        )
        
        return result.modified_count > 0 

    @retry_mongodb_operation()
    def update_channels_status_by_playlist_ids(self, playlist_ids: List[str]) -> Dict[str, Any]:
        """Update status of channels that have the given playlist IDs to 'crawled_video'.
        
        Args:
            playlist_ids (List[str]): List of playlist IDs to find channels
            
        Returns:
            Dict[str, Any]: Result containing:
                - updated_count: Number of channels updated
                - playlist_ids: List of playlist IDs that were found in channels
                - not_found_playlist_ids: List of playlist IDs that were not found in any channel
        """
        if not playlist_ids:
            return {
                "updated_count": 0,
                "playlist_ids": [],
                "not_found_playlist_ids": []
            }
        
        try:
            # Create bulk operations
            operations = []
            found_playlist_ids = set()
            
            # Find channels with matching playlist IDs
            channels = self.collections["channels"].find({"playlistId": {"$in": playlist_ids}})
            
            # Create update operations for each channel
            for channel in channels:
                playlist_id = channel.get("playlistId")
                if playlist_id:
                    found_playlist_ids.add(playlist_id)
                    operations.append(
                        pymongo.UpdateOne(
                            {"_id": channel["_id"]},
                            {"$set": {"status": "crawled_video"}}
                        )
                    )
            
            if operations:
                # Execute bulk write
                result = self.collections["channels"].bulk_write(operations, ordered=False)
                print(f"✅ Updated {result.modified_count} channels successfully")
                
                # Find playlist IDs that were not found in any channel
                not_found_playlist_ids = set(playlist_ids) - found_playlist_ids
                
                return {
                    "updated_count": result.modified_count,
                    "playlist_ids": list(found_playlist_ids),
                    "not_found_playlist_ids": list(not_found_playlist_ids)
                }
            else:
                print("ℹ️ No channels found with the given playlist IDs")
                return {
                    "updated_count": 0,
                    "playlist_ids": [],
                    "not_found_playlist_ids": playlist_ids
                }
                
        except Exception as e:
            print(f"❌ Error updating channel status by playlist IDs: {e}")
            return {
                "updated_count": 0,
                "playlist_ids": [],
                "not_found_playlist_ids": playlist_ids
            }

    @retry_mongodb_operation()
    def update_videos_status_by_video_ids(self, video_ids: List[str]) -> Dict[str, Any]:
        """Update status of videos that have the given video IDs to 'crawled_comment'.
        
        Args:
            video_ids (List[str]): List of video IDs to find videos
            
        Returns:
            Dict[str, Any]: Result containing:
                - updated_count: Number of videos updated
                - video_ids: List of video IDs that were found in videos collection
                - not_found_video_ids: List of video IDs that were not found in any video
        """
        if not video_ids:
            return {
                "updated_count": 0,
                "video_ids": [],
                "not_found_video_ids": []
            }
        
        try:
            # Create bulk operations
            operations = []
            found_video_ids = set()
            
            # Find videos with matching video IDs
            videos = self.collections["videos"].find({"videoId": {"$in": video_ids}})
            
            # Create update operations for each video
            for video in videos:
                video_id = video.get("videoId")
                if video_id:
                    found_video_ids.add(video_id)
                    operations.append(
                        pymongo.UpdateOne(
                            {"_id": video["_id"]},
                            {"$set": {"status": STATUS_ENTITY["crawled_comment"]}}
                        )
                    )
            
            if operations:
                # Execute bulk write
                result = self.collections["videos"].bulk_write(operations, ordered=False)
                print(f"✅ Updated {result.modified_count} videos successfully")
                
                # Find video IDs that were not found in any video
                not_found_video_ids = set(video_ids) - found_video_ids
                
                return {
                    "updated_count": result.modified_count,
                    "video_ids": list(found_video_ids),
                    "not_found_video_ids": list(not_found_video_ids)
                }
            else:
                print("ℹ️ No videos found with the given video IDs")
                return {
                    "updated_count": 0,
                    "video_ids": [],
                    "not_found_video_ids": video_ids
                }
                
        except Exception as e:
            print(f"❌ Error updating video status by video IDs: {e}")
            return {
                "updated_count": 0,
                "video_ids": [],
                "not_found_video_ids": video_ids
            }
    
    