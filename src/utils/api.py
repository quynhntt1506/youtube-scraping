import googleapiclient.discovery
import googleapiclient.errors
import requests
import json
from typing import List, Optional, Any, Dict
from datetime import datetime
from pathlib import Path

from src.models.channel import Channel
from src.models.video import Video
from src.models.comment import Comment
from src.database.database import Database
from src.database.api_key_manager import APIKeyManager
from src.utils.common import convert_to_datetime
from src.utils.logger import CustomLogger
from src.config.config import (
    # PROCESSED_DATA_DIR,
    # PROCESSED_PLAYLIST_DATA_DIR,
    # PROCESSED_CHANNEL_DATA_DIR,
    # PROCESSED_VIDEO_DATA_DIR,
    # PROCESSED_COMMENTS_DATA_DIR,
    MAX_RESULTS_PER_PAGE,
    MAX_ID_PAYLOAD,
    STATUS_ENTITY
)

class YouTubeAPI:
    def __init__(self):
        self.db = Database()
        self.api_manager = APIKeyManager(self.db)
        self.api_keys = self._load_api_keys()
        self.current_key_index = 0
        self.youtube = self._build_service()
        self.call_count = 0
        self.logger = CustomLogger("youtube_api")

    def _load_api_keys(self) -> List[str]:
        """Load active API keys from database."""
        try:
            # Get all active API keys from database
            active_keys = self.api_manager.get_active_api_keys()
            return [key["apiKey"] for key in active_keys]
        except Exception as e:
            self.logger.error(f"Error loading API keys from database: {e}")
            return []

    def _build_service(self) -> Optional[Any]:
        """Initialize YouTube API service."""
        if not self.api_keys:
            return None
        try:
            return googleapiclient.discovery.build(
                "youtube", "v3", developerKey=self.api_keys[self.current_key_index]
            )
        except Exception as e:
            self.logger.error(f"Error building YouTube service: {e}")
            return None

    def _switch_api_key(self) -> bool:
        """Switch to next API key if available."""
        # Mark current API key as unactive if quota is 0
        if self.current_key_index < len(self.api_keys):
            current_api_key = self.api_keys[self.current_key_index]
            api_key_doc = self.api_manager.get_api_key_stats(current_api_key)
            if api_key_doc and api_key_doc["remainingQuota"] <= 0:
                self.api_manager.update_quota(current_api_key, 0)  # This will set status to unactive

        # Try to get next active API key
        self.current_key_index += 1
        if self.current_key_index >= len(self.api_keys):
            # Reload active API keys from database
            self.api_keys = self._load_api_keys()
            self.current_key_index = 0
            if not self.api_keys:
                self.logger.error("No more active API keys available.")
                return False

        self.youtube = self._build_service()
        return self.youtube is not None

    def _track_quota(self, quota: int, quota_usage: Dict[str, int]) -> Dict[str, int]:
        """Track quota usage for current API key and maintain total per key.
        
        Args:
            quota (int): Quota units used in current request
            quota_usage (Dict[str, int]): Current quota usage tracking dict
            
        Returns:
            Dict[str, int]: Updated quota usage tracking dict
        """
        if self.current_key_index < len(self.api_keys):
            current_api_key = self.api_keys[self.current_key_index]
            if current_api_key in quota_usage:
                quota_usage[current_api_key] += quota
            else:
                quota_usage[current_api_key] = quota
        return quota_usage

    def search_channel_by_keyword(self, query: str, max_results: int = 100) -> dict:
        """Search for channels and videos."""
        channels = []
        videos = []
        next_page_token = None
        all_responses = []
        used_api_key = None
        quota_usage = {}

        # Ensure youtube service is initialized
        if not self.youtube:
            if not self._switch_api_key():
                return {"channels": [], "videos": [], "api_key": None, "quota_usage": {}}

        # Get current API key
        if self.current_key_index < len(self.api_keys):
            used_api_key = self.api_keys[self.current_key_index]

        while len(channels) < max_results:
            try:
                request = self.youtube.search().list(
                    part="snippet",
                    type="channel",
                    q=query,
                    maxResults=MAX_RESULTS_PER_PAGE,
                    regionCode="VN",
                    relevanceLanguage="vi",
                    pageToken=next_page_token
                )
                
                self.call_count += 1
                response = request.execute()
                all_responses.append(response)
                
                # Track quota usage
                quota_usage = self._track_quota(100, quota_usage)

                for item in response.get("items", []):
                    if item["id"]["kind"] == "youtube#channel":
                        channel_info = {
                            "channelId": item["snippet"]["channelId"],
                            "title": item["snippet"]["title"],
                            "description": item["snippet"]["description"],
                            "publishedAt": convert_to_datetime(item["snippet"]["publishedAt"]),
                            "status": STATUS_ENTITY["to_crawl"]
                        }
                        channels.append(channel_info)

                next_page_token = response.get("nextPageToken")
                if not next_page_token:
                    break

            except googleapiclient.errors.HttpError as e:
                error_details = e.error_details[0]
                if error_details.get("reason") == "quotaExceeded":
                    self.logger.warning(f"API key quota exceeded. Switching to next key...")
                    if not self._switch_api_key():
                        self.logger.error("No more API keys available. Stopping search.")
                        break
                    # Update used_api_key after switching
                    if self.current_key_index < len(self.api_keys):
                        used_api_key = self.api_keys[self.current_key_index]
                    continue
                else:
                    self.logger.error(f"API Error: {e}")
                    continue

        # Save all responses to file
        # if all_responses:
        #     self.save_crawl_result(all_responses, query)
            
        return {
            "channels": channels,
            "videos": videos,
            "api_key": used_api_key,
            "quota_usage": quota_usage
        }

    def get_channel_details(self, channel_ids: List[str]) -> Dict[str, Any]:
        """Get detailed information for multiple channels."""
        detailed_channels = []
        quota_usage = {}
        all_responses = []
        
        for i in range(0, len(channel_ids), MAX_ID_PAYLOAD):
            batch_ids = channel_ids[i:i+MAX_ID_PAYLOAD]
            try:
                request = self.youtube.channels().list(
                    part="snippet,statistics,topicDetails,brandingSettings,contentDetails",
                    id=",".join(batch_ids)
                )
                response = request.execute()
                all_responses.append(response)
                
                # Track quota usage
                quota_usage = self._track_quota(1, quota_usage)
                
                for item in response.get("items", []):
                    try:
                        channel_info = Channel.from_youtube_response(item)
                        # Convert to dict and remove _id field if it's None
                        channel_dict = channel_info.model_dump(by_alias=True)
                        if channel_dict.get("_id") is None:
                            channel_dict.pop("_id", None)
                        detailed_channels.append(channel_dict)
                    except Exception as e:
                        self.logger.error(f"Error processing channel {item.get('id')}: {e}")
                        continue
                    
            except googleapiclient.errors.HttpError as e:
                self.logger.error(f"API Error getting channel details: {e}")
                if not self._switch_api_key():
                    break
                continue

        # if detailed_channels:
        #     self.save_crawl_result_channel(detailed_channels, "channel")
        print(quota_usage)
        return {
            "detailed_channels": detailed_channels,
            "quota_usage": quota_usage
        }

    # def save_crawl_result(self, result: list, keyword: str) -> None:
    #     """Save crawl results to JSON file."""
    #     data = {
    #         "time": datetime.now().isoformat(),
    #         "keyword": keyword,
    #         "responses": result
    #     }
        
    #     # Create date-based directory
    #     today_str = datetime.now().strftime('%d-%m-%Y')
    #     save_dir = PROCESSED_DATA_DIR / today_str
    #     save_dir.mkdir(parents=True, exist_ok=True)
        
    #     # Save to file
    #     file_path = save_dir / f"{keyword}.json"
    #     with open(file_path, "w", encoding="utf-8") as f:
    #         json.dump(data, f, ensure_ascii=False, indent=4)
    
    # def save_crawl_result_playlist(self, result: list, keyword: str) -> None:
    #     """Save crawl results to JSON file."""
    #     data = {
    #         "time": datetime.now().isoformat(),
    #         "keyword": keyword,
    #         "responses": result
    #     }
        
    #     # Create date-based directory
    #     today_str = datetime.now().strftime('%d-%m-%Y')
    #     save_dir = PROCESSED_PLAYLIST_DATA_DIR / today_str
    #     save_dir.mkdir(parents=True, exist_ok=True)
        
    #     # Save to file
    #     file_path = save_dir / f"{keyword}.json"
    #     with open(file_path, "w", encoding="utf-8") as f:
    #         json.dump(data, f, ensure_ascii=False, indent=4)
    # def save_crawl_result_channel(self, result: list, keyword: str) -> None:
    #     """Save crawl results to JSON file."""
    #     data = {
    #         "time": datetime.now().isoformat(),
    #         "keyword": keyword,
    #         "responses": result
    #     }
        
    #     # Create date-based directory
    #     today_str = datetime.now().strftime('%d-%m-%Y')
    #     save_dir = PROCESSED_CHANNEL_DATA_DIR / today_str
    #     save_dir.mkdir(parents=True, exist_ok=True)
        
    #     # Save to file
    #     file_path = save_dir / f"{keyword}.json"
    #     with open(file_path, "w", encoding="utf-8") as f:
    #         json.dump(data, f, ensure_ascii=False, indent=4, default=lambda x: x.isoformat() if isinstance(x, datetime) else x)
    
    # def save_crawl_result_videos(self, result: list, keyword: str) -> None:
    #     """Save crawl results to JSON file."""
    #     data = {
    #         "time": datetime.now().isoformat(),
    #         "keyword": keyword,
    #         "responses": result
    #     }
        
    #     # Create date-based directory
    #     today_str = datetime.now().strftime('%d-%m-%Y')
    #     save_dir = PROCESSED_VIDEO_DATA_DIR / today_str
    #     save_dir.mkdir(parents=True, exist_ok=True)
        
    #     # Save to file
    #     file_path = save_dir / f"{keyword}.json"
    #     with open(file_path, "w", encoding="utf-8") as f:
    #         json.dump(data, f, ensure_ascii=False, indent=4)

    # def save_crawl_result_comments(self, result: list, keyword: str) -> None:
    #     """Save crawl results to JSON file."""
    #     data = {
    #         "time": datetime.now().isoformat(),
    #         "keyword": keyword,
    #         "responses": result
    #     }
        
    #     # Create date-based directory
    #     today_str = datetime.now().strftime('%d-%m-%Y')
    #     save_dir = PROCESSED_COMMENTS_DATA_DIR / today_str
    #     save_dir.mkdir(parents=True, exist_ok=True)
        
    #     # Save to file
    #     file_path = save_dir / f"{keyword}.json"
    #     with open(file_path, "w", encoding="utf-8") as f:
    #         json.dump(data, f, ensure_ascii=False, indent=4)

    def close(self):
        """Close database connection."""
        self.db.close()

    def get_all_videos_from_playlist(self, playlist_id: str) -> Dict[str, Any]:
        """Get all videos from a playlist."""
        videos = []
        quota_usage = {}
        all_responses = []
        next_page_token = None
        
        try:
            while True:
                try:
                    request = self.youtube.playlistItems().list(
                        part="snippet,contentDetails",
                        playlistId=playlist_id,
                        maxResults=MAX_RESULTS_PER_PAGE,
                        pageToken=next_page_token
                    )
                    
                    response = request.execute()
                    all_responses.append(response)
                    
                    # Track quota usage
                    quota_usage = self._track_quota(1, quota_usage)

                    for item in response.get("items", []):
                        video_info = Video.from_youtube_response_playlist(item, playlist_id)
                        # Convert to dict and remove _id field if it's None
                        video_dict = video_info.model_dump(by_alias=True)
                        if video_dict.get("_id") is None:
                            video_dict.pop("_id", None)
                        videos.append(video_dict)
                    next_page_token = response.get("nextPageToken")
                    if not next_page_token:
                        break
                        
                except googleapiclient.errors.HttpError as e:
                    error_details = e.error_details[0]
                    if error_details.get("reason") == "quotaExceeded":
                        self.logger.warning(f"API key quota exceeded. Switching to next key...")
                        if not self._switch_api_key():
                            self.logger.error("No more API keys available. Stopping video retrieval.")
                            break
                        continue
                    elif error_details.get("reason") == "playlistNotFound":
                        self.logger.warning(f"Playlist {playlist_id} not found.")
                        break
                    else:
                        self.logger.error(f"API Error getting playlist items: {e}")
                        break
                        
        except Exception as e:
            self.logger.error(f"Error getting videos for playlist {playlist_id}: {e}")
            return {
                "videos": [],
                "quota_usage": quota_usage,
                "crawled_playlist_ids": []
            }
            
        self.logger.info(f"Crawled {len(videos)} videos for playlist {playlist_id}")
        
        return {
            "crawled_playlist_ids": [playlist_id],
            "videos": videos,
            "quota_usage": quota_usage
        }

    def get_video_details(self, video_ids: List[str]) -> Dict[str, Any]:
        """Get detailed information for multiple videos."""
        detailed_videos = []
        quota_usage = {}
        all_responses = []
        crawled_video_ids = []
        
        for i in range(0, len(video_ids), MAX_ID_PAYLOAD):
            batch_ids = video_ids[i:i+MAX_ID_PAYLOAD]
            try:
                request = self.youtube.videos().list(
                    part="snippet,statistics,contentDetails,topicDetails,status",
                    id=",".join(batch_ids)
                )
                response = request.execute()
                all_responses.append(response)
                
                # Track quota usage
                quota_usage = self._track_quota(1, quota_usage)
                    
                for item in response.get("items", []):
                    video_info = Video.from_youtube_response_detail(item)
                    # Convert to dict and remove _id field if it's None
                    video_dict = video_info.model_dump(by_alias=True)
                    if video_dict.get("_id") is None:
                        video_dict.pop("_id", None)
                    detailed_videos.append(video_dict)
                    crawled_video_ids.append(video_dict.get("videoId"))
            except googleapiclient.errors.HttpError as e:
                self.logger.error(f"API Error getting video details: {e}")
                if not self._switch_api_key():
                    break
                continue
        # if all_responses:
        #     self.save_crawl_result_videos(all_responses, "video")
            
        return {
            "crawled_video_ids": crawled_video_ids,
            "detailed_videos": detailed_videos,
            "quota_usage": quota_usage
        }

    def get_video_comments(self, video_id: str) -> Dict[str, Any]:
        """Get all comments and replies for a video.
        
        Args:
            video_id (str): The ID of the video to get comments for
            
        Returns:
            Dict[str, Any]: Dictionary containing:
                - comments: List of all comments and their replies
                - quota_usage: Dictionary tracking API quota usage
        """
        comments = []
        quota_usage = {}
        all_responses = []
        next_page_token = None
        
        try:
            while True:  # Continue until no more pages
                try:
                    request = self.youtube.commentThreads().list(
                        part="snippet,replies",
                        videoId=video_id,
                        maxResults=100,  # Maximum allowed by YouTube API
                        pageToken=next_page_token
                    )
                    response = request.execute()
                    all_responses.append(response)
                    
                    # Track quota usage
                    quota_usage = self._track_quota(1, quota_usage)
                    
                    for item in response.get("items", []):
                        try:
                            comment = Comment.from_youtube_response(item)
                            # Convert to dict and remove _id field if it's None
                            comment_dict = comment.model_dump(by_alias=True)
                            if comment_dict.get("_id") is None:
                                comment_dict.pop("_id", None)
                            # Also handle _id in replies if they exist
                            replies = comment_dict.get("replies", [])
                            if replies:
                                for reply in replies:
                                    if reply.get("_id") is None:
                                        reply.pop("_id", None)
                            comments.append(comment_dict)
                        except Exception as e:
                            self.logger.error(f"Error processing comment: {e}")
                            continue
                    
                    next_page_token = response.get("nextPageToken")
                    if not next_page_token:
                        break
                        
                except googleapiclient.errors.HttpError as e:
                    error_details = e.error_details[0]
                    if error_details.get("reason") == "quotaExceeded":
                        self.logger.warning(f"API key quota exceeded. Switching to next key...")
                        if not self._switch_api_key():
                            self.logger.error("No more API keys available. Stopping comment retrieval.")
                            break
                        continue
                    elif error_details.get("reason") == "commentsDisabled":
                        self.logger.warning(f"Comments are disabled for video {video_id}")
                        break
                    else:
                        self.logger.error(f"API Error getting comments: {e}")
                        break
                        
        except Exception as e:
            self.logger.error(f"Error getting comments for video {video_id}: {e}")
            
        # if all_responses:
        #     self.save_crawl_result_comments(all_responses, f"comments_{video_id}")
            
        return {
            "comments": comments,
            "quota_usage": quota_usage
        }