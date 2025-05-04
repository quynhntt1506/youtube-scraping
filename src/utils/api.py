import googleapiclient.discovery
import googleapiclient.errors
from typing import List, Optional, Any, Dict
from datetime import datetime
import requests
from pathlib import Path
from utils.database import Database
from utils.common import convert_to_datetime
from config.config import CHANNEL_IMAGES_DIR, VIDEO_IMAGES_DIR, PROCESSED_DATA_DIR
import json
import os
from .api_key_manager import APIKeyManager

class YouTubeAPI:
    def __init__(self):
        self.db = Database()
        self.api_manager = APIKeyManager(self.db)
        self.api_keys = self._load_api_keys()
        self.current_key_index = 0
        self.youtube = self._build_service()
        self.call_count = 0

    def _load_api_keys(self) -> List[str]:
        """Load active API keys from database."""
        try:
            # Get all active API keys from database
            active_keys = self.api_manager.get_active_api_keys()
            return [key["api_key"] for key in active_keys]
        except Exception as e:
            print(f"Error loading API keys from database: {e}")
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
            print(f"Error building YouTube service: {e}")
            return None

    def _switch_api_key(self) -> bool:
        """Switch to next API key if available."""
        # Mark current API key as unactive if quota is 0
        if self.current_key_index < len(self.api_keys):
            current_api_key = self.api_keys[self.current_key_index]
            api_key_doc = self.api_manager.get_api_key_stats(current_api_key)
            if api_key_doc and api_key_doc["remaining_quota"] <= 0:
                self.api_manager.update_quota(current_api_key, 0)  # This will set status to unactive

        # Try to get next active API key
        self.current_key_index += 1
        if self.current_key_index >= len(self.api_keys):
            # Reload active API keys from database
            self.api_keys = self._load_api_keys()
            self.current_key_index = 0
            if not self.api_keys:
                print("No more active API keys available.")
                return False

        self.youtube = self._build_service()
        return self.youtube is not None

    def search_channel_by_keyword(self, query: str, max_results: int = 100) -> dict:
        """Search for channels and videos."""
        channels = []
        videos = []
        next_page_token = None
        all_responses = []
        used_api_key = None
        used_quota = 0

        # Ensure youtube service is initialized
        if not self.youtube:
            if not self._switch_api_key():
                return {"channels": [], "videos": [], "api_key": None, "used_quota": 0}

        # Get current API key
        if self.current_key_index < len(self.api_keys):
            used_api_key = self.api_keys[self.current_key_index]

        while len(channels) < max_results:
            try:
                request = self.youtube.search().list(
                    part="snippet",
                    type="channel",
                    q=query,
                    maxResults=50,
                    regionCode="VN",
                    relevanceLanguage="vi",
                    pageToken=next_page_token
                )
                
                self.call_count += 1
                response = request.execute()
                all_responses.append(response)
                
                # Update quota usage
                if self.current_key_index < len(self.api_keys):
                    self.api_manager.update_quota(used_api_key, 100)
                    used_quota += 100

                for item in response.get("items", []):
                    if item["id"]["kind"] == "youtube#channel":
                        channel_info = {
                            "channelId": item["snippet"]["channelId"],
                            "title": item["snippet"]["title"],
                            "description": item["snippet"]["description"],
                            "publishedAt": convert_to_datetime(item["snippet"]["publishedAt"]),
                        }
                        if not any(channel["channelId"] == channel_info["channelId"] for channel in channels):
                            channels.append(channel_info)

                next_page_token = response.get("nextPageToken")
                if not next_page_token:
                    break

            except googleapiclient.errors.HttpError as e:
                error_details = e.error_details[0]
                if error_details.get("reason") == "quotaExceeded":
                    print(f"API key quota exceeded. Switching to next key...")
                    if not self._switch_api_key():
                        print("No more API keys available. Stopping search.")
                        break
                    # Update used_api_key after switching
                    if self.current_key_index < len(self.api_keys):
                        used_api_key = self.api_keys[self.current_key_index]
                    # Continue with the same next_page_token to get remaining results
                    continue
                else:
                    print(f"API Error: {e}")
                    continue

        # Save all responses to file
        if all_responses:
            self.save_crawl_result(all_responses, query)
            
        return {
            "channels": channels,
            "videos": videos,
            "api_key": used_api_key,
            "used_quota": used_quota
        }

    def search_channel_and_video_by_keyword(self, query: str, max_results: int = 100) -> dict:
        """Search for channels and videos."""
        channels = []
        videos = []
        next_page_token = None
        all_responses = []
        used_api_key = None
        used_quota = 0

        # Ensure youtube service is initialized
        if not self.youtube:
            if not self._switch_api_key():
                return {"channels": [], "videos": [], "api_key": None, "used_quota": 0}

        # Get current API key
        if self.current_key_index < len(self.api_keys):
            used_api_key = self.api_keys[self.current_key_index]

        while len(channels) < max_results:
            try:
                request = self.youtube.search().list(
                    part="snippet",
                    type="video,channel",
                    q=query,
                    maxResults=50,
                    regionCode="VN",
                    relevanceLanguage="vi",
                    pageToken=next_page_token
                )
                
                self.call_count += 1
                response = request.execute()
                all_responses.append(response)
                
                # Update quota usage
                if self.current_key_index < len(self.api_keys):
                    # current_api_key = self.api_keys[self.current_key_index]
                    self.api_manager.update_quota(used_api_key, 100)
                    used_quota += 100

                for item in response.get("items", []):
                    if item["id"]["kind"] == "youtube#channel":
                        channel_info = {
                            "channelId": item["snippet"]["channelId"],
                            "title": item["snippet"]["title"],
                            "description": item["snippet"]["description"],
                            "publishedAt": convert_to_datetime(item["snippet"]["publishedAt"]),
                        }
                        if not any(channel["channelId"] == channel_info["channelId"] for channel in channels):
                            channels.append(channel_info)
                            
                    elif item["id"]["kind"] == "youtube#video":
                        video_info = {
                            "videoId": item["id"]["videoId"],
                            "title": item["snippet"]["title"],
                            "description": item["snippet"]["description"],
                            "publishedAt": convert_to_datetime(item["snippet"]["publishedAt"]),
                            "channelId": item["snippet"]["channelId"],
                            "channelTitle": item["snippet"]["channelTitle"],
                            "thumbnailUrl": item["snippet"]["thumbnails"].get("high", {}).get("url", "N/A"),
                            "crawlDate": datetime.now()
                        }
                        videos.append(video_info)
                        # Check if channel exists in channels list
                        channel_video_info = {
                            "channelId": video_info["channelId"],
                            "title": video_info["channelTitle"],
                        }
                        if not any(channel["channelId"] == channel_video_info["channelId"] for channel in channels):
                            channels.append(channel_video_info)
                next_page_token = response.get("nextPageToken")
                if not next_page_token:
                    break

            except googleapiclient.errors.HttpError as e:
                print(f"API Error: {e}")
                if not self._switch_api_key():
                    break
                continue

        # Save all responses to file
        if all_responses:
            self.save_crawl_result(all_responses, query)
            
        return {
            "channels": channels,
            "videos": videos,
            "api_key": used_api_key,
            "used_quota": used_quota
        }

    def search_video_by_keyword_filter_pulished_date(self, query: str, published_after: str, max_results: int = 50) -> dict:
        """
        Search for videos and channels published after a specified date.
        
        Args:
            query (str): Search query
            published_after (str): Date in ISO 8601 format (YYYY-MM-DDThh:mm:ss.sZ)
            max_results (int): Maximum number of results to return (default: 50)
            
        Returns:
            dict: Dictionary containing lists of videos and channels
        """
        channels = []
        videos = []
        next_page_token = None
        response_array = []
        current_date = datetime.now().strftime("%d-%m-%Y")
        used_api_key = None
        used_quota = 0
        
        # Create file to save response
        folder_path = os.path.join("result_crawl", current_date)
        os.makedirs(folder_path, exist_ok=True)
        result_file_path = os.path.join(folder_path, f"{query}_{published_after}.json")
        
        if not self.youtube:
            if not self._switch_api_key():
                return {"channels": [], "videos": [], "api_key": None, "used_quota": 0}
        
        # Get current API key
        if self.current_key_index < len(self.api_keys):
            used_api_key = self.api_keys[self.current_key_index]
        
        while len(videos) < max_results:
            try:
                request = self.youtube.search().list(
                    part="snippet",
                    q=query,
                    type="video",
                    maxResults=min(50, max_results - len(videos)),
                    publishedAfter=published_after,
                    regionCode="VN",
                    relevanceLanguage="vi",
                    pageToken=next_page_token
                )
                response = request.execute()
                response_array.append(response)
                
                # Update quota usage
                if self.current_key_index < len(self.api_keys):
                    self.api_manager.update_quota(used_api_key, 100)
                    used_quota += 100
                
                for item in response.get("items", []):
                    if item["id"]["kind"] == "youtube#channel":
                        channel_info = {
                            "channelId": item["snippet"]["channelId"],
                            "title": item["snippet"]["title"],
                            "description": item["snippet"]["description"],
                            "publishedAt": convert_to_datetime(item["snippet"]["publishedAt"]),
                        }
                        channels.append(channel_info)
                            
                    elif item["id"]["kind"] == "youtube#video":
                        video_info = {
                            "videoId": item["id"]["videoId"],
                            "title": item["snippet"]["title"],
                            "description": item["snippet"]["description"],
                            "publishedAt": convert_to_datetime(item["snippet"]["publishedAt"]),
                            "channelId": item["snippet"]["channelId"],
                            "channelTitle": item["snippet"]["channelTitle"],
                            "thumbnailUrl": item["snippet"]["thumbnails"].get("high", {}).get("url", "N/A"),
                        }
                        videos.append(video_info)
                            
                        # Add channel info from video if not already in channels array
                        channel_info_video = {
                            "channelId": item["snippet"]["channelId"],
                            "title": item["snippet"]["channelTitle"],
                        }
                        # Check if channelId already exists in channels array
                        if not any(channel["channelId"] == channel_info_video["channelId"] for channel in channels):
                            channels.append(channel_info_video)
                
                next_page_token = response.get("nextPageToken")
                if not next_page_token:
                    break
                    
            except googleapiclient.errors.HttpError as e:
                print(f"API Error searching videos: {e}")
                if not self._switch_api_key():
                    break
                continue
                
        # Lưu toàn bộ response_array vào file sau khi hoàn thành
        with open(result_file_path, "w", encoding="utf-8") as f:
            json.dump(response_array, f, ensure_ascii=False, indent=4)
                
        return {
            "videos": videos,
            "channels": channels,
            "api_key": used_api_key,
            "used_quota": used_quota
        }

    def get_channel_details(self, channel_ids: List[str]) -> Dict[str, Any]:
        """Get detailed information for multiple channels.
        
        Returns:
            Dict[str, Any]: Dictionary containing detailed channels and quota usage
        """
        detailed_channels = []
        used_quota = 0
        
        for i in range(0, len(channel_ids), 50):
            batch_ids = channel_ids[i:i+50]
            try:
                request = self.youtube.channels().list(
                    part="snippet,statistics,topicDetails,brandingSettings,contentDetails",
                    id=",".join(batch_ids)
                )
                response = request.execute()
                
                # Update quota usage
                if self.current_key_index < len(self.api_keys):
                    current_api_key = self.api_keys[self.current_key_index]
                    
                for item in response.get("items", []):
                    channel_info = self._process_channel_item(item)
                    detailed_channels.append(channel_info)
                    used_quota += 1
                    self.api_manager.update_quota(current_api_key, 1)
            except googleapiclient.errors.HttpError as e:
                print(f"API Error getting channel details: {e}")
                if not self._switch_api_key():
                    break
                continue

        return {
            "detailed_channels": detailed_channels,
            "used_quota": used_quota
        }

    def _process_channel_item(self, item: dict) -> dict:
        """Process a single channel item from the API response."""
        channel_id = item["id"]
        today_str = datetime.now().strftime('%d-%m-%Y')
        
        # # Download and save avatar
        avatar_url = item["snippet"]["thumbnails"].get("default", {}).get("url", "")
        # avatar_path = self._download_image(
        #     avatar_url, 
        #     CHANNEL_IMAGES_DIR / today_str / f"{channel_id}_avatar.jpg"
        # )

        # # Download and save banner
        banner_url = item["brandingSettings"].get("image", {}).get("bannerExternalUrl", "")
        # banner_path = self._download_image(
        #     banner_url,
        #     CHANNEL_IMAGES_DIR / today_str / f"{channel_id}_banner.jpg"
        # )

        playlist_id = item["contentDetails"].get("relatedPlaylists", {}).get("uploads", "")
        return {
            "channelId": channel_id,
            "title": item["snippet"]["title"],
            "description": item["snippet"]["description"],
            "publishedAt": datetime.fromisoformat(item["snippet"]["publishedAt"].replace("Z", "+00:00")),
            "country": item["snippet"].get("country", ""),
            "subscriberCount": int(item["statistics"].get("subscriberCount", 0)),
            "videoCount": int(item["statistics"].get("videoCount", 0)),
            "viewCount": int(item["statistics"].get("viewCount", 0)),
            "topics": ",".join(item["topicDetails"].get("topicIds", []) if item.get("topicDetails") else []),
            "email": self._extract_email(item["snippet"]["description"]),
            "avatarUrl": avatar_url,
            "bannerUrl": banner_url,
            "playlistId": playlist_id,
            "crawlDate": datetime.now()
        }

    def _download_image(self, url: str, save_path: Path) -> Optional[Path]:
        """Download an image and save it to the specified path."""
        if not url:
            return None
            
        save_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            response = requests.get(url, stream=True)
            if response.status_code == 200:
                with open(save_path, 'wb') as f:
                    for chunk in response.iter_content(1024):
                        f.write(chunk)
                return save_path
        except Exception as e:
            print(f"Error downloading image {url}: {e}")
        return None

    def _extract_email(self, text: str) -> str:
        """Extract email from text if present."""
        if not text:
            return ""
        import re
        email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        emails = re.findall(email_pattern, text)
        return emails[0] if emails else ""
    
    def save_crawl_result(self, result: list, keyword: str) -> None:
        """Save crawl results to JSON file."""
        data = {
            "time": datetime.now().isoformat(),
            "keyword": keyword,
            "responses": result
        }
        
        # Create date-based directory
        today_str = datetime.now().strftime('%d-%m-%Y')
        save_dir = PROCESSED_DATA_DIR / today_str
        save_dir.mkdir(parents=True, exist_ok=True)
        
        # Save to file
        file_path = save_dir / f"{keyword}.json"
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

    def close(self):
        """Close database connection."""
        self.db.close()

    def get_channels_playlist_videos(self, detailed_channels: List[dict], max_results_per_playlist: int = 50) -> Dict[str, Any]:
        """
        Get videos from uploads playlists of multiple channels.
        
        Args:
            detailed_channels (List[dict]): List of channel details containing playlistId
            max_results_per_playlist (int): Maximum number of videos to return per playlist (default: 50)
            
        Returns:
            Dict[str, Any]: Dictionary containing videos and quota usage
        """
        all_videos = []
        used_quota = 0
        
        for channel in detailed_channels:
            playlist_id = channel.get("playlistId")
            if not playlist_id:
                continue
                
            videos = []
            next_page_token = None
            
            try:
                while len(videos) < max_results_per_playlist:
                    try:
                        request = self.youtube.playlistItems().list(
                            part="snippet,contentDetails",
                            playlistId=playlist_id,
                            maxResults=min(50, max_results_per_playlist - len(videos)),
                            pageToken=next_page_token
                        )
                        
                        response = request.execute()
                        used_quota += 1
                        
                        for item in response.get("items", []):
                            video_info = {
                                "videoId": item["contentDetails"]["videoId"],
                                "title": item["snippet"]["title"],
                                "description": item["snippet"]["description"],
                                "publishedAt": convert_to_datetime(item["snippet"]["publishedAt"]),
                                "channelId": item["snippet"]["channelId"],
                                "channelTitle": item["snippet"]["channelTitle"],
                                "thumbnailUrl": item["snippet"]["thumbnails"].get("high", {}).get("url", "N/A"),
                                "position": item["snippet"]["position"],
                                "playlistId": playlist_id,
                                "crawlDate": datetime.now()
                            }
                            videos.append(video_info)
                        
                        next_page_token = response.get("nextPageToken")
                        if not next_page_token:
                            break
                            
                    except googleapiclient.errors.HttpError as e:
                        error_details = e.error_details[0]
                        if error_details.get("reason") == "playlistNotFound":
                            print(f"Uploads playlist not found for channel {playlist_id}. Skipping...")
                            break
                        else:
                            print(f"API Error getting playlist items for channel {playlist_id}: {e}")
                            if not self._switch_api_key():
                                break
                            continue
                        
            except Exception as e:
                print(f"Error getting videos for channel {playlist_id}: {e}")
                continue
                
            all_videos.extend(videos)
            
        return {
            "videos": all_videos,
            "used_quota": used_quota
        }