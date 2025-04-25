import googleapiclient.discovery
import googleapiclient.errors
from typing import List, Optional, Any
from datetime import datetime
import requests
from pathlib import Path
from utils.database import Database
from config.config import API_KEYS_FILE, CHANNEL_IMAGES_DIR, VIDEO_IMAGES_DIR, PROCESSED_DATA_DIR
import json

class YouTubeAPI:
    def __init__(self):
        self.api_keys = self._load_api_keys()
        self.current_key_index = 0
        self.youtube = self._build_service()
        self.call_count = 0

    def _load_api_keys(self) -> List[str]:
        """Load API keys from file."""
        try:
            with open(API_KEYS_FILE, "r", encoding="utf-8") as f:
                return [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            print(f"Error: {API_KEYS_FILE} not found.")
            return []
        except Exception as e:
            print(f"Error reading {API_KEYS_FILE}: {e}")
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
        self.current_key_index += 1
        if self.current_key_index >= len(self.api_keys):
            print("No more API keys available.")
            return False
        self.youtube = self._build_service()
        return self.youtube is not None

    def search(self, query: str, max_results: int = 100) -> dict:
        """Search for channels and videos."""
        channels = []
        videos = []
        next_page_token = None
        all_responses = []
        database = Database()

        # Ensure youtube service is initialized
        if not self.youtube:
            if not self._switch_api_key():
                return {"channels": [], "videos": []}

        while len(videos) < max_results:
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

                for item in response.get("items", []):
                    if item["id"]["kind"] == "youtube#channel":
                        channel_info = {
                            "channelId": item["snippet"]["channelId"],
                            "title": item["snippet"]["title"],
                            "description": item["snippet"]["description"],
                            "publishedAt": item["snippet"]["publishedAt"],
                        }
                        channels.append(channel_info)

                    elif item["id"]["kind"] == "youtube#video":
                        video_info = {
                            "videoId": item["id"]["videoId"],
                            "title": item["snippet"]["title"],
                            "description": item["snippet"]["description"],
                            "publishedAt": item["snippet"]["publishedAt"],
                            "channelId": item["snippet"]["channelId"],
                            "channelTitle": item["snippet"]["channelTitle"],
                            "thumbnailUrl": item["snippet"]["thumbnails"].get("high", {}).get("url", "N/A"),
                            "crawlDate": datetime.now()
                        }
                        videos.append(video_info)

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
            
        return {"channels": channels, "videos": videos}

    def get_channel_details(self, channel_ids: List[str]) -> List[dict]:
        """Get detailed information for multiple channels."""
        detailed_channels = []
        for i in range(0, len(channel_ids), 50):
            batch_ids = channel_ids[i:i+50]
            try:
                request = self.youtube.channels().list(
                    part="snippet,statistics,topicDetails,brandingSettings",
                    id=",".join(batch_ids)
                )
                response = request.execute()
                
                for item in response.get("items", []):
                    channel_info = self._process_channel_item(item)
                    detailed_channels.append(channel_info)

            except googleapiclient.errors.HttpError as e:
                print(f"API Error getting channel details: {e}")
                if not self._switch_api_key():
                    break
                continue

        return detailed_channels

    def _process_channel_item(self, item: dict) -> dict:
        """Process a single channel item from the API response."""
        channel_id = item["id"]
        today_str = datetime.now().strftime('%d-%m-%Y')
        
        # Download and save avatar
        avatar_url = item["snippet"]["thumbnails"].get("default", {}).get("url", "")
        avatar_path = self._download_image(
            avatar_url, 
            CHANNEL_IMAGES_DIR / today_str / f"{channel_id}_avatar.jpg"
        )

        # Download and save banner
        banner_url = item["brandingSettings"].get("image", {}).get("bannerExternalUrl", "")
        banner_path = self._download_image(
            banner_url,
            CHANNEL_IMAGES_DIR / today_str / f"{channel_id}_banner.jpg"
        )

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
            "avatarFile": str(avatar_path) if avatar_path else "",
            "bannerUrl": banner_url,
            "bannerFile": str(banner_path) if banner_path else "",
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