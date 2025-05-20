from typing import Dict, Any, List
from pathlib import Path
from datetime import datetime
import json

from src.utils.logger import CustomLogger
from src.utils.api import YouTubeAPI
from src.database.database import Database
from src.database.api_key_manager import APIKeyManager
from src.controller.image_downloader import download_channel_images
from src.controller.thumbnail_downloader import download_video_thumbnails
from src.utils.common import parse_youtube_channel_url
from src.config.config import MAX_CHANNELS, MAX_ENTITY_IN_BATCH, MIN_ENTITY_IN_BATCH
from src.controller.send_to_data_controller import *

# Initialize logger
logger = CustomLogger("crawler_auto")

class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime objects."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

def get_path_file_json(type: str, name: str) -> Path:
    # Create data/json/yyyy-mm-dd directory
    today = datetime.now().strftime("%Y-%m-%d")
    json_dir = Path("data/json") / today / type
    json_dir.mkdir(parents=True, exist_ok=True)
    json_path = json_dir / f"{name}.json"
        
    return json_path

def crawl_channel_by_id(channel_ids: List[str]) -> Dict[str, Any]:
    """Crawl channel by id."""
    api = YouTubeAPI()
    db = Database()

    try:
        logger.info(f"Crawling channel {channel_ids}")
        channel_result = api.get_channel_detail_by_ids(channel_ids)
        image_result = download_channel_images(channel_result["detailed_channels"])

        all_detaled_channels = [];
       
        # Save each channel's data to a separate JSON file
        for channel in image_result["updated_channels"]:
            channel_id = channel["channelId"]
            json_path = get_path_file_json("channel", channel_id)
            channel["jsonPath"] = str(json_path)
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(channel, f, ensure_ascii=False, indent=2, cls=DateTimeEncoder)
            logger.info(f"Saved channel data to {json_path}")
            if channel:
                send_channel_to_data_controller([channel])
            all_detaled_channels.append(channel)

        result_db = db.insert_many_channels(all_detaled_channels)
        return {
            "new_channels": all_detaled_channels,
        }
    finally:
        db.close()

def crawl_channel_by_custom_urls(custom_urls: List[str]) -> Dict[str, Any]:
    """Crawl channel by custom urls."""
    api = YouTubeAPI()
    db = Database()

    try:
        logger.info(f"Crawling channel {custom_urls}")
        channel_result = api.get_channel_detail_by_custom_urls(custom_urls)
        image_result = download_channel_images(channel_result["detailed_channels"])

        all_detaled_channels = [];

        # Save each channel's data to a separate JSON file
        for channel in image_result["updated_channels"]:
            channel_id = channel["channelId"]
            json_path = get_path_file_json("channel", channel_id)
            channel["jsonPath"] = str(json_path)
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(channel, f, ensure_ascii=False, indent=2, cls=DateTimeEncoder)
            logger.info(f"Saved channel data to {json_path}")
            if channel:
                send_channel_to_data_controller([channel])
                crawl_videos_in_playlist(channel["playlistId"])
            all_detaled_channels.append(channel)

        result_db = db.insert_many_channels(all_detaled_channels)
        return {
            "new_channels": all_detaled_channels,
        }
    finally:
        db.close()

def crawl_channel_by_urls(channel_urls: List[str]) -> Dict[str, Any]:
    """Crawl channel by url."""
    api = YouTubeAPI()
    db = Database()
    channel_ids = []
    custom_urls = []
    usernames = []
    detail_channel_results = []
    try:
        for channel_url in channel_urls:
            parse_url = parse_youtube_channel_url(channel_url)
            if parse_url["type"] == "channel_id":
                channel_ids.append(parse_url["value"])
            elif parse_url["type"] == "custom_url":
                custom_urls.append(parse_url["value"])
            elif parse_url["type"] == "username":
                usernames.append(parse_url["value"])
        logger.info(f"Crawling channel {channel_urls}")
        if channel_ids:
            channel_result = api.get_channel_detail_by_ids(channel_ids)
            detail_channel_results.extend(channel_result["detailed_channels"])
        elif custom_urls:
            channel_result = api.get_channel_detail_by_custom_urls(custom_urls)
            detail_channel_results.extend(channel_result["detailed_channels"])
        elif usernames:
            channel_result = api.get_channel_detail_by_usernames(usernames)
            detail_channel_results.extend(channel_result["detailed_channels"])

        image_result = download_channel_images(detail_channel_results)

        all_detailed_channels = [];
        # Create data/json/yyyy-mm-dd directory
        today = datetime.now().strftime("%Y-%m-%d")
        json_dir = Path("data/json") / today
        json_dir.mkdir(parents=True, exist_ok=True)

        # Save each channel's data to a separate JSON file
        for channel in image_result["updated_channels"]:
            channel_id = channel["channelId"]
            json_path = json_dir / f"{channel_id}.json"
            channel["jsonPath"] = str(json_path)
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(channel, f, ensure_ascii=False, indent=2, cls=DateTimeEncoder)
            logger.info(f"Saved channel data to {json_path}")
            all_detailed_channels.append(channel)

        result_db = db.insert_many_channels(all_detailed_channels)
        return {
            # "new_channels": channel_result["detailed_channels"],
            "new_channels": all_detailed_channels,
        }
    finally:
        db.close()

def crawl_video_by_ids(video_ids: List[str]) -> Dict[str, Any]:
    """Crawl video by id."""
    api = YouTubeAPI()
    db = Database()

    try:
        video_result = api.get_video_details(video_ids)
        image_result = download_video_thumbnails(video_result["detailed_videos"])

        all_detailed_videos = [];

        # Save each channel's data to a separate JSON file
        for video in image_result["updated_videos"]:
            video_id = video["videoId"]
            json_path = get_path_file_json("video", video_id)
            video["jsonPath"] = str(json_path)
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(video, f, ensure_ascii=False, indent=2, cls=DateTimeEncoder)
            logger.info(f"Saved video data to {json_path}")
            all_detailed_videos.append(video)
            if (video):
                send_video_to_data_controller([video])

        result_db = db.insert_many_videos(all_detailed_videos)
        return {
            "new_videos": all_detailed_videos,
        }
    finally:
        db.close()

def crawl_videos_in_playlist(playlist_id: str) -> Dict[str, Any]:
    """Crawl videos from a playlist."""
    api = YouTubeAPI()

    playlist_result = api.get_all_videos_from_playlist(playlist_id)
    videos = playlist_result["videos"]
    logger.info(f"Found {len(videos)} videos from playlist")
    
    # Get video details and process in batches of 50
    all_videos = []
    
    for i in range(0, len(videos), MAX_ENTITY_IN_BATCH):
        batch_videos = videos[i:i+MAX_ENTITY_IN_BATCH]
        logger.info(f"Processing batch of {len(batch_videos)} videos")
        
        # Get video details for this batch
        video_ids = [video["videoId"] for video in batch_videos]
        result_crawl = crawl_video_by_ids(video_ids)
        all_videos.extend(result_crawl["new_videos"])
        
        logger.info(f"Completed processing batch of {len(batch_videos)} videos")

    return {
        "videos": all_videos,
    }

