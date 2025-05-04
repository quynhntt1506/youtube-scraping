import json
import requests
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List

from utils.api import YouTubeAPI
from utils.database import Database
from utils.logger import CustomLogger
from config.config import (
    VIDEO_IMAGES_DIR,
    MAX_CHANNELS
)

# Initialize logger
logger = CustomLogger("main")

def download_video_thumbnails(videos: list) -> int:
    """Download thumbnails for videos."""
    count_success = 0
    today_str = datetime.now().strftime('%d-%m-%Y')
    save_dir = VIDEO_IMAGES_DIR / today_str
    save_dir.mkdir(parents=True, exist_ok=True)
    
    for video in videos:
        video_id = video.get("videoId")
        thumbnail_url = video.get("thumbnailUrl")
        
        if not thumbnail_url or not video_id:
            logger.warning(f"Skipping due to missing videoId or thumbnailUrl")
            continue
            
        try:
            response = requests.get(thumbnail_url)
            if response.status_code == 200:
                save_path = save_dir / f"{video_id}.jpg"
                with open(save_path, "wb") as f:
                    f.write(response.content)
                count_success += 1
                logger.info(f"Saved: {save_path}")
            else:
                logger.warning(f"Failed to download image from {thumbnail_url}")
        except Exception as e:
            logger.error(f"Error with {video_id}: {str(e)}")
            
    return count_success

def crawl_video_in_channel_by_keyword(keyword: str, save_keyword_only: bool = False, published_after: str = None, max_results: int = MAX_CHANNELS) -> Dict[str, Any]:
    """Process a single keyword."""
    api = YouTubeAPI()
    db = Database()
    
    try:
        if published_after is not None:
            search_result = api.search_video_by_keyword_filter_pulished_date(keyword, published_after, max_results=max_results)
            logger.info(f"Search keyword filter pulished date by api key {search_result['api_key']} has {search_result['used_quota']} used quota")
            logger.info(f"Response from api: {len(search_result['channels'])} channels and {len(search_result['videos'])} videos")
        else:
            search_result = api.search_channel_by_keyword(keyword, max_results=max_results)
            logger.info(f"Search keyword all by api key {search_result['api_key']} has {search_result['used_quota']} used quota")
            logger.info(f"Response from api: {len(search_result['channels'])} channels and {len(search_result['videos'])} videos")
        
        channels = search_result["channels"]
        videos = search_result["videos"]
        api_key = search_result["api_key"]
        used_quota = search_result["used_quota"]
        
        # Check channels that don't exist in database
        new_channels = []
        for channel in channels:
            channel_id = channel.get("channelId")
            if channel_id and not db.channel_exists(channel_id):
                new_channels.append(channel)
        
        # Get detailed channel information
        channel_ids = [c["channelId"] for c in new_channels]
        channel_result = api.get_channel_details(channel_ids)
        detailed_channels = channel_result["detailed_channels"]
        used_quota += channel_result["used_quota"]
        
        # Save detailed channels to database
        if detailed_channels:
            channel_result = db.insert_many_channels(detailed_channels)
            logger.info(f"Inserted {channel_result.get('new_channels_count')} new channels successfully")
            logger.info(f"Updated {channel_result.get('updated_channels_count')} existing channels")

            new_channels_ids = channel_result["new_channel_ids"]
        
        # Get videos from channels' uploads playlists
        playlist_result = api.get_channels_playlist_videos(detailed_channels)
        playlist_videos = playlist_result["videos"]
        videos = videos + playlist_videos
        used_quota += playlist_result["used_quota"]
        logger.info(f"After crawl playlist, Inserted {len(playlist_videos)} new videos successfully from playlist of channels")
        logger.info(f"After crawl playlist, used quota: {playlist_result["used_quota"]}")

        # Save videos to database
        data_saved_db = db.insert_many_videos(videos)
        logger.info(f"Inserted {data_saved_db.get('new_videos_count')} new videos successfully")
        logger.info(f"Updated {data_saved_db.get('updated_videos_count')} existing videos")

        new_videos_ids = data_saved_db.get("new_video_ids")

        # Log quota usage information
        logger.info(f"Total Quota Used: {used_quota} units")
        
        return {
            "new_videos": new_videos_ids,
            "new_channels": new_channels_ids,
            "count_channels_from_api": len(channels),
            "count_videos_from_api": len(videos),
            "used_quota": used_quota,
            "api_key": api_key
        }
        
    finally:
        db.close()

def crawl_video_in_channel_by_keyword_from_file():
    """Main function to process keywords from file."""
    keywords_file = Path("keywords.txt")
    if not keywords_file.exists():
        logger.error("Error: keywords.txt file not found")
        return
        
    with open(keywords_file, "r", encoding="utf-8") as f:
        keywords = [line.strip() for line in f if line.strip()]
        
    # Process keywords in batches of 5
    batch_size = 5
    for i in range(0, len(keywords), batch_size):
        batch_keywords = keywords[i:i+batch_size]
        logger.info(f"Processing batch of {len(batch_keywords)} keywords...")
        
        # Collect results for batch processing
        keywords_data = []
        keyword_usage_data = []
        current_api_key = None
        
        for keyword in batch_keywords:
            logger.info(f"Processing keyword: {keyword}")
            result = crawl_video_in_channel_by_keyword(keyword, save_keyword_only=True)
            if result:
                keywords_data.append({
                    "keyword": keyword,
                    "channels": result.get("new_channels", []),
                    "videos": result.get("new_videos", []),
                    "count_channels_from_api": result.get("count_channels_from_api", 0),
                    "count_videos_from_api": result.get("count_videos_from_api", 0)
                })
                
                # Collect keyword usage data
                if result.get("used_quota"):
                    keyword_usage_data.append({
                        "keyword": keyword,
                        "used_quota": result["used_quota"],
                        "crawl_date": result.get("crawlDate", datetime.now()).isoformat()
                    })
                    current_api_key = result.get("api_key")
        
        # Update all keywords in batch
        if keywords_data:
            db = Database()
            try:
                # Update keywords
                results = db.update_many_keywords(keywords_data)
                # logger.info(f"Completed batch: {results}")
                
                # Update keyword usage history if we have data
                if keyword_usage_data and current_api_key:
                    save_keyword_to_apikey_db = db.add_many_keyword_usage(current_api_key, keyword_usage_data)
                    logger.info(f"Inserted {save_keyword_to_apikey_db.get('new_keyword_usage_count')} keyword usage records successfully")
                    logger.info(f"Updated {save_keyword_to_apikey_db.get('updated_api_key_count')} API key documents")
            finally:
                db.close()

if __name__ == "__main__":
    crawl_video_in_channel_by_keyword_from_file() 


