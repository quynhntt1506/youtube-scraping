from typing import Dict, Any
from pathlib import Path
from datetime import datetime

from utils.logger import CustomLogger
from utils.api import YouTubeAPI
from utils.database import Database
from utils.api_key_manager import APIKeyManager
from src.controller.image_downloader import download_channel_images
from src.controller.thumbnail_downloader import download_video_thumbnails
from config.config import MAX_CHANNELS

# Initialize logger
logger = CustomLogger("crawler")

def crawl_video_in_channel_by_keyword(keyword: str, save_keyword_only: bool = False, published_after: str = None, max_results: int = MAX_CHANNELS) -> Dict[str, Any]:
    """Process a single keyword."""
    api = YouTubeAPI()
    db = Database()
    api_manager = APIKeyManager(db)
    
    # Initialize variables
    new_channels_ids = []
    new_videos_ids = []
    used_quota = 0
    
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

        # Download channel images
        if detailed_channels:
            image_result = download_channel_images(detailed_channels)
            logger.info(f"Downloaded {image_result['avatars']} avatars and {image_result['banners']} banners")
            # Save detailed channels to database
            channel_result = db.insert_many_channels(image_result["updated_channels"])
            logger.info(f"Inserted {channel_result.get('new_channels_count')} new channels successfully")
            logger.info(f"Updated {channel_result.get('updated_channels_count')} existing channels")

            new_channels_ids = channel_result["new_channel_ids"]
        
        # Get videos from channels' uploads playlists
        playlist_result = api.get_channels_playlist_videos(detailed_channels)
        playlist_videos = playlist_result["videos"]
        videos = videos + playlist_videos
        used_quota += playlist_result["used_quota"]
        logger.info(f"After crawl playlist, Inserted {len(playlist_videos)} new videos successfully from playlist of channels")
        logger.info(f"After crawl playlist, used quota: {playlist_result['used_quota']}")
        logger.info(f"Start download thumbnails videos")
        # Download thumbnail videos
        result_download_thumbnails = download_video_thumbnails(videos)
        logger.info(f"Downloaded {result_download_thumbnails['count']} thumbnails for new videos")
        logger.info(f"Start save videos to database")
        # Save videos to database
        data_saved_db = db.insert_many_videos(result_download_thumbnails["updated_videos"])
        logger.info(f"Inserted {data_saved_db.get('new_videos_count')} new videos successfully")
        logger.info(f"Updated {data_saved_db.get('updated_videos_count')} existing videos")

        new_videos_ids = data_saved_db.get("new_video_ids", [])
        
        # Update quota for each api_key used
        for api_key, quota in api.quota_usage.items():
            api_manager.update_quota(api_key, quota)
            logger.info(f"Updated quota for API key {api_key}: {quota} units")
        
        return {
            "new_videos": new_videos_ids,
            "new_channels": new_channels_ids,
            "count_channels_from_api": len(channels),
            "count_videos_from_api": len(videos),
            "used_quota": used_quota,
            "quota_usage": api.quota_usage
        }
        
    finally:
        db.close()

def crawl_video_in_channel_by_many_keywords(keywords: list[str]):
    # """Main function to process keywords from file."""
    # keywords_file = Path("keywords.txt")
    # if not keywords_file.exists():
    #     logger.error("Error: keywords.txt file not found")
    #     return
        
    # with open(keywords_file, "r", encoding="utf-8") as f:
    #     keywords = [line.strip() for line in f if line.strip()]
        
    # Process keywords in batches of 5
    batch_size = 5
    for i in range(0, len(keywords), batch_size):
        batch_keywords = keywords[i:i+batch_size]
        logger.info(f"Processing batch of {len(batch_keywords)} keywords...")
        
        # Collect results for batch processing
        keywords_data = []
        
        for keyword in batch_keywords:
            logger.info(f"Processing keyword: {keyword}")
            # Check if keyword is already crawled
            db = Database()
            keyword_doc = db.get_keyword_by_keyword(keyword)
            if keyword_doc and keyword_doc.get("status") == "crawled":
                logger.info(f"Keyword {keyword} is already crawled, skipping...")
                continue
            elif keyword_doc and keyword_doc.get("status") == "to_crawl":
                # Update status to crawling
                db.update_keyword_status(keyword, "crawling")
                logger.info(f"Updated status of keyword {keyword} to 'crawling'")
                
                result = crawl_video_in_channel_by_keyword(keyword, save_keyword_only=True)
                if result:
                    keywords_data.append({
                        "keyword": keyword,
                        "channels": result.get("new_channels", []),
                        "videos": result.get("new_videos", []),
                        "count_channels_from_api": result.get("count_channels_from_api", 0),
                        "count_videos_from_api": result.get("count_videos_from_api", 0)
                    })
                    
                    # Process quota usage for each api_key
                    quota_usage = result.get("quota_usage", {})
                    for api_key, used_quota in quota_usage.items():
                        # Create keyword usage data for this api_key
                        keyword_usage_data = [{
                            "keyword": keyword,
                            "used_quota": used_quota,
                            "crawl_date": datetime.now().isoformat()
                        }]
                        
                        # Add keyword usage history for this api_key
                        db = Database()
                        try:
                            save_keyword_to_apikey_db = db.add_many_keyword_usage(api_key, keyword_usage_data)
                            logger.info(f"Added keyword usage for API key {api_key}:")
                            logger.info(f"- Keyword: {keyword}")
                            logger.info(f"- Used quota: {used_quota}")
                            logger.info(f"- Inserted {save_keyword_to_apikey_db.get('new_keyword_usage_count')} keyword usage records")
                            logger.info(f"- Updated {save_keyword_to_apikey_db.get('updated_api_key_count')} API key documents")
                            db.update_keyword_status(keyword, "crawled")
                            logger.info(f"Updated status of keyword {keyword} to 'crawled'")
                        finally:
                            db.close()
            else:
                logger.warning(f"Keyword {keyword} not found in database or has invalid status")
        
        # Update all keywords in batch
        if keywords_data:
            db = Database()
            try:
                # Update keywords
                results = db.update_many_keywords(keywords_data)
                logger.info(f"Processed {results.get('count_operations')} keywords successfully")
                logger.info(f"Inserted {results.get('new_keywords_count')} new keywords")
                logger.info(f"Updated {results.get('updated_keywords_count')} existing keywords")
            finally:
                db.close() 