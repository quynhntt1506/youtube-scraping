import json
import requests
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List

from utils.api import YouTubeAPI
from utils.database import Database
from config.config import (
    VIDEO_IMAGES_DIR,
    MAX_CHANNELS
)

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
            print(f"❌ Skipping due to missing videoId or thumbnailUrl")
            continue
            
        try:
            response = requests.get(thumbnail_url)
            if response.status_code == 200:
                save_path = save_dir / f"{video_id}.jpg"
                with open(save_path, "wb") as f:
                    f.write(response.content)
                count_success += 1
                print(f"✅ Saved: {save_path}")
            else:
                print(f"⚠️ Failed to download image from {thumbnail_url}")
        except Exception as e:
            print(f"❌ Error with {video_id}: {str(e)}")
            
    return count_success

def crawl_by_keyword(keyword: str, save_keyword_only: bool = False, published_after: str = None, max_results: int = MAX_CHANNELS) -> Dict[str, Any]:
    """Process a single keyword."""
    api = YouTubeAPI()
    db = Database()
    
    try:
        if published_after is not None:
            search_result = api.search_by_keyword_filter_pulished_date(keyword, published_after, max_results=max_results)
        else:
            search_result = api.search_by_keyword(keyword, max_results=max_results)
        
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
        
        # Save videos to database
        data_saved_db = db.insert_many_videos(videos)
        new_videos = data_saved_db.get("new_video_ids")
        
        # Get detailed channel information
        channel_ids = [c["channelId"] for c in channels]
        channel_result = api.get_channel_details(channel_ids)
        detailed_channels = channel_result["detailed_channels"]
        used_quota += channel_result["used_quota"]
        
        # Save detailed channels to database
        if detailed_channels:
            channel_result = db.insert_many_channels(detailed_channels)
            new_channels = channel_result["new_channel_ids"]
        
        # Print quota usage information
        print(f"\nTotal Quota Used: {used_quota} units")
        
        return {
            "new_videos": new_videos,
            "new_channels": new_channels,
            "count_channels_from_api": len(channels),
            "count_videos_from_api": len(videos),
            "used_quota": used_quota,
            "api_key": api_key
        }
         #     # Download video thumbnails
        #     thumbnail_count = download_video_thumbnails(videos)
    

        
    finally:
        db.close()
    

def main():
    """Main function to process keywords from file."""
    keywords_file = Path("keywords.txt")
    if not keywords_file.exists():
        print("Error: keywords.txt file not found")
        return
        
    with open(keywords_file, "r", encoding="utf-8") as f:
        keywords = [line.strip() for line in f if line.strip()]
        
    # Process keywords in batches of 5
    batch_size = 5
    for i in range(0, len(keywords), batch_size):
        batch_keywords = keywords[i:i+batch_size]
        print(f"\nProcessing batch of {len(batch_keywords)} keywords...")
        
        # Collect results for batch processing
        keywords_data = []
        keyword_usage_data = []
        current_api_key = None
        
        for keyword in batch_keywords:
            print(f"Processing keyword: {keyword}")
            result = crawl_by_keyword(keyword, save_keyword_only=True)
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
                print(f"Completed batch: {results}")
                
                # Update keyword usage history if we have data
                if keyword_usage_data and current_api_key:
                    db.add_many_keyword_usage(current_api_key, keyword_usage_data)
            finally:
                db.close()

if __name__ == "__main__":
    main() 