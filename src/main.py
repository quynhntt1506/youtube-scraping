import json
import requests
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

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

def process_keyword(keyword: str, save_keyword_only: bool = False) -> Dict[str, Any]:
    """Process a single keyword."""
    api = YouTubeAPI()
    db = Database()
    
    try:
        # Search for channels and videos
        search_result = api.search(keyword, MAX_CHANNELS)
        channels = search_result["channels"]
        videos = search_result["videos"]
        
        # Check channels that don't exist in database
        new_channels = []
        for channel in channels:
            channel_id = channel.get("channelId")
            if channel_id and not db.channel_exists(channel_id):
                new_channels.append(channel)
        
        # Check and insert videos that don't exist in database
        new_videos = []
        for video in videos:
            video_id = video.get("videoId")
            if video_id and not db.video_exists(video_id):
                new_videos.append(video)
                db.insert_video(video)
        
        if save_keyword_only:
            # Save keyword data to database
            db.update_keyword_data(keyword, new_channels, new_videos, len(channels), len(videos))
            return {
                "keyword": keyword,
                "count_channels": len(channels),
                "count_videos": len(videos),
                "count_new_channels": len(new_channels),
                "count_new_videos": len(new_videos)
            }
        else:
            # Get detailed channel information
            channel_ids = [c["channelId"] for c in channels]
            detailed_channels = api.get_channel_details(channel_ids)
            
            # Download video thumbnails
            thumbnail_count = download_video_thumbnails(videos)
        
            # Save results
            result = {
                "keyword": keyword,
                "count_channels": len(detailed_channels),
                "count_videos": len(videos),
                "count_new_channels": len(new_channels),
                "count_new_videos": len(new_videos),
                "count_thumbnails": thumbnail_count,
                "channels": detailed_channels,
                "videos": videos
            }
            return result
        
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
        
    for keyword in keywords:
        print(f"\nProcessing keyword: {keyword}")
        result = process_keyword(keyword, save_keyword_only=True)
        print(f"Completed: {result}")

if __name__ == "__main__":
    main() 