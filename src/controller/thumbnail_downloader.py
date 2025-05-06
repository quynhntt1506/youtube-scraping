import asyncio
import aiohttp
from pathlib import Path
from typing import Dict, Any
from datetime import datetime

from utils.logger import CustomLogger
from config.config import VIDEO_IMAGES_DIR

# Initialize logger
logger = CustomLogger("thumbnail_downloader")

async def download_thumbnail(session: aiohttp.ClientSession, video_id: str, thumbnail_url: str, save_path: Path) -> bool:
    """Download a single thumbnail asynchronously."""
    try:
        async with session.get(thumbnail_url) as response:
            if response.status == 200:
                content = await response.read()
                save_path.write_bytes(content)
                return True
            else:
                logger.warning(f"Failed to download thumbnail from {thumbnail_url}")
                return False
    except Exception as e:
        logger.error(f"Error downloading thumbnail for video {video_id}: {str(e)}")
        return False

async def download_batch_thumbnails(videos: list, base_dir: Path, folder_name: str) -> Dict[str, Any]:
    """Download a batch of thumbnails concurrently."""
    count_success = 0
    updated_videos = []
    
    async with aiohttp.ClientSession() as session:
        tasks = []
        video_paths = {}  # Store video_id -> save_path mapping
        
        for video in videos:
            video_id = video.get("videoId")
            thumbnail_url = video.get("thumbnailUrl")
            
            if not thumbnail_url or not video_id:
                continue
                
            save_path = base_dir / folder_name / f"{video_id}.jpg"
            # Remove project root path
            relative_path = str(save_path).replace("D:/OSINT/youtube-crawl/", "")
            video_paths[video_id] = relative_path
            tasks.append(download_thumbnail(session, video_id, thumbnail_url, save_path))
        
        if tasks:
            # Download all thumbnails concurrently
            results = await asyncio.gather(*tasks)
            
            # Update videos with successful downloads
            for i, (video, success) in enumerate(zip(videos, results)):
                if success:
                    count_success += 1
                    video_data = video.copy()
                    video_data["thumbnailPath"] = str(video_paths[video["videoId"]])
                    updated_videos.append(video_data)
            
    return {
        "count": count_success,
        "updated_videos": updated_videos
    }

def download_video_thumbnails(videos: list) -> Dict[str, Any]:
    """Download thumbnails for videos in batches of 100."""
    count_success = 0
    updated_videos = []
    today_str = datetime.now().strftime('%d-%m-%Y')
    base_dir = VIDEO_IMAGES_DIR / today_str
    base_dir.mkdir(parents=True, exist_ok=True)
    
    # Get current folder number by counting existing folders
    existing_folders = [f for f in base_dir.iterdir() if f.is_dir()]
    current_folder_num = len(existing_folders) + 1
    start_num = (current_folder_num - 1) * 5000 + 1
    end_num = current_folder_num * 5000
    current_folder_name = f"{start_num}-{end_num}"
    current_folder_path = base_dir / current_folder_name
    current_folder_path.mkdir(exist_ok=True)
    
    # Process videos in batches of 100
    for i in range(0, len(videos), 100):
        batch = videos[i:i+100]
        logger.info(f"Processing batch {i//100 + 1} of {(len(videos) + 99) // 100}")
        
        # Download batch concurrently
        results = asyncio.run(download_batch_thumbnails(batch, base_dir, current_folder_name))
        count_success += results["count"]
        updated_videos.extend(results["updated_videos"])
        
        # Check if current folder has reached 5000 files
        current_files = len(list(current_folder_path.glob("*.jpg")))
        if current_files >= 5000:
            # Create new folder for next batch
            current_folder_num += 1
            start_num = (current_folder_num - 1) * 5000 + 1
            end_num = current_folder_num * 5000
            current_folder_name = f"{start_num}-{end_num}"
            current_folder_path = base_dir / current_folder_name
            current_folder_path.mkdir(exist_ok=True)
            logger.info(f"Created new folder {current_folder_name} after reaching 5000 files")
        
        # Log progress after each batch
        logger.info(f"Completed batch {i//100 + 1}. Total downloaded: {count_success}")
        
    return {
        "count": count_success,
        "updated_videos": updated_videos
    } 