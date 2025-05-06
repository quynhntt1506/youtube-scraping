import asyncio
import aiohttp
from pathlib import Path
from typing import Dict, List, Any
from datetime import datetime

from utils.logger import CustomLogger
from config.config import CHANNEL_IMAGES_DIR, VIDEO_IMAGES_DIR

# Initialize logger
logger = CustomLogger("image_downloader")

async def download_image(session: aiohttp.ClientSession, url: str, save_path: Path) -> bool:
    """Download a single image asynchronously."""
    try:
        async with session.get(url) as response:
            if response.status == 200:
                content = await response.read()
                save_path.write_bytes(content)
                return True
            else:
                logger.warning(f"Failed to download image from {url}")
                return False
    except Exception as e:
        logger.error(f"Error downloading image from {url}: {str(e)}")
        return False

async def download_batch_images(channels: list, avatars_dir: Path, banners_dir: Path) -> Dict[str, List[Dict]]:
    """Download a batch of images concurrently."""
    count_avatars = 0
    count_banners = 0
    updated_channels = []
    
    async with aiohttp.ClientSession() as session:
        tasks = []
        channel_paths = {}  # Store channel_id -> (avatar_path, banner_path) mapping
        
        for channel in channels:
            channel_id = channel.get("channelId")
            if not channel_id:
                continue
                
            avatar_url = channel.get("avatarUrl")
            banner_url = channel.get("bannerUrl")
            channel_data = channel.copy()
            
            if avatar_url:
                avatar_path = avatars_dir / f"{channel_id}.jpg"
                # Remove project root path
                relative_avatar_path = str(avatar_path).replace("D:/OSINT/youtube-crawl/", "")
                channel_paths[channel_id] = {"avatar": relative_avatar_path}
                tasks.append(download_image(session, avatar_url, avatar_path))
                
            if banner_url:
                banner_path = banners_dir / f"{channel_id}.jpg"
                # Remove project root path
                relative_banner_path = str(banner_path).replace("D:/OSINT/youtube-crawl/", "")
                if channel_id in channel_paths:
                    channel_paths[channel_id]["banner"] = relative_banner_path
                else:
                    channel_paths[channel_id] = {"banner": relative_banner_path}
                tasks.append(download_image(session, banner_url, banner_path))
        
        if tasks:
            # Download all images concurrently
            results = await asyncio.gather(*tasks)
            
            # Update channels with successful downloads
            task_index = 0
            for channel in channels:
                channel_id = channel.get("channelId")
                if not channel_id:
                    continue
                    
                channel_data = channel.copy()
                paths = channel_paths.get(channel_id, {})
                
                # Check avatar download result
                if "avatar" in paths:
                    if results[task_index]:
                        count_avatars += 1
                        channel_data["avatarPath"] = str(paths["avatar"])
                    task_index += 1
                
                # Check banner download result
                if "banner" in paths:
                    if results[task_index]:
                        count_banners += 1
                        channel_data["bannerPath"] = str(paths["banner"])
                    task_index += 1
                
                updated_channels.append(channel_data)
            
    return {
        "avatars": count_avatars,
        "banners": count_banners,
        "updated_channels": updated_channels
    }

def download_channel_images(detailed_channels: list) -> Dict[str, Any]:
    """Download channel avatars and banners concurrently in batches of 100."""
    total_avatars = 0
    total_banners = 0
    updated_channels = []
    today_str = datetime.now().strftime('%Y-%m-%d')
    base_dir = CHANNEL_IMAGES_DIR / today_str
    base_dir.mkdir(parents=True, exist_ok=True)
    
    # Create avatars and banners directories
    avatars_dir = base_dir / "avatars"
    banners_dir = base_dir / "banners"
    avatars_dir.mkdir(exist_ok=True)
    banners_dir.mkdir(exist_ok=True)
    
    # Get current folder numbers by counting existing folders
    existing_avatar_folders = [f for f in avatars_dir.iterdir() if f.is_dir()]
    existing_banner_folders = [f for f in banners_dir.iterdir() if f.is_dir()]
    
    current_avatar_folder_num = len(existing_avatar_folders) + 1
    current_banner_folder_num = len(existing_banner_folders) + 1
    
    # Initialize current folders
    avatar_start = (current_avatar_folder_num - 1) * 5000 + 1
    avatar_end = current_avatar_folder_num * 5000
    current_avatar_folder = avatars_dir / f"{avatar_start}-{avatar_end}"
    current_avatar_folder.mkdir(exist_ok=True)
    
    banner_start = (current_banner_folder_num - 1) * 5000 + 1
    banner_end = current_banner_folder_num * 5000
    current_banner_folder = banners_dir / f"{banner_start}-{banner_end}"
    current_banner_folder.mkdir(exist_ok=True)
    
    # Process channels in batches of 100
    for i in range(0, len(detailed_channels), 100):
        batch = detailed_channels[i:i+100]
        logger.info(f"Processing batch {i//100 + 1} of {(len(detailed_channels) + 99) // 100}")
        
        # Download batch concurrently
        results = asyncio.run(download_batch_images(batch, current_avatar_folder, current_banner_folder))
        total_avatars += results["avatars"]
        total_banners += results["banners"]
        updated_channels.extend(results["updated_channels"])
        
        # Check if current folders have reached 5000 files
        current_avatar_files = len(list(current_avatar_folder.glob("*.jpg")))
        current_banner_files = len(list(current_banner_folder.glob("*.jpg")))
        
        # Create new avatar folder if needed
        if current_avatar_files >= 5000:
            current_avatar_folder_num += 1
            avatar_start = (current_avatar_folder_num - 1) * 5000 + 1
            avatar_end = current_avatar_folder_num * 5000
            current_avatar_folder = avatars_dir / f"{avatar_start}-{avatar_end}"
            current_avatar_folder.mkdir(exist_ok=True)
            logger.info(f"Created new avatar folder {current_avatar_folder.name} after reaching 5000 files")
        
        # Create new banner folder if needed
        if current_banner_files >= 5000:
            current_banner_folder_num += 1
            banner_start = (current_banner_folder_num - 1) * 5000 + 1
            banner_end = current_banner_folder_num * 5000
            current_banner_folder = banners_dir / f"{banner_start}-{banner_end}"
            current_banner_folder.mkdir(exist_ok=True)
            logger.info(f"Created new banner folder {current_banner_folder.name} after reaching 5000 files")
        
        # Log progress after each batch
        logger.info(f"Completed batch {i//100 + 1}. Downloaded {total_avatars} avatars and {total_banners} banners so far")
    
    return {
        "avatars": total_avatars,
        "banners": total_banners,
        "updated_channels": updated_channels
    } 