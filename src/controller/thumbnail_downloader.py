import os
import asyncio
import aiohttp
import paramiko
from pathlib import Path
from typing import Dict, Any
from datetime import datetime

from src.utils.logger import CustomLogger
from src.config.config import DIRECTORIES, MAX_FILES_PER_FOLDER

# Initialize logger
logger = CustomLogger("thumbnail_downloader")

# SFTP Configuration for Docker environment
SFTP_CONFIG = {
    "hostname": "192.168.161.230",
    "username": "htsc",
    "password": "Htsc@123",
    "remote_base_path": "/mnt/data/youtube"
}

def count_files_in_remote_dir(sftp, remote_dir: str) -> int:
    """Count number of .jpg files in a remote directory using SFTP."""
    try:
        return len([f for f in sftp.listdir(remote_dir) if f.endswith('.jpg')])
    except Exception as e:
        logger.error(f"Error counting files in remote directory {remote_dir}: {str(e)}")
        return 0

def ensure_remote_dir_exists(sftp, remote_dir: str) -> bool:
    """Ensure remote directory exists, create if it doesn't."""
    try:
        sftp.stat(remote_dir)
        return True
    except FileNotFoundError:
        try:
            sftp.mkdir(remote_dir)
            return True
        except Exception as e:
            logger.error(f"Error creating remote directory {remote_dir}: {str(e)}")
            return False

async def download_thumbnail(session: aiohttp.ClientSession, video_id: str, thumbnail_url: str, save_path: Path, is_docker: bool = False) -> bool:
    """Download a single thumbnail asynchronously and save it either locally or via SFTP."""
    try:
        async with session.get(thumbnail_url) as response:
            if response.status == 200:
                content = await response.read()
                
                if is_docker:
                    # Save to remote server via SFTP when in Docker
                    try:
                        with paramiko.SSHClient() as ssh:
                            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                            ssh.connect(
                                hostname=SFTP_CONFIG["hostname"],
                                username=SFTP_CONFIG["username"],
                                password=SFTP_CONFIG["password"]
                            )
                            
                            with ssh.open_sftp() as sftp:
                                # Ensure remote directory exists
                                remote_dir = os.path.dirname(str(save_path))
                                if not ensure_remote_dir_exists(sftp, remote_dir):
                                    return False
                                
                                # Save file
                                with sftp.file(str(save_path), 'wb') as remote_file:
                                    remote_file.write(content)
                        logger.info(f"Successfully saved thumbnail to remote server: {save_path}")
                    except Exception as e:
                        logger.error(f"SFTP Error: {str(e)}")
                        return False
                else:
                    # Save locally when not in Docker
                    save_path.write_bytes(content)
                    logger.info(f"Successfully saved thumbnail locally: {save_path}")
                
                return True
            else:
                logger.warning(f"Failed to download thumbnail from {thumbnail_url}")
                return False
    except Exception as e:
        logger.error(f"Error downloading thumbnail for video {video_id}: {str(e)}")
        return False

async def download_batch_thumbnails(videos: list, base_dir: Path, folder_name: str, is_docker: bool = False) -> Dict[str, Any]:
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
                
            save_path = base_dir / folder_name / f"{video_id}_thumbnail.jpg"
            # Remove project root path for local storage
            if not is_docker:
                relative_path = str(save_path).replace("D:/OSINT/youtube-crawl/", "")
            else:
                relative_path = str(save_path)
            video_paths[video_id] = relative_path
            tasks.append(download_thumbnail(session, video_id, thumbnail_url, save_path, is_docker))
        
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
    today_str = datetime.now().strftime('%Y-%m-%d')
    is_docker = os.path.exists('/.dockerenv')
    
    logger.info(f"Running in {'Docker' if is_docker else 'local'} environment")
    
    # Chọn thư mục gốc dựa trên môi trường
    if is_docker:
        # Sử dụng đường dẫn SFTP khi chạy trong Docker
        base_dir = Path(SFTP_CONFIG["remote_base_path"]) / "videos" / "thumbnails"
        logger.info(f"Using remote storage path: {base_dir}")
    else:
        # Sử dụng đường dẫn local khi chạy bình thường
        base_dir = DIRECTORIES["thumbnailvideos"]
        logger.info(f"Using local storage path: {base_dir}")
    
    base_dir = base_dir / today_str
    
    # Create base directory
    if is_docker:
        with paramiko.SSHClient() as ssh:
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(
                hostname=SFTP_CONFIG["hostname"],
                username=SFTP_CONFIG["username"],
                password=SFTP_CONFIG["password"]
            )
            with ssh.open_sftp() as sftp:
                ensure_remote_dir_exists(sftp, str(base_dir))
    else:
        base_dir.mkdir(parents=True, exist_ok=True)
    
    # Get current folder number by counting existing folders
    if is_docker:
        with paramiko.SSHClient() as ssh:
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(
                hostname=SFTP_CONFIG["hostname"],
                username=SFTP_CONFIG["username"],
                password=SFTP_CONFIG["password"]
            )
            with ssh.open_sftp() as sftp:
                try:
                    existing_folders = [f for f in sftp.listdir(str(base_dir)) if sftp.stat(f"{base_dir}/{f}").st_mode & 0o40000]
                    current_folder_num = len(existing_folders) + 1
                except Exception as e:
                    logger.error(f"Error listing remote directories: {str(e)}")
                    current_folder_num = 1
    else:
        existing_folders = [f for f in base_dir.iterdir() if f.is_dir()]
        current_folder_num = len(existing_folders) + 1
    
    current_folder_name = str(current_folder_num)
    current_folder_path = base_dir / current_folder_name
    
    # Create current folder
    if is_docker:
        with paramiko.SSHClient() as ssh:
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(
                hostname=SFTP_CONFIG["hostname"],
                username=SFTP_CONFIG["username"],
                password=SFTP_CONFIG["password"]
            )
            with ssh.open_sftp() as sftp:
                ensure_remote_dir_exists(sftp, str(current_folder_path))
    else:
        current_folder_path.mkdir(exist_ok=True)
    
    # Process videos in batches of 100
    for i in range(0, len(videos), 100):
        batch = videos[i:i+100]
        logger.info(f"Processing batch {i//100 + 1} of {(len(videos) + 99) // 100}")
        
        # Download batch concurrently
        results = asyncio.run(download_batch_thumbnails(batch, base_dir, current_folder_name, is_docker))
        count_success += results["count"]
        updated_videos.extend(results["updated_videos"])
        
        # Check if current folder has reached 5000 files
        if is_docker:
            with paramiko.SSHClient() as ssh:
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(
                    hostname=SFTP_CONFIG["hostname"],
                    username=SFTP_CONFIG["username"],
                    password=SFTP_CONFIG["password"]
                )
                with ssh.open_sftp() as sftp:
                    current_files = count_files_in_remote_dir(sftp, str(current_folder_path))
        else:
            current_files = len(list(current_folder_path.glob("*.jpg")))
            
        if current_files >= MAX_FILES_PER_FOLDER:
            # Create new folder for next batch
            current_folder_num += 1
            current_folder_name = str(current_folder_num)
            current_folder_path = base_dir / current_folder_name
            
            # Create new folder
            if is_docker:
                with paramiko.SSHClient() as ssh:
                    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                    ssh.connect(
                        hostname=SFTP_CONFIG["hostname"],
                        username=SFTP_CONFIG["username"],
                        password=SFTP_CONFIG["password"]
                    )
                    with ssh.open_sftp() as sftp:
                        ensure_remote_dir_exists(sftp, str(current_folder_path))
            else:
                current_folder_path.mkdir(exist_ok=True)
                
            logger.info(f"Created new folder {current_folder_name} after reaching 5000 files")
        
        # Log progress after each batch
        logger.info(f"Completed batch {i//100 + 1}. Total downloaded: {count_success}")
        
    return {
        "count": count_success,
        "updated_videos": updated_videos
    } 