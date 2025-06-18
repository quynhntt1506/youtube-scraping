import asyncio
import aiohttp
import os
import paramiko
from pathlib import Path
from typing import Dict, List, Any
from datetime import datetime
from src.utils.logger import CustomLogger
from src.config.config import DIRECTORIES, COUNT_FILES_DOWNLOAD_SIMULTANEOUSLY, MAX_FILES_PER_FOLDER, SFTP_CONFIG
import tempfile

# Initialize logger
logger = CustomLogger("image_downloader")

# SFTP Configuration for Docker environment
# SFTP_CONFIG = {
#     "hostname": "192.168.132.250",
#     "username": "htsc",
#     "password": "Htsc@123",
#     "remote_base_path": "/crawl-youtube/data"
# }

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
            # Create parent directories first
            parent_dir = os.path.dirname(remote_dir)
            if parent_dir and parent_dir != '/':
                ensure_remote_dir_exists(sftp, parent_dir)
            sftp.mkdir(remote_dir)
            logger.info(f"Created remote directory: {remote_dir}")
            return True
        except Exception as e:
            logger.error(f"Error creating remote directory {remote_dir}: {str(e)}")
            return False

def create_remote_directories(sftp, base_dir: str) -> bool:
    """Create all necessary remote directories."""
    try:
        # Create base directory structure
        directories = [
            base_dir,
            f"{base_dir}/channels",
            f"{base_dir}/channels/avatars",
            f"{base_dir}/channels/banners",
            f"{base_dir}/videos",
            f"{base_dir}/videos/thumbnails"
        ]
        
        for directory in directories:
            if not ensure_remote_dir_exists(sftp, directory):
                logger.error(f"Failed to create directory: {directory}")
                return False
                
        logger.info("Successfully created all remote directories")
        return True
    except Exception as e:
        logger.error(f"Error creating remote directories: {str(e)}")
        return False

async def download_image(session: aiohttp.ClientSession, url: str, save_path: str, is_docker: bool = False) -> bool:
    """Download a single image and save it."""
    try:
        async with session.get(url) as response:
            if response.status == 200:
                content = await response.read()
                
                if is_docker:
                    # Create SFTP client
                    with paramiko.SSHClient() as ssh:
                        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                        ssh.connect(
                            hostname=SFTP_CONFIG["hostname"],
                            username=SFTP_CONFIG["username"],
                            password=SFTP_CONFIG["password"]
                        )
                        
                        with ssh.open_sftp() as sftp:
                            # Create temporary file
                            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                                temp_file.write(content)
                                temp_file.flush()
                                temp_path = temp_file.name
                                
                                try:
                        
                                    
                                    # Upload file using put
                                    sftp.put(temp_path, save_path)
                                    
                                    # Verify file size
                                    remote_size = sftp.stat(save_path).st_size
                                    if remote_size == len(content):
                                        return True
                                    else:
                                        logger.error(f"Size mismatch: original={len(content)}, uploaded={remote_size}")
                                        return False
                                finally:
                                    # Clean up temp file
                                    try:
                                        os.unlink(temp_path)
                                    except:
                                        pass
                else:
                    # Save locally
                    with open(save_path, 'wb') as f:
                        f.write(content)
                    return True
            else:
                logger.error(f"Failed to download image from {url}: {response.status}")
                return False
    except Exception as e:
        logger.error(f"Error downloading image from {url}: {str(e)}")
        return False

async def download_batch_images(channels: list, avatars_dir: str, banners_dir: str, is_docker: bool = False) -> Dict[str, List[Dict]]:
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
            print("AVATAR: ", avatar_url)
            if avatar_url:
                avatar_path = avatars_dir + f"/{channel_id}_avatar.jpg"
                # Remove project root path for local storage
                if not is_docker:
                    relative_avatar_path = avatar_path.replace("D:/OSINT/youtube-crawl/", "")
                else:
                    relative_avatar_path = avatar_path
                channel_paths[channel_id] = {"avatar": relative_avatar_path}
                tasks.append(download_image(session, avatar_url, avatar_path, is_docker))
                
            if banner_url:
                banner_path = banners_dir + f"/{channel_id}_banner.jpg"
                # Remove project root path for local storage
                if not is_docker:
                    relative_banner_path = banner_path.replace("D:/OSINT/youtube-crawl/", "")
                else:
                    relative_banner_path = banner_path
                if channel_id in channel_paths:
                    channel_paths[channel_id]["banner"] = relative_banner_path
                else:
                    channel_paths[channel_id] = {"banner": relative_banner_path}
                tasks.append(download_image(session, banner_url, banner_path, is_docker))
        
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
                        # channel_data["avatarPath"] = str(paths["avatar"])
                        channel_data["avatarPath"] = paths["avatar"]
                    task_index += 1
                
                # Check banner download result
                if "banner" in paths:
                    if results[task_index]:
                        count_banners += 1
                        channel_data["bannerPath"] = paths["banner"]
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
    # is_docker = os.path.exists('/.dockerenv')
    is_docker = True
    
    logger.info(f"Running in {'Docker' if is_docker else 'local'} environment")
        
    # Chọn thư mục gốc dựa trên môi trường
    # if is_docker:
        # Sử dụng đường dẫn SFTP khi chạy trong Docker
    channels_dir = SFTP_CONFIG["remote_base_path"] + "/channels"
    logger.info(f"Using remote storage path: {channels_dir}")
    
    # Create all necessary directories first
    with paramiko.SSHClient() as ssh:
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(
            hostname=SFTP_CONFIG["hostname"],
            username=SFTP_CONFIG["username"],
            password=SFTP_CONFIG["password"]
        )
        with ssh.open_sftp() as sftp:
            if not create_remote_directories(sftp, str(SFTP_CONFIG["remote_base_path"])):
                logger.error("Failed to create remote directories. Exiting...")
                return {
                    "avatars": 0,
                    "banners": 0,
                    "updated_channels": []
                }
    # else:
    #     # Sử dụng đường dẫn local khi chạy bình thường
    #     channels_dir = DIRECTORIES["channels"]
    #     logger.info(f"Using local storage path: {channels_dir}")
        
    avatars_dir = channels_dir + "/avatars"
    banners_dir = channels_dir + "/banners"
    # avatars_dir = avatars_all_dir / today_str
    # banners_dir = banners_all_dir / today_str
    
    # Create directories
    # if is_docker:
    #     with paramiko.SSHClient() as ssh:
    #         ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    #         ssh.connect(
    #             hostname=SFTP_CONFIG["hostname"],
    #             username=SFTP_CONFIG["username"],
    #             password=SFTP_CONFIG["password"]
    #         )
    #         # with ssh.open_sftp() as sftp:
    #         #     ensure_remote_dir_exists(sftp, str(avatars_dir))
    #         #     ensure_remote_dir_exists(sftp, str(banners_dir))
    # else:
    #     avatars_dir.mkdir(parents=True, exist_ok=True)
    #     avatars_dir.mkdir(parents=True, exist_ok=True)
        # avatars_dir.mkdir(parents=True, exist_ok=True)
        # banners_dir.mkdir(parents=True, exist_ok=True)
    
    
    # Process channels in batches of 100
    for i in range(0, len(detailed_channels), COUNT_FILES_DOWNLOAD_SIMULTANEOUSLY):
        batch = detailed_channels[i:i+COUNT_FILES_DOWNLOAD_SIMULTANEOUSLY]
        logger.info(f"Processing batch {i//COUNT_FILES_DOWNLOAD_SIMULTANEOUSLY + 1} of {(len(detailed_channels) + (COUNT_FILES_DOWNLOAD_SIMULTANEOUSLY - 1)) // COUNT_FILES_DOWNLOAD_SIMULTANEOUSLY}")
        
        # Download batch concurrently
        results = asyncio.run(download_batch_images(batch, avatars_dir, banners_dir, is_docker))
        total_avatars += results["avatars"]
        total_banners += results["banners"]
        updated_channels.extend(results["updated_channels"])
        
        # # Check if current folders have reached 5000 files
        # if is_docker:
        #     with paramiko.SSHClient() as ssh:
        #         ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        #         ssh.connect(
        #             hostname=SFTP_CONFIG["hostname"],
        #             username=SFTP_CONFIG["username"],
        #             password=SFTP_CONFIG["password"]
        #         )
        #         with ssh.open_sftp() as sftp:
        #             current_avatar_files = count_files_in_remote_dir(sftp, str(current_avatar_folder))
        #             current_banner_files = count_files_in_remote_dir(sftp, str(current_banner_folder))
        # else:
        #     current_avatar_files = len(list(current_avatar_folder.glob("*.jpg")))
        #     current_banner_files = len(list(current_banner_folder.glob("*.jpg")))
        
        # # Create new avatar folder if needed
        # if current_avatar_files >= MAX_FILES_PER_FOLDER:
        #     current_avatar_folder_num += 1
        #     current_avatar_folder = avatars_dir / str(current_avatar_folder_num)
            
        #     if is_docker:
        #         with paramiko.SSHClient() as ssh:
        #             ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        #             ssh.connect(
        #                 hostname=SFTP_CONFIG["hostname"],
        #                 username=SFTP_CONFIG["username"],
        #                 password=SFTP_CONFIG["password"]
        #             )
        #             with ssh.open_sftp() as sftp:
        #                 ensure_remote_dir_exists(sftp, str(current_avatar_folder))
        #     else:
        #         current_avatar_folder.mkdir(exist_ok=True)
                
        #     logger.info(f"Created new avatar folder {current_avatar_folder.name} after reaching 5000 files")
        
        # # Create new banner folder if needed
        # if current_banner_files >= MAX_FILES_PER_FOLDER:
        #     current_banner_folder_num += 1
        #     current_banner_folder = banners_dir / str(current_banner_folder_num)
            
        #     if is_docker:
        #         with paramiko.SSHClient() as ssh:
        #             ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        #             ssh.connect(
        #                 hostname=SFTP_CONFIG["hostname"],
        #                 username=SFTP_CONFIG["username"],
        #                 password=SFTP_CONFIG["password"]
        #             )
        #             with ssh.open_sftp() as sftp:
        #                 ensure_remote_dir_exists(sftp, str(current_banner_folder))
        #     else:
        #         current_banner_folder.mkdir(exist_ok=True)
                
        #     logger.info(f"Created new banner folder {current_banner_folder.name} after reaching 5000 files")
        
        # # Log progress after each batch
        # logger.info(f"Completed batch {i//COUNT_FILES_DOWNLOAD_SIMULTANEOUSLY + 1}. Downloaded {total_avatars} avatars and {total_banners} banners so far")
    
    return {
        "avatars": total_avatars,
        "banners": total_banners,
        "updated_channels": updated_channels
    } 

def read_file_from_sftp(remote_path: str) -> bytes:
    """Read file from remote server using SFTP."""
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        # Connect to remote server
        ssh.connect(
            hostname=SFTP_CONFIG["hostname"],
            username=SFTP_CONFIG["username"],
            password=SFTP_CONFIG["password"]
        )
        
        # Create SFTP client
        sftp = ssh.open_sftp()
        
        # Create temporary file
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_path = temp_file.name
            
            try:
                # Download file
                sftp.get(remote_path, temp_path)
                
                # Read content
                with open(temp_path, 'rb') as f:
                    content = f.read()
                    
                return content
                
            finally:
                # Clean up temp file
                try:
                    os.unlink(temp_path)
                except:
                    pass
                    
    except Exception as e:
        logger.error(f"Error reading file from remote server: {str(e)}")
        raise
    finally:
        ssh.close()

def check_file_exists_sftp(remote_path: str) -> bool:
    """Check if file exists on remote server."""
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        # Connect to remote server
        ssh.connect(
            hostname=SFTP_CONFIG["hostname"],
            username=SFTP_CONFIG["username"],
            password=SFTP_CONFIG["password"]
        )
        
        # Create SFTP client
        sftp = ssh.open_sftp()
        
        try:
            sftp.stat(remote_path)
            return True
        except FileNotFoundError:
            return False
            
    except Exception as e:
        logger.error(f"Error checking file on remote server: {str(e)}")
        return False
    finally:
        ssh.close() 