from typing import Dict, Any, List
from pathlib import Path
from datetime import datetime
import json
import os
import paramiko
from io import StringIO
import tempfile

from src.utils.logger import CustomLogger
from src.utils.api import YouTubeAPI
from src.database.database import Database
from src.database.api_key_manager import APIKeyManager
from src.controller.image_downloader import download_channel_images
from src.controller.thumbnail_downloader import download_video_thumbnails
from src.utils.common import parse_youtube_channel_url
from src.config.config import MAX_CHANNELS, MAX_ENTITY_IN_BATCH, MIN_ENTITY_IN_BATCH, SFTP_CONFIG
from src.controller.send_to_data_controller import *

# Initialize logger
logger = CustomLogger("crawler_to_send_rabbitmq")

class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime objects."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

def get_path_file_json(type: str, name: str) -> Path:
    # Create data/json/yyyy-mm-dd directory on remote machine
    today = datetime.now().strftime("%Y-%m-%d")
    remote_path = f"/home/htsc/crawl-youtube/data-request/json/{today}/{type}"
    remote_file = f"{remote_path}/{name}.json"
    
    # Create SFTP client
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        # Connect to remote server
        ssh.connect(
            hostname=SFTP_CONFIG["hostname"],
            username=SFTP_CONFIG["username"],  # Replace with actual username
            password=SFTP_CONFIG["password"]   # Replace with actual password
        )
        
        # Create SFTP client
        sftp = ssh.open_sftp()
        
        # Create directory if not exists
        try:
            sftp.stat(remote_path)
        except FileNotFoundError:
            # Create parent directories recursively
            current_path = ""
            for part in remote_path.split("/"):
                if part:
                    current_path += "/" + part
                    try:
                        sftp.stat(current_path)
                    except FileNotFoundError:
                        sftp.mkdir(current_path)
            
        return remote_file
        
    except Exception as e:
        logger.error(f"Error connecting to remote server: {str(e)}")
        raise
    finally:
        ssh.close()

def save_json_to_remote(json_data: dict, remote_path: str):
    """Save JSON data to remote machine using SFTP."""
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
        
        # Convert dict to JSON string
        json_str = json.dumps(json_data, ensure_ascii=False, indent=2, cls=DateTimeEncoder)
        
        # Create a temporary file
        with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8') as temp_file:
            temp_file.write(json_str)
            temp_file.flush()
            temp_path = temp_file.name
            
            try:
                # Upload using put instead of putfo
                sftp.put(temp_path, remote_path)
                
                # Verify file size
                local_size = os.path.getsize(temp_path)
                remote_size = sftp.stat(remote_path).st_size
                
                if local_size != remote_size:
                    raise Exception(f"Size mismatch: local={local_size}, remote={remote_size}")
                    
            finally:
                # Clean up temp file
                try:
                    os.unlink(temp_path)
                except:
                    pass
                
    except Exception as e:
        logger.error(f"Error saving JSON to remote server: {str(e)}")
        raise
    finally:
        ssh.close()

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
            remote_path = get_path_file_json("channel", channel_id)
            channel["jsonPath"] = str(remote_path)
            save_json_to_remote(channel, remote_path)
            logger.info(f"Saved channel data to {remote_path}")
            if channel:
                send_channel_to_data_controller([channel])
                crawl_videos_in_playlist(channel["playlistId"])
            all_detaled_channels.append(channel)

        # result_db = db.insert_many_channels(all_detaled_channels)
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
            remote_path = get_path_file_json("channel", channel_id)
            channel["jsonPath"] = str(remote_path)
            channel["photoInfos"] = {
                f"{channel_id.lower()}_avatar.jpg": channel.get("avatarUrl", ""),
                f"{channel_id.lower()}_banner.jpg": channel.get("bannerUrl", ""),
            }
            save_json_to_remote(channel, remote_path)
            logger.info(f"Saved channel data to {remote_path}")
            if channel:
                send_channel_to_data_controller([channel])
                crawl_videos_in_playlist(channel["playlistId"])
            all_detaled_channels.append(channel)

        # result_db = db.insert_many_channels(all_detaled_channels)
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

        # Save each channel's data to a separate JSON file
        for channel in image_result["updated_channels"]:
            channel_id = channel["channelId"]
            remote_path = get_path_file_json("channel", channel_id)
            channel["jsonPath"] = str(remote_path)
            save_json_to_remote(channel, remote_path)
            logger.info(f"Saved channel data to {remote_path}")
            all_detailed_channels.append(channel)

        # result_db = db.insert_many_channels(all_detailed_channels)
        return {
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

        # Save each video's data to a separate JSON file
        for video in image_result["updated_videos"]:
            video_id = video["videoId"]
            # crawl comments
            comment_result = api.get_video_comments(video_id)
            if (comment_result): 
                video["comments"] = comment_result["comments"]
            else:
                video["comments"] = []
                
            # Add photoInfos
            video["photoInfos"] = {
                f"{video_id.lower()}_thumbnail.jpg": video.get("thumbnailUrl", "")
            }
            print(video["photoInfos"])
            remote_path = get_path_file_json("video", video_id)
            video["jsonPath"] = str(remote_path)
            save_json_to_remote(video, remote_path)
            logger.info(f"Saved video data to {remote_path}")
            all_detailed_videos.append(video)
            if (video):
                send_video_to_data_controller([video])

        # result_db = db.insert_many_videos(all_detailed_videos)
        return {
            "new_videos": all_detailed_videos,
        }
    finally:
        db.close()

def crawl_video_by_urls(video_urls: List[str]) -> Dict[str, Any]:
    """Crawl video by url."""
    api = YouTubeAPI()
    db = Database()

    for video_url in video_urls:
        extract_url = api.extract_youtube_id(video_url)
        video_ids = []
        playlist_ids = []
        
        if extract_url["type"] in ["video", "shorts"]:
            video_ids.append(extract_url["id"])
        elif extract_url["type"] == "playlist":
            playlist_ids.append(extract_url["id"])
        else:
            logger.error(f"Invalid video url: {video_url}")
        
        if video_ids:
            result_crawl = crawl_video_by_ids(video_ids)
            logger.info(f'Crawled {len(result_crawl["new_videos"])} videos from url_ids')
        if playlist_ids:
            for playlist_id in playlist_ids:
                result_crawl = crawl_videos_in_playlist(playlist_id)
                logger.info(f'Crawled {len(result_crawl["videos"])} videos from playlist_ids')


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

