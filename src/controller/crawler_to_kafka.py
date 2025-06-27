from typing import Dict, Any, List
from pathlib import Path
from datetime import datetime
import json
import os
import paramiko
from io import StringIO
import tempfile
from kafka import KafkaProducer
from src.config.config import KAFKA_BOOTSTRAP_SERVERS
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
logger = CustomLogger("crawler_to_send_kafka")

class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime objects."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

def send_to_kafka(topic: str, message: dict, bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS):
    """
    Gửi message (dạng dict/json) vào Kafka topic.
    Args:
        topic (str): Tên topic Kafka.
        message (dict): Dữ liệu gửi đi (dạng dict/json).
        bootstrap_servers (str): Địa chỉ Kafka broker.
    """
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    json_message = json.dumps(message).encode('utf-8')
    producer.send(topic, json_message)
    producer.flush()
    logger.info("Sent data crawled to kafka")
    producer.close()

def update_quota_usage(quota_usage: Dict[str, int]) -> None:
    api_manager = APIKeyManager()
    total_quota = 0
    for api_key, quota in quota_usage.items():
        api_manager.update_quota(api_key, quota)
        logger.info(f"Updated quota for API key {api_key}: {quota} units")
        total_quota += quota
    return total_quota

def crawl_channels_by_keyword(keyword: str, max_results: int = MAX_CHANNELS) -> Dict[str, Any]:
    """Crawl channels by keyword."""
    api = YouTubeAPI()
    
    try:
        # Search channels
        search_result = api.search_channel_by_keyword(keyword, max_results=max_results)
        logger.info(f"Search keyword '{keyword}'")
        logger.info(f"Found {len(search_result['channels'])} channels")
        search_quota = update_quota_usage(search_result["quota_usage"])
        # Get detailed channel information
        logger.info(f"Total quota used to search: {search_quota}")
        channel_ids = [c["channelId"] for c in search_result["channels"]] 
        detailed_channels = []
        for i in range(0, len(channel_ids), MAX_ENTITY_IN_BATCH):
            batch_channel_ids = channel_ids[i:i+MAX_ENTITY_IN_BATCH]
            batch_channel_result = crawl_channel_by_id(batch_channel_ids)
            batch_detailed_channels = batch_channel_result["detailed_channels"]
            detailed_channels.extend(batch_detailed_channels)
            # Update quota usage to db 
    
        return {
            "detailed_channels": detailed_channels,
        }
        
    finally:
        logger.info(f"Done crawl channel by keyword")

def crawl_channel_by_id(channel_ids: List[str]) -> Dict[str, Any]:
    """Crawl channel by id."""
    api = YouTubeAPI()

    try:
        logger.info(f"Crawling channel {channel_ids}")
        channel_result = api.get_channel_detail_by_ids(channel_ids)
        channel_quota = update_quota_usage(channel_result["quota_usage"])
        logger.info(f"Total quota used to get channel detail by id: {channel_quota}")
        all_detaled_channels = [];
        # Save each channel's data to a separate JSON file
        if (channel_result["detailed_channels"]):
            for channel in channel_result["detailed_channels"]:
                if channel:
                    # print(channel);
                    send_to_kafka("youtube.channel.crawler.raw", channel)
                    # crawl_videos_in_playlist(channel["playlistId"])
                    all_detaled_channels.append(channel)
                    logger.info(f"Crawled and send channel {channel['channelId']} successfully")
        else:
            logger.info(f"Error processing crawl channel")
        # result_db = db.insert_many_channels(all_detaled_channels)
        return {
            "detailed_channels": all_detaled_channels,
        }
    finally:
        logger.info(f"Done crawl channel by id")

def crawl_channel_by_custom_urls(custom_urls: List[str]) -> Dict[str, Any]:
    """Crawl channel by custom urls."""
    api = YouTubeAPI()
    # db = Database()

    try:
        logger.info(f"Crawling channel {custom_urls}")
        channel_result = api.get_channel_detail_by_custom_urls(custom_urls)
        channel_quota = update_quota_usage(channel_result["quota_usage"])
        logger.info(f"Total quota used to get channel detail by custom url: {channel_quota}")
        all_detaled_channels = [];

        # Save each channel's data to a separate JSON file
        if (channel_result["detailed_channels"]):
            for channel in channel_result["detailed_channels"]:
                if channel:
                    send_to_kafka("youtube.channel.crawler.raw", channel)
                    # crawl_videos_in_playlist(channel["playlistId"])
                    all_detaled_channels.append(channel)
                    logger.info(f"Crawled and send channel {channel['channelId']} successfully")
        else:
            logger.info(f"Error processing crawl channel")
        # result_db = db.insert_many_channels(all_detaled_channels)
        return {
            "new_channels": all_detaled_channels,
        }
    finally:
        # db.close()
        logger.info(f"Done crawl channel by custom url")

def crawl_video_by_ids(video_ids: List[str]) -> Dict[str, Any]:
    """Crawl video by id."""
    api = YouTubeAPI()
    # db = Database()

    try:
        video_result = api.get_video_details(video_ids)
        video_quota = update_quota_usage(video_result["quota_usage"])
        logger.info(f"Total quota used to get video by id: {video_quota}")
        all_detailed_videos = [];

        # Save each video's data to a separate JSON file
        if (video_result["detailed_videos"]):
            for video in video_result["detailed_videos"]:
                video_id = video["videoId"]
                # crawl comments
                comment_result = api.get_video_comments(video_id)
                if (comment_result): 
                    video["comments"] = comment_result["comments"]
                else:
                    video["comments"] = []
                # print(video)
                all_detailed_videos.append(video)
                if (video):
                    send_to_kafka("youtube.video.crawler.raw", video)
                    logger.info(f"Crawled and send video {video['videoId']} successfully")
        # result_db = db.insert_many_videos(all_detailed_videos)
        return {
            "new_videos": all_detailed_videos,
        }        
    finally:
        # db.close()
        logger.info(f"Done crawl video by id")

def crawl_videos_in_playlist(playlist_id: str) -> Dict[str, Any]:
    """Crawl videos from a playlist."""
    api = YouTubeAPI()

    playlist_result = api.get_all_videos_from_playlist(playlist_id)
    playlist_quota = update_quota_usage(playlist_result["quota_usage"])
    logger.info(f"Total quota used to get video in playlist: {playlist_quota}")
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

def crawl_info_from_keyword(keyword:str) -> Dict[str, Any]:
    try: 
        channel_result = crawl_channels_by_keyword(keyword);
        if (channel_result["detailed_channels"]):
            playlist_ids = [c["playlistId"] for c in [{"playlistId": "UUQBTfWwmrardq2rbCLssinw"}] if c.get("playlistId")]
            if (len(playlist_ids) != 0):
                for playlist_id in playlist_ids:
                    video_result = crawl_videos_in_playlist(playlist_id)
                    logger.info(f"Crawled {len(video_result['videos'])} from playlist id {playlist_id}")
                
                return {
                    "success": True,
                    "channels_count": len(channel_result["detailed_channels"]),
                    "playlists_count": len(playlist_ids)
                }
            else:
                logger.info("No playlist IDs found in channel results")
                return {
                    "success": False,
                    "error": "No playlist IDs found"
                }
        else:
            logger.info("Error processing crawl channel by keyword")
            return {
                "success": False,
                "error": "No channels found"
            }
    except Exception as e:
        logger.error(f"Error in crawl_info_from_keyword: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }
    finally:
        logger.info("Done crawl info from keyword")