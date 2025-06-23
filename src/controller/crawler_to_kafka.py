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


def crawl_channel_by_id(channel_ids: List[str]) -> Dict[str, Any]:
    """Crawl channel by id."""
    api = YouTubeAPI()

    try:
        logger.info(f"Crawling channel {channel_ids}")
        channel_result = api.get_channel_detail_by_ids(channel_ids)
        all_detaled_channels = [];
        # Save each channel's data to a separate JSON file
        if (channel_result["detailed_channels"]):
            for channel in channel_result["detailed_channels"]:
                if channel:
                    print(channel);
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
        logger.info(f"Done crawl channel by id")

def crawl_channel_by_custom_urls(custom_urls: List[str]) -> Dict[str, Any]:
    """Crawl channel by custom urls."""
    api = YouTubeAPI()
    # db = Database()

    try:
        logger.info(f"Crawling channel {custom_urls}")
        channel_result = api.get_channel_detail_by_custom_urls(custom_urls)

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
        # image_result = download_video_thumbnails(video_result["detailed_videos"])

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
                print(video)
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

def crawl_video_by_urls(video_urls: List[str]) -> Dict[str, Any]:
    """Crawl video by url."""
    api = YouTubeAPI()
    # db = Database()

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
            logger.info(f"Crawled {len(result_crawl['new_videos'])} videos from url_ids")
        if playlist_ids:
            for playlist_id in playlist_ids:
                result_crawl = crawl_videos_in_playlist(playlist_id)
                logger.info(f"Crawled {len(result_crawl['videos'])} videos from playlist_ids")


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

