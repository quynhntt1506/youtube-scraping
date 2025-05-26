import requests
import json
import os
from pathlib import Path
from typing import Dict, Any, List
from src.utils.logger import CustomLogger

logger = CustomLogger("send_to_crawler")

def send_channel_to_data_controller(detailed_channels: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Send crawled channel data to crawler API.
    
    Args:
        detailed_channels (List[Dict[str, Any]]): List of channel data containing paths
        
    Returns:
        Dict[str, Any]: Result containing success and failed channels
    """
    results = {
        "success": [],
        "failed": []
    }
    
    for channel in detailed_channels:
        try:
            channel_id = channel["channelId"]
            json_path = channel["jsonPath"]
            
            if not json_path:
                logger.error(f"Missing required json path for channel {channel_id}")
                results["failed"].append(channel_id)
                continue
                
            # API endpoint
            url = "http://localhost:8080/api/upload/multiple"
            
            # Prepare files for form-data
            files = [
                ('files', (f'{channel_id}.json', open(json_path, 'rb'), 'application/json'))
            ]
            
            # Add avatar if exists
            if channel.get("avatarPath"):
                files.append(('files', (f'{channel_id}_avatar.jpg', open(channel["avatarPath"], 'rb'), 'image/jpeg')))
            
            # Add banner if exists
            if channel.get("bannerPath"):
                files.append(('files', (f'{channel_id}_banner.jpg', open(channel["bannerPath"], 'rb'), 'image/jpeg')))
            
            # Prepare data
            data = {
                'data': 'YOUTUBE_CHANNEL_INFO'
            }
            
            # Send request
            response = requests.post(url, files=files, data=data)
            
            # Close file handles
            for _, file_tuple in files:
                file_tuple[1].close()
            
            # Check response
            if response.status_code == 200:
                logger.info(f"Successfully sent data for channel {channel_id}")
                results["success"].append(channel_id)
            else:
                logger.error(f"Failed to send data for channel {channel_id}. Status code: {response.status_code}")
                logger.error(f"Response: {response.text}")
                results["failed"].append(channel_id)
                
        except Exception as e:
            logger.error(f"Error sending data for channel {channel_id}: {str(e)}")
            results["failed"].append(channel_id)
            
    return results

def send_video_to_data_controller(detailed_videos: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Send crawled video data to crawler API.
    
    Args:
        detailed_videos (List[Dict[str, Any]]): List of video data containing paths
        
    Returns:
        Dict[str, Any]: Result containing success and failed videos
    """
    results = {
        "success": [],
        "failed": []
    }
    
    for video in detailed_videos:
        try:
            video_id = video["videoId"]
            thumbnail_path = video["thumbnailPath"]
            json_path = video["jsonPath"]
            
            if not all([thumbnail_path, json_path]):
                logger.error(f"Missing required paths for video {video_id}")
                results["failed"].append(video_id)
                continue
                
            # API endpoint
            url = "http://localhost:8080/api/upload/multiple"
            
            # Prepare files for form-data
            files = [
                ('files', (f'{video_id}.json', open(json_path, 'rb'), 'application/json')),
                ('files', (f'{video_id}_thumbnail.jpg', open(thumbnail_path, 'rb'), 'image/jpeg'))
            ]
            
            # Prepare data
            data = {
                'data': 'YOUTUBE_VIDEO_INFO'
            }
            
            # Send request
            response = requests.post(url, files=files, data=data)
            
            # Close file handles
            for _, file_tuple in files:
                file_tuple[1].close()
            
            # Check response
            if response.status_code == 200:
                logger.info(f"Successfully sent data for video {video_id}")
                results["success"].append(video_id)
            else:
                logger.error(f"Failed to send data for video {video_id}. Status code: {response.status_code}")
                logger.error(f"Response: {response.text}")
                results["failed"].append(video_id)
                
        except Exception as e:
            logger.error(f"Error sending data for video {video_id}: {str(e)}")
            results["failed"].append(video_id)
            
    return results

# def send_batch_to_crawler(channels_data: List[Dict[str, Any]], image_paths: List[Dict[str, str]]) -> Dict[str, Any]:
#     """Send a batch of crawled channels to crawler API.
    
#     Args:
#         channels_data (List[Dict[str, Any]]): List of channel data from crawl result
#         image_paths (List[Dict[str, str]]): List of dicts containing avatar and banner paths
        
#     Returns:
#         Dict[str, Any]: Result containing success and failed channels
#     """
#     results = {
#         "success": [],
#         "failed": []
#     }
    
#     for channel_data, image_path in zip(channels_data, image_paths):
#         success = send_to_crawler(
#             channel_data,
#             image_path["avatar"],
#             image_path["banner"]
#         )
        
#         if success:
#             results["success"].append(channel_data["channelId"])
#         else:
#             results["failed"].append(channel_data["channelId"])
            
#     return results 