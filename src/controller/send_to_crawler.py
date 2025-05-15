import requests
import json
import os
from pathlib import Path
from typing import Dict, Any, List
from src.utils.logger import CustomLogger

logger = CustomLogger("send_to_crawler")

def send_to_crawler(detailed_channels: List[Dict[str, Any]]) -> Dict[str, Any]:
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
            avatar_path = channel["avatarPath"]
            banner_path = channel["bannerPath"]
            json_path = channel["jsonPath"]
            
            if not all([avatar_path, banner_path, json_path]):
                logger.error(f"Missing required paths for channel {channel_id}")
                results["failed"].append(channel_id)
                continue
                
            # API endpoint
            url = "http://localhost:8080/api/upload/multiple"
            
            # Prepare files for form-data
            files = [
                ('files', ('channel_data.json', open(json_path, 'rb'), 'application/json')),
                ('files', ('avatar.jpg', open(avatar_path, 'rb'), 'image/jpeg')),
                ('files', ('banner.jpg', open(banner_path, 'rb'), 'image/jpeg'))
            ]
            
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