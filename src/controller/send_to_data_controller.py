import requests
import json
import os
from pathlib import Path
from typing import Dict, Any, List
from src.utils.logger import CustomLogger
import paramiko
from src.config.config import SFTP_CONFIG
from io import BytesIO
from src.controller.image_downloader import read_file_from_sftp, check_file_exists_sftp

logger = CustomLogger("send_to_crawler")

def delete_remote_files(file_paths: List[str]) -> None:
    """Delete files from remote machine using SFTP.
    
    Args:
        file_paths (List[str]): List of file paths to delete
    """
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
        
        # Delete each file
        for file_path in file_paths:
            try:
                sftp.remove(file_path)
                logger.info(f"Successfully deleted file: {file_path}")
            except Exception as e:
                logger.error(f"Error deleting file {file_path}: {str(e)}")
                
    except Exception as e:
        logger.error(f"Error connecting to remote server: {str(e)}")
    finally:
        ssh.close()

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
                
            # Read JSON file
            try:
                json_content = read_file_from_sftp(json_path)
                files = [
                    ('files', (f'{channel_id}.json', BytesIO(json_content), 'application/json'))
                ]
                
                # Add avatar if exists
                if channel.get("avatarPath"):
                    try:
                        avatar_content = read_file_from_sftp(channel["avatarPath"])
                        files.append(('files', (f'{channel_id.lower()}_avatar.jpg', BytesIO(avatar_content), 'image/jpeg')))
                    except Exception as e:
                        logger.error(f"Error reading avatar file: {str(e)}")
                
                # Add banner if exists
                if channel.get("bannerPath"):
                    try:
                        banner_content = read_file_from_sftp(channel["bannerPath"])
                        files.append(('files', (f'{channel_id.lower()}_banner.jpg', BytesIO(banner_content), 'image/jpeg')))
                    except Exception as e:
                        logger.error(f"Error reading banner file: {str(e)}")
            except Exception as e:
                logger.error(f"Error reading JSON file: {str(e)}")
                continue
            
            # API endpoint
            url = "http://192.168.132.250:8080/api/upload/multiple"
            
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
                
                # Delete files after successful upload
                files_to_delete = [json_path]
                if channel.get("avatarPath"):
                    files_to_delete.append(channel["avatarPath"])
                if channel.get("bannerPath"):
                    files_to_delete.append(channel["bannerPath"])
                delete_remote_files(files_to_delete)
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
            url = "http://192.168.132.250:8080/api/upload/multiple"
            
            # Read files using SFTP
            try:
                # Read JSON file
                json_content = read_file_from_sftp(json_path)
                json_file = BytesIO(json_content)
                
                # Read thumbnail file
                thumbnail_content = read_file_from_sftp(thumbnail_path)
                thumbnail_file = BytesIO(thumbnail_content)
                
                # Prepare files for form-data
                files = [
                    ('files', (f'{video_id.lower()}_data.json', json_file, 'application/json')),
                    ('files', (f'{video_id.lower()}_thumbnail.jpg', thumbnail_file, 'image/jpeg'))
                ]
                
                # Prepare data
                data = {
                    'data': 'YOUTUBE_VIDEO_INFO'
                }
                
                # Send request
                response = requests.post(url, files=files, data=data)
                
                # Close file handles
                json_file.close()
                thumbnail_file.close()
                
                # Check response
                if response.status_code == 200:
                    logger.info(f"Successfully sent data for video {video_id}")
                    results["success"].append(video_id)
                    
                    # Delete files after successful upload
                    files_to_delete = [json_path, thumbnail_path]
                    delete_remote_files(files_to_delete)
                else:
                    logger.error(f"Failed to send data for video {video_id}. Status code: {response.status_code}")
                    logger.error(f"Response: {response.text}")
                    results["failed"].append(video_id)
                    
            except Exception as e:
                logger.error(f"Error reading files for video {video_id}: {str(e)}")
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