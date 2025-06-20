from typing import Dict, Any, List
from pathlib import Path
from datetime import datetime

from utils.logger import CustomLogger
from utils.api import YouTubeAPI
from src.database.database import Database
from src.database.api_key_manager import APIKeyManager
from src.controller.image_downloader import download_channel_images_auto
from src.controller.thumbnail_downloader import download_video_thumbnails_auto
from config.config import MAX_CHANNELS, MAX_ENTITY_IN_BATCH, MIN_ENTITY_IN_BATCH

# Initialize logger
logger = CustomLogger("crawler_auto")

def update_quota_usage(quota_usage: Dict[str, int]) -> None:
    db = Database()
    """Update quota usage for each API key."""
    api_manager = APIKeyManager(db)
    total_quota = 0
    for api_key, quota in quota_usage.items():
        api_manager.update_quota(api_key, quota)
        logger.info(f"Updated quota for API key {api_key}: {quota} units")
        total_quota += quota
    return total_quota

def crawl_channels_by_keyword(keyword: str, max_results: int = MAX_CHANNELS) -> Dict[str, Any]:
    """Crawl channels by keyword."""
    api = YouTubeAPI()
    db = Database()
    
    try:
        # Search channels
        search_result = api.search_channel_by_keyword(keyword, max_results=max_results)
        logger.info(f"Search keyword '{keyword}'")
        logger.info(f"Found {len(search_result['channels'])} channels")
        # Update quota usage to db 
        search_quota = update_quota_usage(search_result["quota_usage"])
        
        # Filter new channels
        new_channels = []
        for channel in search_result["channels"]:
            channel_id = channel.get("channelId")
            if channel_id and not db.channel_exists(channel_id):
                new_channels.append(channel)
        
        # Insert new channels to db
        db.insert_many_channels(new_channels)
        new_channel_ids = []
        detailed_channels = []
        total_channel_quota = 0
        # Get detailed channel information
        channel_ids = [c["channelId"] for c in new_channels] 
        for i in range(0, len(channel_ids), MAX_ENTITY_IN_BATCH):
            batch_channel_ids = channel_ids[i:i+MAX_ENTITY_IN_BATCH]
            batch_channel_result = api.get_channel_detail_by_ids(batch_channel_ids)
            batch_detailed_channels = batch_channel_result["detailed_channels"]
            detailed_channels.extend(batch_detailed_channels)
            
            # Update quota usage to db 
            channel_quota = update_quota_usage(batch_channel_result["quota_usage"])            
            total_channel_quota += channel_quota

            # # Download channel images
            # batch_image_result = download_channel_images_auto(batch_detailed_channels)
            # logger.info(f"Downloaded {batch_image_result['avatars']} avatars and {batch_image_result['banners']} banners")
            
            # Save to database
            batch_db_channel_result = db.insert_many_channels(batch_detailed_channels)
            new_channel_ids.extend(batch_db_channel_result["new_channel_ids"])
            logger.info(f"Inserted {batch_db_channel_result.get('new_channels_count')} new channels")
            logger.info(f"Updated {batch_db_channel_result.get('updated_channels_count')} existing channels")
            
        logger.info(f"Total quota used: {search_quota + total_channel_quota}")
        
        return {
            "new_channels": new_channel_ids,
            "detailed_channels": detailed_channels,
            "quota_used": search_quota + total_channel_quota
        }
        
    finally:
        db.close()

def crawl_videos_from_channels(channels: List[dict]) -> Dict[str, Any]:
    """Crawl videos from channel playlists."""
    api = YouTubeAPI()
    db = Database()
    
    try:
        # Get playlist IDs
        playlist_ids = [c["playlistId"] for c in channels if c.get("playlistId")]
        videos = []
        quota_usage = 0  # Initialize quota_usage
        
        # Get videos from playlists
        for i in range(0, len(playlist_ids), MIN_ENTITY_IN_BATCH):
            batch_playlist_ids = playlist_ids[i:i+MIN_ENTITY_IN_BATCH]
            playlist_result = api.get_all_videos_from_playlists(batch_playlist_ids)
            batch_videos = playlist_result["videos"]
            videos.extend(batch_videos)
            logger.info(f"Found {len(batch_videos)} videos from playlists")
            db.insert_many_videos(batch_videos)
            # Update channel status to crawled_video
            result_update_status_channel_db = db.update_channels_status_by_playlist_ids(playlist_result["crawled_playlist_ids"])
            logger.info(f"Updated {result_update_status_channel_db['updated_count']} channels to 'crawled_video'")
            
            # Update quota usage to db 
            playlist_quota = update_quota_usage(playlist_result["quota_usage"])
            quota_usage += playlist_quota
        
        # Get video details and process in batches of 50
        all_videos = []
        all_video_ids = []
        
        for i in range(0, len(videos), MAX_ENTITY_IN_BATCH):
            batch_videos = videos[i:i+MAX_ENTITY_IN_BATCH]
            logger.info(f"Processing batch of {len(batch_videos)} videos")
            
            # Get video details for this batch
            video_ids = [video["videoId"] for video in batch_videos]
            video_details = api.get_video_details(video_ids)
            logger.info(f"Got details for {len(video_details['detailed_videos'])} videos in current batch")
            
            # Update quota usage
            batch_quota = update_quota_usage(video_details["quota_usage"])
            quota_usage += batch_quota
            
            # # Download thumbnails for this batch
            # thumbnail_result = download_video_thumbnails_auto(video_details["detailed_videos"])
            # logger.info(f"Downloaded {thumbnail_result['count']} thumbnails in current batch")
            
            # Save to database
            if video_details["detailed_videos"]:
                db_result = db.insert_many_videos(video_details["detailed_videos"])
                logger.info(f"Inserted {db_result.get('new_videos_count')} new videos")
                logger.info(f"Updated {db_result.get('updated_videos_count')} existing videos")
                all_videos.extend(video_details["detailed_videos"])
                all_video_ids.extend(db_result.get("new_video_ids", []))
            
            logger.info(f"Completed processing batch of {len(batch_videos)} videos")
        
        logger.info(f"Total quota used: {quota_usage}")
        
        return {
            "new_videos": all_video_ids,
            "videos": all_videos,
            "quota_used": quota_usage
        }
        
    finally:
        db.close()

def crawl_comments_from_videos(video_ids: List[str]) -> Dict[str, Any]:
    """Crawl comments from videos in batches of 10."""
    api = YouTubeAPI()
    db = Database()
    
    try:
        all_comments = []
        all_comment_ids = []
        crawled_video_ids = []
        quota_usage = {}
        db_result = {"new_comment_ids": []}  # Initialize db_result
        
        # Process videos in batches of 50
        for i in range(0, len(video_ids), MIN_ENTITY_IN_BATCH):
            batch_videos = video_ids[i:i+MIN_ENTITY_IN_BATCH]
            logger.info(f"Processing batch of videos: {batch_videos}")
            
            batch_comments = []
            batch_crawled_videos = []
            
            for video_id in batch_videos:
                try:
                    comment_result = api.get_video_comments(video_id)
                    if comment_result:
                        batch_crawled_videos.append(video_id)
                        comments = comment_result["comments"]
                        batch_comments.extend(comments)
                        logger.info(f"Crawled {len(comments)} comments for video {video_id}")
                        
                        # Update quota usage
                        if "quota_usage" in comment_result:
                            update_quota_usage(comment_result["quota_usage"])
                except Exception as e:
                    logger.error(f"Error crawling comments for video {video_id}: {str(e)}")
                    continue
            
            # Insert comments and update video status for this batch
            if batch_comments:
                # Insert comments
                db_result = db.insert_many_comments(batch_comments)
                all_comment_ids.extend(db_result.get("new_comment_ids", []))
                logger.info(f"Inserted {db_result.get('new_comments_count')} new comments")
                logger.info(f"Updated {db_result.get('updated_comments_count')} existing comments")
                all_comments.extend(batch_comments)
                
                # Update video status for this batch
            if batch_crawled_videos:
                result_update_status = db.update_videos_status_by_video_ids(batch_crawled_videos)
                logger.info(f"Updated {result_update_status['updated_count']} videos to 'crawled_comments' in current batch")
                crawled_video_ids.extend(batch_crawled_videos)
            
            logger.info(f"Completed batch of videos: {batch_videos}")
        
        return {
            "new_comments": db_result.get("new_comment_ids", []),
            "comments": all_comments,
        }
        
    finally:
        db.close()

def crawl_video_in_channel_by_keyword(keyword: str, max_results: int = MAX_CHANNELS) -> Dict[str, Any]:
    """Process a single keyword."""
    api = YouTubeAPI()
    db = Database()
    api_manager = APIKeyManager(db)
    
    try:
        # Crawl channels
        channel_result = crawl_channels_by_keyword(keyword, max_results)
        new_channels_ids = channel_result["new_channels"]
        detailed_channels = channel_result["detailed_channels"]

        # Crawl videos
        # video_result = crawl_videos_from_channels(detailed_channels)
        # new_videos_ids = video_result["new_videos"]
        # videos = video_result["videos"]
        
        # Crawl comments
        # video_ids = [v["videoId"] for v in videos]
        # comment_result = crawl_comments_from_videos(video_ids)
        
        return {
            # "new_videos": new_videos_ids,
            "new_channels": new_channels_ids,
            # "new_comments": comment_result["new_comments"],
            "count_channels": len(detailed_channels),
            # "count_videos": len(videos),
            # "count_comments": len(comment_result["comments"]),
        }
        
    finally:
        db.close()

def crawl_video_in_channel_by_many_keywords(keywords: list[str]):
        
    # Process keywords in batches of 5
    batch_size = 5
    for i in range(0, len(keywords), batch_size):
        batch_keywords = keywords[i:i+batch_size]
        logger.info(f"Processing batch of {len(batch_keywords)} keywords...")
        
        # Collect results for batch processing
        keywords_data = []
        
        for keyword in batch_keywords:
            logger.info(f"Processing keyword: {keyword}")
            # Check if keyword is already crawled
            db = Database()
            keyword_doc = db.get_keyword_by_keyword(keyword)
            if keyword_doc and keyword_doc.get("status") == "crawled":
                logger.info(f"Keyword {keyword} is already crawled, skipping...")
                continue
            elif keyword_doc and keyword_doc.get("status") == "to_crawl":
                # Update status to crawling
                db.update_keyword_status(keyword, "crawling")
                logger.info(f"Updated status of keyword {keyword} to 'crawling'")
                
                result = crawl_video_in_channel_by_keyword(keyword)
                if result:
                    logger.info(f"Crawled {result.get('count_channels')} channels for keyword {keyword}")
                    db.update_keyword_status(keyword, "crawled")
                    logger.info(f"Updated status of keyword {keyword} to 'crawled'")
                else:
                    logger.warning(f"Keyword {keyword} not found in database or has invalid status")

def crawl_videos_from_crawled_channels(batch_size: int = MIN_ENTITY_IN_BATCH) -> Dict[str, Any]:
    """Crawl videos from all channels with status crawled_channel.
    
    Args:
        batch_size (int): Number of channels to process in each batch
        
    Returns:
        Dict[str, Any]: Result containing:
            - total_channels: Total number of channels processed
            - total_videos: Total number of videos crawled
            - total_comments: Total number of comments crawled
            - errors: List of errors encountered
    """
    db = Database()
    
    try:
        total_channels_processed = 0
        total_videos = 0
        total_comments = 0
        errors = []
        skip = 0
        
        while True:
            # Find channels with status crawled_channel
            channels = db.collections["channels"].find(
                {"status": "crawled_channel"},
                {"channelId": 1, "playlistId": 1}
            ).skip(skip).limit(batch_size)
            
            channels = list(channels)
            if not channels:
                logger.info("No more channels with status crawled_channel found")
                break
            
            logger.info(f"Processing batch of {len(channels)} channels (skip: {skip})")
            
            # Crawl videos from this batch of channels
            video_result = crawl_videos_from_channels(channels)
            if video_result:
                batch_videos = len(video_result.get("videos", []))
                total_videos += batch_videos
                logger.info(f"Crawled {batch_videos} videos from current batch")
                
                # # Crawl comments from videos in this batch
                # video_ids = [v["videoId"] for v in video_result.get("videos", [])]
                # if video_ids:
                #     comment_result = crawl_comments_from_videos(video_ids)
                #     if comment_result:
                #         batch_comments = len(comment_result.get("comments", []))
                #         total_comments += batch_comments
                #         logger.info(f"Crawled {batch_comments} comments for current batch")
            
            total_channels_processed += len(channels)
            skip += len(channels)  # Update skip for next batch
            logger.info(f"Processed {total_channels_processed} channels so far")
        
        logger.info(f"Finished crawling videos. Total channels processed: {total_channels_processed}, Total videos: {total_videos}")
        return {
            "total_channels": total_channels_processed,
            "total_videos": total_videos,
            "total_comments": total_comments,
            "errors": errors
        }
        
    except Exception as e:
        error_msg = f"Error in crawl_videos_from_crawled_channels: {str(e)}"
        logger.error(error_msg)
        return {
            "total_channels": total_channels_processed,
            "total_videos": total_videos,
            "total_comments": total_comments,
            "errors": [error_msg]
        }
    finally:
        db.close()

def crawl_comments_from_crawled_videos(batch_size: int = MAX_ENTITY_IN_BATCH) -> Dict[str, Any]:
    """Crawl comments from all videos with status crawled_video.
    
    Args:
        batch_size (int): Number of videos to process in each batch
        
    Returns:
        Dict[str, Any]: Result containing:
            - total_videos: Total number of videos processed
            - total_comments: Total number of comments crawled
            - errors: List of errors encountered
    """
    db = Database()
    
    try:
        total_videos_processed = 0
        total_comments = 0
        errors = []
        skip = 0
        
        while True:
            # Find videos with status crawled_video using skip for pagination
            videos = db.collections["videos"].find(
                {"status": "crawled_video"},
                {"videoId": 1}
            ).skip(skip).limit(batch_size)
            
            videos = list(videos)
            if not videos:
                logger.info("No more videos with status crawled_video found")
                break
            
            logger.info(f"Processing batch of {len(videos)} videos (skip: {skip})")
            
            # Get video IDs for this batch
            video_ids = [v["videoId"] for v in videos]
            
            # Crawl comments for this batch
            comment_result = crawl_comments_from_videos(video_ids)
            if comment_result:
                batch_comments = len(comment_result.get("comments", []))
                total_comments += batch_comments
                logger.info(f"Crawled {batch_comments} comments for current batch")
            
            total_videos_processed += len(videos)
            skip += len(videos)  # Update skip for next batch
            logger.info(f"Processed {total_videos_processed} videos so far")
        
        logger.info(f"Finished crawling comments. Total videos processed: {total_videos_processed}, Total comments: {total_comments}")
        return {
            "total_videos": total_videos_processed,
            "total_comments": total_comments,
            "errors": errors
        }
        
    except Exception as e:
        error_msg = f"Error in crawl_comments_from_crawled_videos: {str(e)}"
        logger.error(error_msg)
        return {
            "total_videos": total_videos_processed,
            "total_comments": total_comments,
            "errors": [error_msg]
        }
    finally:
        db.close()