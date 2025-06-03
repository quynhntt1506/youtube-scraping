import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from typing import Dict, Any, List
from utils.logger import CustomLogger
from src.database.database import Database
from src.controller.crawler import crawl_videos_from_channels
from src.config.config import MAX_ENTITY_IN_BATCH, COUNT_THREADS_DEFAULT, MIN_ENTITY_IN_BATCH

logger = CustomLogger("video_crawler_service")

class VideoCrawlerService:
    def __init__(self, num_threads: int = COUNT_THREADS_DEFAULT, batch_size: int = MIN_ENTITY_IN_BATCH):
        """Initialize the video crawler service.
        
        Args:
            num_threads (int): Number of threads to use for crawling
            batch_size (int): Number of channels to process in each batch
        """
        self.num_threads = num_threads
        self.batch_size = batch_size
        self.db = Database()
        self.result_queue = Queue()
        self.active_threads = 0
        self.thread_lock = threading.Lock()
        
    def increment_active_threads(self):
        """Increment the count of active threads."""
        with self.thread_lock:
            self.active_threads += 1
            logger.info(f"Active threads: {self.active_threads}/{self.num_threads}")
            
    def decrement_active_threads(self):
        """Decrement the count of active threads."""
        with self.thread_lock:
            self.active_threads -= 1
            logger.info(f"Active threads: {self.active_threads}/{self.num_threads}")
        
    def process_batch(self, batch_channels: List[Dict[str, Any]]) -> None:
        """Process a batch of channels in a separate thread.
        
        Args:
            batch_channels (List[Dict[str, Any]]): List of channels to process
        """
        try:
            self.increment_active_threads()
            thread_id = threading.get_ident()
            channel_ids = [c["channelId"] for c in batch_channels]
            logger.info(f"Thread {thread_id} starting to process channels: {channel_ids}")
            
            video_result = crawl_videos_from_channels(batch_channels)
            
            if video_result:
                batch_videos = len(video_result.get("videos", []))
                self.result_queue.put({
                    "channels_processed": len(batch_channels),
                    "videos": batch_videos,
                    "error": None
                })
                logger.info(f"Thread {thread_id} finished processing channels {channel_ids}, found {batch_videos} videos")
            else:
                self.result_queue.put({
                    "channels_processed": len(batch_channels),
                    "videos": 0,
                    "error": None
                })
                logger.info(f"Thread {thread_id} finished processing channels {channel_ids}")
                
        except Exception as e:
            error_msg = f"Error processing batch in thread {thread_id}: {str(e)}"
            logger.error(error_msg)
            self.result_queue.put({
                "channels_processed": len(batch_channels),
                "videos": 0,
                "error": error_msg
            })
        finally:
            self.decrement_active_threads()
    
    def crawl_all_videos(self) -> Dict[str, Any]:
        """Crawl videos from all channels with status crawled_channel using multiple threads.
        
        Returns:
            Dict[str, Any]: Result containing:
                - total_channels: Total number of channels processed
                - total_videos: Total number of videos crawled
                - errors: List of errors encountered
        """
        try:
            total_channels_processed = 0
            total_videos = 0
            errors = []
            skip = 0
            
            logger.info(f"Starting video crawler with {self.num_threads} threads")
            
            with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
                futures = []
                
                while True:
                    # Find channels with status crawled_channel using skip for pagination
                    channels = self.db.collections["channels"].find(
                        {"status": "crawled_channel"},
                        {"channelId": 1, "playlistId": 1}
                    ).skip(skip).limit(MAX_ENTITY_IN_BATCH)
                    
                    channels = list(channels)
                    if not channels:
                        logger.info("No more channels with status crawled_channel found")
                        break
                    
                    # Calculate batch size for each thread
                    total_channels = len(channels)
                    channels_per_thread = total_channels // self.num_threads
                    remaining_channels = total_channels % self.num_threads
                    
                    logger.info(f"Found {total_channels} channels, distributing {channels_per_thread} channels per thread")
                    
                    # Distribute channels among threads
                    start_idx = 0
                    for i in range(self.num_threads):
                        # Add extra channel to first few threads if there are remaining channels
                        batch_size = channels_per_thread + (1 if i < remaining_channels else 0)
                        end_idx = start_idx + batch_size
                        
                        if start_idx < total_channels:
                            batch_channels = channels[start_idx:end_idx]
                            logger.info(f"Submitting batch of {len(batch_channels)} channels to thread {i+1}")
                            future = executor.submit(self.process_batch, batch_channels)
                            futures.append(future)
                            start_idx = end_idx
                    
                    skip += total_channels
                
                # Wait for all futures to complete and collect results
                for future in as_completed(futures):
                    try:
                        result = self.result_queue.get()
                        total_channels_processed += result["channels_processed"]
                        total_videos += result["videos"]
                        if result["error"]:
                            errors.append(result["error"])
                    except Exception as e:
                        errors.append(str(e))
            
            logger.info(f"Finished crawling videos. Total channels processed: {total_channels_processed}, Total videos: {total_videos}")
            return {
                "total_channels": total_channels_processed,
                "total_videos": total_videos,
                "errors": errors
            }
            
        except Exception as e:
            error_msg = f"Error in crawl_all_videos: {str(e)}"
            logger.error(error_msg)
            return {
                "total_channels": total_channels_processed,
                "total_videos": total_videos,
                "errors": [error_msg]
            }
        finally:
            self.db.close()

def start_video_crawler(num_threads: int = COUNT_THREADS_DEFAULT, batch_size: int = MIN_ENTITY_IN_BATCH) -> Dict[str, Any]:
    """Start the video crawler service.
    
    Args:
        num_threads (int): Number of threads to use for crawling
        batch_size (int): Number of channels to process in each batch
        
    Returns:
        Dict[str, Any]: Result of the crawling process
    """
    crawler = VideoCrawlerService(num_threads=num_threads, batch_size=batch_size)
    return crawler.crawl_all_videos() 