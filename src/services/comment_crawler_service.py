import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from typing import Dict, Any, List
from utils.logger import CustomLogger
from src.database.database import Database
from src.controller.crawler_auto import crawl_comments_from_videos
from src.config.config import COUNT_THREADS_DEFAULT, MIN_ENTITY_IN_BATCH, MAX_ENTITY_IN_BATCH
import time

logger = CustomLogger("comment_crawler_service")

class CommentCrawlerService:
    def __init__(self, num_threads: int = COUNT_THREADS_DEFAULT, batch_size: int = MIN_ENTITY_IN_BATCH):
        """Initialize the comment crawler service.
        
        Args:
            num_threads (int): Number of threads to use for crawling
            batch_size (int): Number of videos to process in each batch
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
        
    def process_batch(self, batch_videos: List[Dict[str, Any]]) -> None:
        """Process a batch of videos in a separate thread.
        
        Args:
            batch_videos (List[Dict[str, Any]]): List of videos to process
        """
        try:
            self.increment_active_threads()
            thread_id = threading.get_ident()
            video_ids = [v["videoId"] for v in batch_videos]
            logger.info(f"Thread {thread_id} STARTED at {time.strftime('%H:%M:%S')} processing videos: {video_ids}")
            
            comment_result = crawl_comments_from_videos(video_ids)
            
            if comment_result:
                batch_comments = len(comment_result.get("comments", []))
                self.result_queue.put({
                    "videos_processed": len(batch_videos),
                    "comments": batch_comments,
                    "error": None
                })
                logger.info(f"Thread {thread_id} FINISHED at {time.strftime('%H:%M:%S')} - processed videos {video_ids}, found {batch_comments} comments")
            else:
                self.result_queue.put({
                    "videos_processed": len(batch_videos),
                    "comments": 0,
                    "error": None
                })
                logger.info(f"Thread {thread_id} FINISHED at {time.strftime('%H:%M:%S')} - processed videos {video_ids}, no comments found")
                
        except Exception as e:
            error_msg = f"Error processing batch in thread {thread_id}: {str(e)}"
            logger.error(error_msg)
            self.result_queue.put({
                "videos_processed": len(batch_videos),
                "comments": 0,
                "error": error_msg
            })
        finally:
            self.decrement_active_threads()
    
    def crawl_all_comments(self) -> Dict[str, Any]:
        """Crawl comments from all videos with status crawled_video using multiple threads.
        
        Returns:
            Dict[str, Any]: Result containing:
                - total_videos: Total number of videos processed
                - total_comments: Total number of comments crawled
                - errors: List of errors encountered
        """
        try:
            total_videos_processed = 0
            total_comments = 0
            errors = []
            skip = 0
            
            logger.info(f"Starting comment crawler with {self.num_threads} threads")
            
            with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
                while True:
                    # Query a small batch of videos
                    videos = self.db.collections["videos"].find(
                        {"status": "crawled_video"},
                        {"videoId": 1}
                    ).skip(skip).limit(self.batch_size)
                    
                    videos = list(videos)
                    if not videos:
                        logger.info("No more videos with status crawled_video found")
                        break
                    
                    # Calculate videos per thread for this batch
                    total_videos = len(videos)
                    videos_per_thread = total_videos // self.num_threads
                    remaining_videos = total_videos % self.num_threads
                    
                    logger.info(f"Found {total_videos} videos in current batch, distributing {videos_per_thread} videos per thread")
                    
                    # Distribute videos among threads
                    start_idx = 0
                    futures = []
                    for i in range(self.num_threads):
                        # Add extra video to first few threads if there are remaining videos
                        batch_size = videos_per_thread + (1 if i < remaining_videos else 0)
                        end_idx = start_idx + batch_size
                        
                        if start_idx < total_videos:
                            batch_videos = videos[start_idx:end_idx]
                            video_ids = [v["videoId"] for v in batch_videos]
                            logger.info(f"Submitting batch of {len(batch_videos)} videos to thread {i+1}: {video_ids}")
                            future = executor.submit(self.process_batch, batch_videos)
                            futures.append(future)
                            start_idx = end_idx
                    
                    # Wait for all futures in this batch to complete
                    for future in as_completed(futures):
                        try:
                            result = self.result_queue.get()
                            total_videos_processed += result["videos_processed"]
                            total_comments += result["comments"]
                            if result["error"]:
                                errors.append(result["error"])
                        except Exception as e:
                            errors.append(str(e))
                    
                    # Update skip for next batch
                    skip += total_videos
                    
                    # Log progress
                    logger.info(f"Completed batch. Total progress: {total_videos_processed} videos, {total_comments} comments")
            
            logger.info(f"Finished crawling comments. Total videos processed: {total_videos_processed}, Total comments: {total_comments}")
            return {
                "total_videos": total_videos_processed,
                "total_comments": total_comments,
                "errors": errors
            }
            
        except Exception as e:
            error_msg = f"Error in crawl_all_comments: {str(e)}"
            logger.error(error_msg)
            return {
                "total_videos": total_videos_processed,
                "total_comments": total_comments,
                "errors": [error_msg]
            }
        finally:
            self.db.close()

def start_comment_crawler(num_threads: int = COUNT_THREADS_DEFAULT, batch_size: int = MIN_ENTITY_IN_BATCH) -> Dict[str, Any]:
    """Start the comment crawler service.
    
    Args:
        num_threads (int): Number of threads to use for crawling
        batch_size (int): Number of videos to process in each batch
        
    Returns:
        Dict[str, Any]: Result of the crawling process
    """
    crawler = CommentCrawlerService(num_threads=num_threads, batch_size=batch_size)
    return crawler.crawl_all_comments() 