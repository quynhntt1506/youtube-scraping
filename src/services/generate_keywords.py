import argparse
from utils.keyword_generator import KeywordGenerator
from src.controller.crawler_auto import crawl_video_in_channel_by_keyword
from datetime import datetime
from src.database.database import Database
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List
from utils.logger import CustomLogger
import time
from src.config.config import COUNT_THREADS_DEFAULT
# Initialize logger
logger = CustomLogger("generate_keywords")

def process_keyword(keyword: str):
    """Process a single keyword in a thread."""
    thread_id = threading.get_ident()
    thread_name = threading.current_thread().name
    start_time = time.time()
    logger.info(f"Thread {thread_id} ({thread_name}) started processing keyword: {keyword} at {time.strftime('%H:%M:%S')}")
    
    try:
        # Simulate some initial work
        time.sleep(1)
        logger.info(f"Thread {thread_id}: Starting database operations for {keyword}")
        
        db = Database()
        keyword_doc = db.get_keyword_by_keyword(keyword)
        if keyword_doc and keyword_doc.get("status") == "crawled":
            logger.info(f"Thread {thread_id}: Keyword {keyword} is already crawled, skipping...")
            return
        elif keyword_doc and keyword_doc.get("status") == "to_crawl":
            # Update status to crawling
            db.update_keyword_status(keyword, "crawling")
            logger.info(f"Thread {thread_id}: Updated status of keyword {keyword} to 'crawling'")
            
            # Simulate API call work
            logger.info(f"Thread {thread_id}: Simulating API call for {keyword}")
            time.sleep(3)  # Simulate API call delay
            
            result = crawl_video_in_channel_by_keyword(keyword)
            if result:
                logger.info(f"Thread {thread_id}: Crawled {result.get('count_channels')} channels for keyword {keyword}")
                db.update_keyword_status(keyword, "crawled")
                logger.info(f"Thread {thread_id}: Updated status of keyword {keyword} to 'crawled'")
            else:
                logger.warning(f"Thread {thread_id}: Keyword {keyword} not found in database or has invalid status")
    except Exception as e:
        logger.error(f"Thread {thread_id}: Error processing keyword {keyword}: {str(e)}")
        # Update keyword status to error if something goes wrong
        try:
            db.update_keyword_status(keyword, "error")
            logger.info(f"Thread {thread_id}: Updated status of keyword {keyword} to 'error'")
        except Exception as db_error:
            logger.error(f"Thread {thread_id}: Error updating keyword status: {str(db_error)}")
    finally:
        try:
            db.close()
        except Exception as close_error:
            logger.error(f"Thread {thread_id}: Error closing database connection: {str(close_error)}")
        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"Thread {thread_id} ({thread_name}) finished processing keyword: {keyword} at {time.strftime('%H:%M:%S')} (took {duration:.2f} seconds)")

def generate_and_crawl(num_keywords: int = 1, max_workers: int = COUNT_THREADS_DEFAULT):
    """Generate Vietnamese keywords and start crawling process with threading.
    Will run continuously until stopped.
    
    Args:
        num_keywords (int): Number of keywords to generate in each batch
        max_workers (int): Maximum number of worker threads
    """
    # Initialize keyword generator
    generator = KeywordGenerator()
    start_time = time.time()
    skip = 0
    
    try:
        # while True:  # Run continuously until stopped
        try:
            # Get statistics about available keywords
            stats = generator.get_keyword_stats()
            for category, count in stats.items():
                logger.info(f"{category}: {count} keywords")
            
            # Get batch of keywords
            keywords = generator.generate_keywords(num_keywords)
            if not keywords:
                logger.warning("No more keywords available. Waiting 60 seconds before retrying...")
                time.sleep(60)  # Wait before retrying
                # continue
            
            total_keywords = len(keywords)
            keywords_per_thread = total_keywords // max_workers
            remaining_keywords = total_keywords % max_workers
            
            logger.info(f"Found {total_keywords} keywords, distributing {keywords_per_thread} keywords per thread")
            
            # Process keywords in parallel using ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []
                start_idx = 0
                
                # Distribute keywords among threads
                for i in range(max_workers):
                    # Add extra keyword to first few threads if there are remaining keywords
                    batch_size = keywords_per_thread + (1 if i < remaining_keywords else 0)
                    end_idx = start_idx + batch_size
                    
                    if start_idx < total_keywords:
                        batch_keywords = keywords[start_idx:end_idx]
                        logger.info(f"Submitting batch of {len(batch_keywords)} keywords to thread {i+1}: {batch_keywords}")
                        future = executor.submit(process_keyword_batch, batch_keywords)
                        futures.append(future)
                        start_idx = end_idx
                
                # Wait for all tasks to complete and log results
                for future in as_completed(futures):
                    try:
                        future.result()  # This will raise any exceptions that occurred
                    except Exception as e:
                        logger.error(f"Thread generated an exception: {str(e)}")
                    
                    # Log current thread count after each completion
                    active_threads = threading.enumerate()
                    logger.info(f"Current active threads: {len(active_threads)}")
            
            # Log batch completion
            logger.info(f"Completed batch of {total_keywords} keywords. Starting next batch...")
            skip += total_keywords
            
        except Exception as e:
            logger.error(f"Error in main loop: {str(e)}")
            logger.info("Waiting 60 seconds before retrying...")
            time.sleep(60)  # Wait before retrying
            # continue
            
    finally:
        # Close MongoDB connection
        generator.close()
        end_time = time.time()
        total_duration = end_time - start_time
        logger.info(f"All threads completed at {time.strftime('%H:%M:%S')} (total time: {total_duration:.2f} seconds)")

def process_keyword_batch(keywords: List[str]):
    """Process a batch of keywords in a thread.
    
    Args:
        keywords (List[str]): List of keywords to process
    """
    thread_id = threading.get_ident()
    thread_name = threading.current_thread().name
    start_time = time.time()
    logger.info(f"Thread {thread_id} ({thread_name}) started processing {len(keywords)} keywords at {time.strftime('%H:%M:%S')}")
    
    try:
        db = Database()
        for keyword in keywords:
            try:
                keyword_doc = db.get_keyword_by_keyword(keyword)
                if keyword_doc and keyword_doc.get("status") == "crawled":
                    logger.info(f"Thread {thread_id}: Keyword {keyword} is already crawled, skipping...")
                    continue
                elif keyword_doc and keyword_doc.get("status") == "to_crawl":
                    # Update status to crawling
                    db.update_keyword_status(keyword, "crawling")
                    logger.info(f"Thread {thread_id}: Updated status of keyword {keyword} to 'crawling'")
                    
                    result = crawl_video_in_channel_by_keyword(keyword)
                    if result:
                        logger.info(f"Thread {thread_id}: Crawled {result.get('count_channels')} channels for keyword {keyword}")
                        db.update_keyword_status(keyword, "crawled")
                        logger.info(f"Thread {thread_id}: Updated status of keyword {keyword} to 'crawled'")
                    else:
                        logger.warning(f"Thread {thread_id}: Keyword {keyword} not found in database or has invalid status")
            except Exception as e:
                logger.error(f"Thread {thread_id}: Error processing keyword {keyword}: {str(e)}")
                try:
                    db.update_keyword_status(keyword, "error")
                    logger.info(f"Thread {thread_id}: Updated status of keyword {keyword} to 'error'")
                except Exception as db_error:
                    logger.error(f"Thread {thread_id}: Error updating keyword status: {str(db_error)}")
    except Exception as e:
        logger.error(f"Thread {thread_id}: Error in batch processing: {str(e)}")
    finally:
        try:
            db.close()
        except Exception as close_error:
            logger.error(f"Thread {thread_id}: Error closing database connection: {str(close_error)}")
        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"Thread {thread_id} ({thread_name}) finished processing batch at {time.strftime('%H:%M:%S')} (took {duration:.2f} seconds)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Vietnamese keywords and start crawling process")
    parser.add_argument("--num-keywords", type=int, default=1,
                       help="Number of keywords to generate in each batch (default: 1)")
    parser.add_argument("--max-workers", type=int, default=COUNT_THREADS_DEFAULT,
                       help=f"Maximum number of worker threads (default: {COUNT_THREADS_DEFAULT})")
    
    args = parser.parse_args()
    generate_and_crawl(args.num_keywords, args.max_workers)