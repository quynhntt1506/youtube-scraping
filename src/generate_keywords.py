import argparse
from utils.keyword_generator import KeywordGenerator
from src.controller.crawler import crawl_video_in_channel_by_keyword
from datetime import datetime
from src.database.database import Database
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List
from utils.logger import CustomLogger
import time

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
                logger.info(f"Thread {thread_id}: Crawled {result.get('count_channels')} channels, {result.get('count_videos')} videos, comments for keyword {keyword}")
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

def generate_and_crawl(num_keywords: int = 1, max_workers: int = 5):
    """Generate Vietnamese keywords and start crawling process with threading.
    
    Args:
        num_keywords (int): Number of keywords to generate
        max_workers (int): Maximum number of worker threads
    """
    # Initialize keyword generator
    generator = KeywordGenerator()
    start_time = time.time()
    
    try:
        # Get statistics about available keywords
        stats = generator.get_keyword_stats()
        for category, count in stats.items():
            logger.info(f"{category}: {count} keywords")
        
        # Generate keywords
        keywords = generator.generate_keywords(num_keywords)
        logger.info(f"Generated {len(keywords)} keywords")
        logger.info(f"Starting {max_workers} worker threads at {time.strftime('%H:%M:%S')}")
        
        # Process keywords in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_keyword = {executor.submit(process_keyword, keyword): keyword for keyword in keywords}
            
            # Log initial thread count
            active_threads = threading.enumerate()
            logger.info(f"Initial active threads: {len(active_threads)}")
            
            # Wait for all tasks to complete and log results
            for future in as_completed(future_to_keyword):
                keyword = future_to_keyword[future]
                try:
                    future.result()  # This will raise any exceptions that occurred
                except Exception as e:
                    logger.error(f"Thread processing keyword {keyword} generated an exception: {str(e)}")
                
                # Log current thread count after each completion
                active_threads = threading.enumerate()
                logger.info(f"Current active threads: {len(active_threads)}")
        
    finally:
        # Close MongoDB connection
        generator.close()
        end_time = time.time()
        total_duration = end_time - start_time
        logger.info(f"All threads completed at {time.strftime('%H:%M:%S')} (total time: {total_duration:.2f} seconds)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Vietnamese keywords and start crawling process")
    parser.add_argument("--num-keywords", type=int, default=1,
                       help="Number of keywords to generate (default: 1)")
    
    args = parser.parse_args()
    generate_and_crawl(args.num_keywords) 