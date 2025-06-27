from typing import List
from src.utils.logger import CustomLogger
from src.database.keyword_manager import KeywordManager
from src.controller.crawler_to_kafka import crawl_info_from_keyword
import time

logger = CustomLogger(__name__)

def process_keyword_from_db(keyword: str):
    """
    Process a single keyword from database with status management.
    
    Args:
        keyword (str): The keyword to process
    """
    keyword_manager = KeywordManager()
    start_time = time.time()
    
    try:
        logger.info(f"Starting database operations for keyword: {keyword}")
        
        # Get keyword from database
        keyword_doc = keyword_manager.get_keyword_by_keyword(keyword)
        
        if keyword_doc and keyword_doc.get("status") == "crawled":
            logger.info(f"Keyword {keyword} is already crawled, skipping...")
            return
        elif keyword_doc and keyword_doc.get("status") == "to_crawl":
            # Update status to crawling
            keyword_manager.update_keyword(keyword, status="crawling")
            logger.info(f"Updated status of keyword {keyword} to 'crawling'")
            
            # Crawl the keyword
            logger.info(f"Starting crawl for keyword: {keyword}")
            try:
                crawl_result = crawl_info_from_keyword(keyword)
                
                # Check if crawl was successful
                if crawl_result and crawl_result.get("success", False):
                    logger.info(f"Successfully crawled keyword {keyword}")
                    keyword_manager.update_keyword(keyword, status="crawled")
                    logger.info(f"Updated status of keyword {keyword} to 'crawled'")
                else:
                    logger.warning(f"Crawl completed but no data found for keyword {keyword}")
                    keyword_manager.update_keyword(keyword, status="error")
                    logger.info(f"Updated status of keyword {keyword} to 'error' - no data found")
                    
            except Exception as crawl_error:
                logger.error(f"Failed to crawl keyword {keyword}: {str(crawl_error)}")
                keyword_manager.update_keyword(keyword, status="error")
                logger.info(f"Updated status of keyword {keyword} to 'error'")
        else:
            logger.warning(f"Keyword {keyword} not found in database or has invalid status")
            
    except Exception as e:
        logger.error(f"Error processing keyword {keyword}: {str(e)}")
        # Update keyword status to error if something goes wrong
        try:
            keyword_manager.update_keyword(keyword, status="error")
            logger.info(f"Updated status of keyword {keyword} to 'error'")
        except Exception as db_error:
            logger.error(f"Error updating keyword status: {str(db_error)}")
    finally:
        try:
            keyword_manager.close()
        except Exception as close_error:
            logger.error(f"Error closing database connection: {str(close_error)}")
        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"Finished processing keyword: {keyword} (took {duration:.2f} seconds)")

def generate_and_crawl_keywords(num_keywords: int = 1):
    """
    Generate new keywords and crawl them.
    
    Args:
        num_keywords (int): The number of keywords to generate and process.
    """
    keyword_manager = KeywordManager()
    logger.info("Starting keyword generation and crawling process.")
    
    try:
        # Generate new keywords first
        from src.utils.keyword_generator import KeywordGenerator
        generator = KeywordGenerator()
        
        logger.info(f"Generating {num_keywords} new keywords...")
        generated_keywords = generator.generate_keywords(num_keywords)
        
        if not generated_keywords:
            logger.warning("No keywords were generated. Exiting.")
            return
        
        logger.info(f"Generated {len(generated_keywords)} keywords: {generated_keywords}")
        
        # Process each generated keyword
        for keyword in generated_keywords:
            process_keyword_from_db(keyword)
        
        logger.info("Finished crawling batch of generated keywords.")
        
        # Close generator connection
        generator.close()

    except Exception as e:
        logger.error(f"An error occurred during keyword generation and crawling: {e}", exc_info=True)
    finally:
        try:
            keyword_manager.close()
        except Exception as close_error:
            logger.error(f"Error closing database connection: {str(close_error)}")
        logger.info("Keyword generation and crawling process finished.")

if __name__ == '__main__':
    # Example of how to run the service
    # This will find and crawl 1 keyword with 'to_crawl' status
    generate_and_crawl_keywords(1) 