from typing import List
from src.utils.logger import CustomLogger
from src.utils.keyword_generator import KeywordGenerator
from src.controller.crawler_to_kafka import crawl_info_from_keyword
import time

logger = CustomLogger(__name__)

def generate_and_crawl_keywords(num_keywords: int = 1):
    """
    Generates a batch of keywords and crawls them immediately without saving to the DB.
    
    Args:
        num_keywords (int): The number of keywords to generate and process.
    """
    generator = KeywordGenerator()
    logger.info("Starting keyword generation and crawling process.")
    
    try:
        # Use the new stateless generator function
        keywords = generator.generate_keywords_stateless(num_keywords)
        if not keywords:
            logger.warning("No keywords were generated. Exiting.")
            return

        logger.info(f"Generated {len(keywords)} keywords for crawling: {keywords}")
        
        # Crawl the generated keywords immediately
        crawl_info_from_keyword(keywords)
        
        logger.info("Finished crawling batch of generated keywords.")

    except Exception as e:
        logger.error(f"An error occurred during keyword generation and crawling: {e}", exc_info=True)
    finally:
        # generator.close() # Removed as KeywordGenerator is now stateless
        logger.info("Keyword generation and crawling process finished.")

if __name__ == '__main__':
    # Example of how to run the service
    # This will generate 1 keywords and crawl them.
    generate_and_crawl_keywords(1) 