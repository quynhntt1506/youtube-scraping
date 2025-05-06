import argparse
from utils.keyword_generator import KeywordGenerator
from src.controller.crawler import crawl_video_in_channel_by_many_keywords
from datetime import datetime
from utils.database import Database

def generate_and_crawl(num_keywords: int = 1):
    """Generate Vietnamese keywords and start crawling process.
    
    Args:
        num_keywords (int): Number of keywords to generate
    """
    # Initialize keyword generator
    generator = KeywordGenerator()
    
    try:
        # Get statistics about available keywords
        stats = generator.get_keyword_stats()
        for category, count in stats.items():
            print(f"{category}: {count} keywords")
        
        # Generate keywords
        keywords = generator.generate_keywords(num_keywords)
        
        crawl_video_in_channel_by_many_keywords(keywords)
        
    finally:
        # Close MongoDB connection
        generator.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Vietnamese keywords and start crawling process")
    parser.add_argument("--num-keywords", type=int, default=1,
                       help="Number of keywords to generate (default: 1)")
    
    args = parser.parse_args()
    generate_and_crawl(args.num_keywords) 