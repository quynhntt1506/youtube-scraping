import os
import sys
import argparse
from utils.logger import CustomLogger
from src.services.generate_keywords import generate_and_crawl
from scripts.reset_quota import main as reset_quota_main
# from src.controller.crawler import crawl_videos_from_crawled_channels, crawl_comments_from_crawled_videos
from src.services.comment_crawler_service import start_comment_crawler
from src.services.video_crawler_service import start_video_crawler

# Initialize logger
logger = CustomLogger("main")

def main():
    parser = argparse.ArgumentParser(description='YouTube Crawler Service')
    parser.add_argument('--service', type=str, required=True, choices=['crawl-data', 'reset-quota', 'crawl-video', 'crawl-comment'],
                      help='Service to run: crawl-data or reset-quota')
    parser.add_argument('--num-keywords', type=int, default=1,
                      help='Number of keywords to generate (only for crawl-data service)')
    parser.add_argument('--max-workers', type=int, default=5,
                      help='Maximum number of worker threads')
    
    args = parser.parse_args()
    
    try:
        if args.service == 'crawl-data':
            logger.info("Starting crawl-data service...")
            generate_and_crawl(args.num_keywords, args.max_workers)
        elif args.service == 'reset-quota':
            logger.info("Starting reset-quota service...")
            reset_quota_main()
        elif args.service == 'crawl-video':
            logger.info("Starting crawl-video service...")
            start_video_crawler(args.max_workers)
        elif args.service == 'crawl-comment':
            logger.info("Starting crawl-comment service...")
            start_comment_crawler(args.max_workers)
    except KeyboardInterrupt:
        logger.info("Service stopped by user")
    except Exception as e:
        logger.error(f"Error running service: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 
