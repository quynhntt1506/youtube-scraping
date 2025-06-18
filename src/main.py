import os
import sys
import argparse
from src.utils.logger import CustomLogger
from src.generate_keywords import generate_and_crawl
from src.scripts.reset_quota import main as reset_quota_main
from src.controller.crawler_auto import crawl_videos_from_crawled_channels
from src.rabbitmq_consumer import main as rabbitmq_main

# Initialize logger
logger = CustomLogger("main")

def main():
    parser = argparse.ArgumentParser(description='YouTube Crawler Service')
    parser.add_argument('--service', type=str, required=True,
                      choices=['crawl-data', 'reset-quota', 'crawl-video', 'rabbitmq'],
                      help='Service to run')
    parser.add_argument('--num-keywords', type=int, default=10,
                      help='Number of keywords to generate (default: 10)')
    
    args = parser.parse_args()
    
    try:
        if args.service == 'crawl-data':
            logger.info("Starting crawl-data service...")
            generate_and_crawl(args.num_keywords)
        elif args.service == 'reset-quota':
            logger.info("Starting reset-quota service...")
            reset_quota_main()
        elif args.service == 'crawl-video':
            logger.info("Starting crawl-video service...")
            crawl_videos_from_crawled_channels()
        elif args.service == 'rabbitmq':
            logger.info("Starting RabbitMQ consumer service...")
            rabbitmq_main()
        
    except KeyboardInterrupt:
        logger.info("Service stopped by user")
    except Exception as e:
        logger.error(f"Error running service: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 
