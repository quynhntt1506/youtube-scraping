import os
import sys
import argparse
from src.utils.logger import CustomLogger
from src.services.generate_keywords import generate_and_crawl
from src.scripts.reset_quota import main as reset_quota_main
from src.services.video_crawler_service import start_video_crawler
from src.services.comment_crawler_service import start_comment_crawler
from src.rabbitmq_consumer import main as rabbitmq_main
from src.kafka_consumer import main as kafka_main
from src.controller.crawler_to_kafka import crawl_channel_by_custom_urls

# Initialize logger
logger = CustomLogger("main")

def main():
    parser = argparse.ArgumentParser(description='YouTube Crawler Service')
    parser.add_argument('--service', type=str, required=True,
                      choices=['crawl-data', 'reset-quota', 'crawl-video', 'crawl-comment', 'rabbitmq', 'kafka', 'kafka-channel'],
                      help='Service to run')
    parser.add_argument('--num-keywords', type=int, default=1,
                      help='Number of keywords to generate (default: 1)')
    parser.add_argument('--max-workers', type=int, default=1,
                      help='Maximum number of worker threads')
    parser.add_argument('--channel-url', type=str, default=1,
                      help='Channel url to crawl')
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
            start_video_crawler(args.max_workers)
        elif args.service == 'crawl-comment':
            logger.info("Starting crawl-video service...")
            start_comment_crawler(args.max_workers)
        elif args.service == 'rabbitmq':
            logger.info("Starting RabbitMQ consumer service...")
            rabbitmq_main()
        elif args.service == 'kafka':
            logger.info("Starting Kafka consumer service...")
            kafka_main()
        elif args.service == 'kafka-channel':
            logger.info("Starting crawl channel url...")
            crawl_channel_by_custom_urls([args.channel_url])
    except KeyboardInterrupt:
        logger.info("Service stopped by user")
    except Exception as e:
        logger.error(f"Error running service: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 
