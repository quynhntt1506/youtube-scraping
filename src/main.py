import os
import sys
import argparse
from utils.logger import CustomLogger
from generate_keywords import generate_and_crawl
from scripts.reset_quota import main as reset_quota_main

# Initialize logger
logger = CustomLogger("main")

def main():
    parser = argparse.ArgumentParser(description='YouTube Crawler Service')
    parser.add_argument('--service', type=str, required=True, choices=['crawl-data', 'reset-quota'],
                      help='Service to run: crawl-data or reset-quota')
    parser.add_argument('--num-keywords', type=int, default=1,
                      help='Number of keywords to generate (only for crawl-data service)')
    
    args = parser.parse_args()
    
    try:
        if args.service == 'crawl-data':
            logger.info("Starting crawl-data service...")
            generate_and_crawl(args.num_keywords)
        elif args.service == 'reset-quota':
            logger.info("Starting reset-quota service...")
            reset_quota_main()
    except KeyboardInterrupt:
        logger.info("Service stopped by user")
    except Exception as e:
        logger.error(f"Error running service: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 
