import os
from pathlib import Path

# MongoDB configuration
# MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://192.168.161.230:27011,192.168.161.230:27012,192.168.161.230:27013/?replicaSet=rs0')
# MONGODB_DB = os.getenv('MONGODB_DB', 'youtube_data')

MONGODB_URI = 'mongodb://localhost:27017/'
MONGODB_DB = 'youtube_crawl_test'


# Define base directories
BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / "data"

# Define all directories
DIRECTORIES = {
    # Local paths
    "channels": DATA_DIR / "images" / "channels",
    "thumbnailvideos": DATA_DIR / "images" / "thumbnailvideos",
    
    # Docker paths
    "channels_build": Path("/mnt/data/youtube/images/channels"),
    "thumbnailvideos_build": Path("/mnt/data/youtube/images/thumbnailvideos"),
}

# Create directories if they don't exist
for directory in DIRECTORIES.values():
    directory.mkdir(parents=True, exist_ok=True)

# API configuration
MAX_CHANNELS = 1
MIN_ENTITY_IN_BATCH = 50
MAX_ENTITY_IN_BATCH = 100
MAX_RESULTS_PER_PAGE = 1
MAX_ID_PAYLOAD = 50
MAX_FILES_PER_FOLDER = 5000
COUNT_FILES_DOWNLOAD_SIMULTANEOUSLY = 100
MAX_RETRIES = 3
RETRY_DELAY = 1  # seconds

# Logging configuration
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# Base paths
# PROCESSED_DATA_DIR = DATA_DIR / "processed"
# PROCESSED_PLAYLIST_DATA_DIR = DATA_DIR / "processed_playlist"
# PROCESSED_CHANNEL_DATA_DIR = DATA_DIR / "processed_channel"
# PROCESSED_VIDEO_DATA_DIR = DATA_DIR / "processed_video"
# PROCESSED_COMMENTS_DATA_DIR = DATA_DIR / "processed_comments"

STATUS_ENTITY = {
    "to_crawl": "to_crawl",
    "crawled_channel": "crawled_channel",
    "crawled_video": "crawled_video",
    "crawled_comment": "crawled_comment"
}



# MongoDB Configuration
MONGODB_COLLECTIONS = {
    "channels": "youtube_channels",
    "videos": "youtube_videos",
    "youtube_crawl_history": "youtube_crawl_history",
    "api_keys": "youtube_api_keys",
    "youtube_keywords": "youtube_keywords",
    "comments": "youtube_comments",
}