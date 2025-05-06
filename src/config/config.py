import os
from pathlib import Path

# MongoDB configuration
MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
MONGODB_DB = os.getenv('MONGODB_DB', 'youtube_crawl')

# Base directories
BASE_DIR = Path('/app')
DATA_DIR = BASE_DIR / 'data'
IMAGES_DIR = DATA_DIR / 'images'
LOGS_DIR = BASE_DIR / 'logs'

# Image directories
CHANNEL_IMAGES_DIR = IMAGES_DIR / 'channels'
VIDEO_IMAGES_DIR = IMAGES_DIR / 'thumbnailvideos'

# Create directories if they don't exist
for directory in [LOGS_DIR, CHANNEL_IMAGES_DIR, VIDEO_IMAGES_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# API configuration
MAX_CHANNELS = 100
MAX_RESULTS = 50
MAX_RETRIES = 3
RETRY_DELAY = 1  # seconds

# Logging configuration
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_LEVEL = 'INFO'
LOG_FILE = LOGS_DIR / 'app.log'

# Base paths
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
API_KEYS_FILE = BASE_DIR / "apikey.txt"

# MongoDB Configuration
MONGODB_COLLECTIONS = {
    "channels": "channels",
    "videos": "videos",
    "keywords": "keywords",
    "api_keys": "api_keys",
    "keyword_generation": "keyword_generation",
}

# Create necessary directories
for directory in [RAW_DATA_DIR, PROCESSED_DATA_DIR, CHANNEL_IMAGES_DIR, VIDEO_IMAGES_DIR]:
    directory.mkdir(parents=True, exist_ok=True) 