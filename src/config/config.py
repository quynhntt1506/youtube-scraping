import os
from pathlib import Path

# Base paths
BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
IMAGES_DIR = DATA_DIR / "images"
CHANNEL_IMAGES_DIR = IMAGES_DIR / "channels"
VIDEO_IMAGES_DIR = IMAGES_DIR / "videos"

# API Configuration
MAX_CHANNELS = 10
API_KEYS_FILE = BASE_DIR / "apikey.txt"

# MongoDB Configuration
MONGODB_URI = "mongodb://localhost:27017/"
MONGODB_DB = "youtube_data"
MONGODB_COLLECTIONS = {
    "channels": "channels",
    "videos": "videos",
    "keywords": "keywords"
}

# Create necessary directories
for directory in [RAW_DATA_DIR, PROCESSED_DATA_DIR, CHANNEL_IMAGES_DIR, VIDEO_IMAGES_DIR]:
    directory.mkdir(parents=True, exist_ok=True) 