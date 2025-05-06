import json
import requests
import asyncio
import aiohttp
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List

from utils.api import YouTubeAPI
from utils.database import Database
from utils.logger import CustomLogger
from config.config import (
    VIDEO_IMAGES_DIR,
    MAX_CHANNELS,
    CHANNEL_IMAGES_DIR
)
from utils.api_key_manager import APIKeyManager
from src.controller.crawler import crawl_video_in_channel_by_keyword_from_file

# Initialize logger
logger = CustomLogger("main")

if __name__ == "__main__":
    crawl_video_in_channel_by_keyword_from_file() 


