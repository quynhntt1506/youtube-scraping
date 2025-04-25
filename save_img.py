import os
import requests
from typing import Dict, Any
from pymongo import MongoClient

# Constants
IMAGE_DIR_VIDEO = "video_thumbnails"

# Create directory if it doesn't exist
if not os.path.exists(IMAGE_DIR_VIDEO):
    os.makedirs(IMAGE_DIR_VIDEO)

# MongoDB setup
client = MongoClient("mongodb://localhost:27017/")
db = client["youtube_data"]
videos_collection = db["videos"]

def download_and_save_thumbnail(video: Dict[str, Any]) -> None:
    """Download and save video thumbnail.
    
    Args:
        video (Dict[str, Any]): Video document containing videoId and thumbnailUrl
    """
    video_id = video.get("videoId")
    thumbnail_url = video.get("thumbnailUrl")

    if not thumbnail_url or not video_id:
        print(f"❌ Skipping due to missing videoId or thumbnailUrl")
        return

    try:
        # Download image
        response = requests.get(thumbnail_url)
        if response.status_code != 200:
            print(f"⚠️ Failed to download image from {thumbnail_url}")
            return

        # Set filename as videoId.jpg
        filename = f"{video_id}.jpg"
        save_path = os.path.join(IMAGE_DIR_VIDEO, filename)

        # Save image
        with open(save_path, "wb") as f:
            f.write(response.content)

        print(f"✅ Saved: {save_path}")

    except Exception as e:
        print(f"❌ Error with {video_id}: {str(e)}")

def main() -> None:
    """Main function to process all videos and download their thumbnails."""
    # Get all documents with thumbnailUrl and videoId
    videos = videos_collection.find({}, {"_id": 0, "videoId": 1, "thumbnailUrl": 1})
    
    for video in videos:
        download_and_save_thumbnail(video)

if __name__ == "__main__":
    main()
