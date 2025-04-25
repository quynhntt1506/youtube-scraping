import os
import requests
from pymongo import MongoClient

# Thư mục lưu ảnh
IMAGE_DIR_VIDEO = "video_thumbnails"
if not os.path.exists(IMAGE_DIR_VIDEO):
    os.makedirs(IMAGE_DIR_VIDEO)

# Kết nối MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["youtube_data"]
videos_collection = db["videos"]

# Lấy tất cả document có thumbnailUrl và videoId
videos = videos_collection.find({}, {"_id": 0, "videoId": 1, "thumbnailUrl": 1})

for video in videos:
    video_id = video.get("videoId")
    thumbnail_url = video.get("thumbnailUrl")

    if not thumbnail_url or not video_id:
        print(f"❌ Bỏ qua vì thiếu videoId hoặc thumbnailUrl")
        continue

    try:
        # Tải ảnh
        response = requests.get(thumbnail_url)
        if response.status_code != 200:
            print(f"⚠️ Không tải được ảnh từ {thumbnail_url}")
            continue

        # Đặt tên file là videoId.jpg
        filename = f"{video_id}.jpg"
        save_path = os.path.join(IMAGE_DIR_VIDEO, filename)

        # Lưu ảnh
        with open(save_path, "wb") as f:
            f.write(response.content)

        print(f"✅ Đã lưu: {save_path}")

    except Exception as e:
        print(f"❌ Lỗi với {video_id}: {str(e)}")
