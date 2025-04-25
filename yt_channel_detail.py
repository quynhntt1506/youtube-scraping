import googleapiclient.discovery
import googleapiclient.errors
import pandas as pd
import re
import requests
import os
import json
from pymongo import MongoClient
from datetime import datetime

MAX_CHANNELS = 1000

mongo_client = MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["youtube_data"]
mongo_collection_channels = mongo_db["channels"]
mongo_collection_videos = mongo_db["videos"]
mongo_collection_keyword = mongo_db["keyword"]

# Thay bằng API key của bạn
# Thay bằng API key thực tế của bạn
API_KEY = "AIzaSyDaYvVHnU50j_Ug4iCDzvUD500R2ylbz6c"

# Tạo dịch vụ YouTube
youtube = googleapiclient.discovery.build(
    "youtube", "v3", developerKey=API_KEY)

# Thư mục lưu ảnh
IMAGE_DIR_CHANNEL = "channel_images"
if not os.path.exists(IMAGE_DIR_CHANNEL):
    os.makedirs(IMAGE_DIR_CHANNEL)

IMAGE_DIR_VIDEO = "video_thumbnails"
if not os.path.exists(IMAGE_DIR_VIDEO):
    os.makedirs(IMAGE_DIR_VIDEO)
def load_api_keys(file_path="apikey.txt"):
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f"Error: {file_path} not found.")
        return []
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return []

def get_channels_and_videos_by_query(query, max_results=MAX_CHANNELS):
    channels = []
    videos = []
    next_page_token = None
    response_array = []
    current_date = datetime.now().strftime("%d-%m-%Y")
    # create file to save response of api google
    folder_path = os.path.join("result_crawl", current_date)
    os.makedirs(folder_path, exist_ok=True) 
    result_file_path = os.path.join(folder_path, f"{query}.json")

    while len(videos) < max_results:
        request = youtube.search().list(
            part="snippet",
            type="video,channel", 
            q=query,
            maxResults=50,
            regionCode="VN",
            relevanceLanguage="vi",
            pageToken=next_page_token
        )
        try:
            response = request.execute()
            response_array.append(response)
            # Save result to file
            with open(result_file_path, "w", encoding="utf-8") as f:
                json.dump(response_array, f, ensure_ascii=False, indent=4)
        except googleapiclient.errors.HttpError as e:
            print(f"Lỗi API khi tìm kiếm: {e}")
            break

        for item in response.get("items", []):
            # Kiểm tra xem là video hay kênh và lấy thông tin
            if item["id"]["kind"] == "youtube#channel":
                # Thông tin kênh
                channel_info = {
                    "channelId": item["snippet"]["channelId"],
                    "title": item["snippet"]["title"],
                    "description": item["snippet"]["description"],
                    "publishedAt": item["snippet"]["publishedAt"],
                }

                if not mongo_collection_channels.find_one({"channelId": channel_info["channelId"]}):
                    channels.append(channel_info)

            elif item["id"]["kind"] == "youtube#video":
                # Thông tin video
                video_info = {
                    "videoId": item["id"]["videoId"],
                    "title": item["snippet"]["title"],
                    "description": item["snippet"]["description"],
                    "publishedAt": item["snippet"]["publishedAt"],
                    "channelId": item["snippet"]["channelId"],
                    "channelTitle": item["snippet"]["channelTitle"],
                    "thumbnailUrl": item["snippet"]["thumbnails"].get("high", {}).get("url", "N/A"),
                }
                if not mongo_collection_videos.find_one({"videoId": video_info["videoId"]}):
                    mongo_collection_videos.insert_one(video_info)
                    videos.append(video_info)
                # thông tin channel của video
                channel_info_video = {
                    "channelId": item["snippet"]["channelId"],
                    "title": item["snippet"]["channelTitle"],
                }
                if not mongo_collection_channels.find_one({"channelId": channel_info_video["channelId"]}):
                    channels.append(channel_info_video)

            if len(videos) >= max_results and len(channels) >= 2 * max_results:
                break

        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            print(f"Đã hết kết quả tìm kiếm cho truy vấn: {query}")
            break

    return {"channels": channels[:max_results], "videos": videos[:max_results]}


def extract_email(description):
    """Trích xuất email từ mô tả kênh nếu có."""
    if not description:
        return ""
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    emails = re.findall(email_pattern, description)
    return emails[0] if emails else ""


def download_image(url, filename):
    """Tải ảnh từ URL và lưu vào thư mục."""
    try:
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(filename, 'wb') as f:
                for chunk in response.iter_content(1024):
                    f.write(chunk)
            return filename
        else:
            return ""
    except Exception as e:
        print(f"Lỗi tải ảnh {url}: {e}")
        return ""


def get_channel_details(channel_ids):
    detailed_channels = []
    for i in range(0, len(channel_ids), 50):
        batch_ids = channel_ids[i:i+50]
        request = youtube.channels().list(
            part="snippet,statistics,topicDetails,brandingSettings",
            id=",".join(batch_ids)
        )
        try:
            response = request.execute()
            today_str = datetime.now().strftime('%d-%m-%Y')
            save_avatar_dir = os.path.join(os.path.join(
                IMAGE_DIR_CHANNEL, 'avatars'), today_str)
            # Tạo thư mục nếu chưa tồn tại
            os.makedirs(save_avatar_dir, exist_ok=True)
            save_banner_dir = os.path.join(os.path.join(
                IMAGE_DIR_CHANNEL, 'banners'), today_str)
            os.makedirs(save_banner_dir, exist_ok=True)
            for item in response.get("items", []):
                channel_id = item["id"]
                # Lấy URL avatar (chọn kích thước default hoặc high nếu có)
                thumbnail_url = item["snippet"]["thumbnails"].get(
                    "default", {}).get("url", "")
                if thumbnail_url != "":
                    thumbnail_file = os.path.join(
                        save_avatar_dir, f"{channel_id}_avatar.jpg")
                    thumbnail_saved = download_image(
                        thumbnail_url, thumbnail_file)
                else:
                    thumbnail_saved = ""

                # Lấy URL banner (kiểm tra an toàn trường image)
                banner_url = ""
                if item["brandingSettings"].get("image"):
                    banner_url = item["brandingSettings"]["image"].get(
                        "bannerExternalUrl", "")
                if banner_url != "":
                    banner_file = os.path.join(
                        save_banner_dir, f"{channel_id}_banner.jpg")
                    banner_saved = download_image(banner_url, banner_file)
                else:
                    banner_saved = ""

                channel_info = {
                    "channelId": channel_id,
                    "title": item["snippet"]["title"],
                    "description": item["snippet"]["description"],
                    "publishedAt": datetime.fromisoformat(item["snippet"]["publishedAt"].replace("Z", "+00:00")),
                    "country": item["snippet"].get("country", ""),
                    "subscriberCount": int(item["statistics"].get("subscriberCount", 0)) if item["statistics"].get("subscriberCount") else 0,
                    "videoCount": int(item["statistics"].get("videoCount", 0)) if item["statistics"].get("videoCount") else 0,
                    "viewCount": int(item["statistics"].get("viewCount", 0)) if item["statistics"].get("viewCount") else 0,
                    "topics": ",".join(item["topicDetails"].get("topicIds", []) if item.get("topicDetails") else []),
                    "email": extract_email(item["brandingSettings"]["channel"].get("description", "") or item["snippet"]["description"]),
                    "avatarUrl": thumbnail_url,
                    "avatarFile": thumbnail_saved,
                    "bannerUrl": banner_url,
                    "bannerFile": banner_saved
                }
                # Lưu vào MongoDB
                mongo_collection_channels.insert_one(channel_info)
                detailed_channels.append(channel_info)
        except googleapiclient.errors.HttpError as e:
            print(f"Lỗi API khi lấy chi tiết kênh: {e}")
    return detailed_channels


def get_thumbnail_video(all_videos):
    count_success = 0
    count_fail = 0
    today_str = datetime.now().strftime('%d-%m-%Y')
    save_thumbnails_dir = os.path.join(IMAGE_DIR_VIDEO, today_str)
    # Tạo thư mục nếu chưa tồn tại
    os.makedirs(save_thumbnails_dir, exist_ok=True)
    for video in all_videos:
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
            save_path = os.path.join(save_thumbnails_dir, filename)

            # Lưu ảnh
            with open(save_path, "wb") as f:
                f.write(response.content)
            count_success = count_success + 1
            print(f"✅ Đã lưu: {save_path}")

        except Exception as e:
            print(f"❌ Lỗi với {video_id}: {str(e)}")
            count_fail = count_fail + 1

    print(f"Đã tải thành công {count_success}, tải thất bại {count_fail}")
    return count_success

def save_keyword_data(keyword):
    result = get_channels_and_videos_by_query(keyword)
    if result is None:
        print(f"Lỗi: Không lấy được kết quả cho từ khóa '{keyword}'")
        return

    new_channels = result.get("channels", [])
    new_videos = result.get("videos", [])

    existing_doc = mongo_collection_keyword.find_one({"keyword": keyword})

    if existing_doc:
        existing_channels = existing_doc.get("channels", [])
        existing_videos = existing_doc.get("videos", [])

        existing_channel_ids = {ch["channelId"] for ch in existing_channels}
        existing_video_ids = {v["videoId"] for v in existing_videos}

        # Lọc các phần tử mới chưa có
        new_unique_channels = [ch for ch in new_channels if ch["channelId"] not in existing_channel_ids]
        new_unique_videos = [v for v in new_videos if v["videoId"] not in existing_video_ids]

        # Ghép danh sách mới với cũ
        updated_channels = existing_channels + new_unique_channels
        updated_videos = existing_videos + new_unique_videos

        # Cập nhật lại toàn bộ
        mongo_collection_keyword.update_one(
            {"keyword": keyword},
            {
                "$set": {
                    "channels": updated_channels,
                    "videos": updated_videos,
                    "count_channels": len(updated_channels),
                    "count_videos": len(updated_videos),
                    "time": datetime.now()
                }
            }
        )
        print(f"Đã cập nhật keyword '{keyword}': Tổng {len(updated_channels)} channels, {len(updated_videos)} videos.")
    else:
        result["keyword"] = keyword
        result["time"] = datetime.now()
        result["count_channels"] = len(new_channels)
        result["count_videos"] = len(new_videos)
        mongo_collection_keyword.insert_one(result)
        print(f"Đã thêm keyword mới '{keyword}': {len(new_channels)} channels, {len(new_videos)} videos.")

def handle_crawl_data_by_keyword(keyword):
    result = get_channels_and_videos_by_query(keyword)
    # Lấy thông tin chi tiết profile
    channel_ids = [c["channelId"] for c in result["channels"]]
    detailed_channels = get_channel_details(channel_ids)
    count_channels = len(detailed_channels)
    # lấy thumnail videos
    count_thumbnails = get_thumbnail_video(result["videos"])
    count_thumbnails = len(result["videos"])

    return {
        "keyword": keyword,
        "count_channels": count_channels,
        "count_thumbnails": count_thumbnails,
    }


def save_crawl_result(result, file_path="crawl_result.json"):
    result["time"] = datetime.now()

    # Nếu file chưa tồn tại, tạo mới
    if not os.path.exists(file_path):
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump([result], f, ensure_ascii=False, indent=4)
        return

    # Nếu file đã tồn tại, đọc nội dung cũ
    with open(file_path, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            data = []

    # Kiểm tra keyword đã tồn tại chưa
    keywords = [entry["keyword"] for entry in data]
    if result["keyword"] not in keywords:
        data.append(result)
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)


def crawl_data_main (key): 
    # Chạy nhiều truy vấn để tăng số lượng kênh
    with open('keywords.txt', 'r', encoding='utf-8') as f:
        queries = [line.strip() for line in f if line.strip()]

    for query in queries:
        if key == 'savekeyword':
            save_keyword_data(query)
        elif key == 'savealldata':
            data_crawl_result = handle_crawl_data_by_keyword(query)
            save_crawl_result(data_crawl_result)

crawl_data_main('savekeyword')
