import json
from kafka import KafkaProducer
from src.config.config import KAFKA_BOOTSTRAP_SERVERS


def send_to_kafka(topic: str, message: dict, bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS):
    """
    Gửi message (dạng dict/json) vào Kafka topic.
    Args:
        topic (str): Tên topic Kafka.
        message (dict): Dữ liệu gửi đi (dạng dict/json).
        bootstrap_servers (str): Địa chỉ Kafka broker.
    """
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    json_message = json.dumps(message).encode('utf-8')
    producer.send(topic, json_message)
    producer.flush()
    producer.close()

channel_data = {
  "channelId": "UCydsSDQJvn5GdUg5bgt2eUg",
  "description": "Chào mừng các \"không ai cả\" đây là kênh không người NHÌN của Samurice, dành cho các khán giả thích nghe hơn là xem.\nSamurice là một người kể chuyện và trong kênh Youtube này các \"không ai cả\" sẽ được nghe những câu chuyện về cuộc sống, con người và chữa lành...\n\nTham gia gói membership Ai Đó và Pha Chế Cà Phê để bình chọn nội dung trong tương lai nhé các ông!\nhttps://www.youtube.com/channel/UCY5zoNtJui5oCII88MuMacw/join\n\nGroup Không Người: https://fb.com/groups/khongnguoi\nSAMURICE: https://youtube.com/@thesamurice\nĐẠT GẠO: https://youtube.com/@datgao\nĐỌC BÁO HỘ: https://youtube.com/@docbaoho\n",
  "publishedAt":  "2023-09-26T02:31:51.000Z",
  "status": "crawled_video",
  "title": "SAMURICE - Podcast trên mạng",
  "avatarUrl": "https://yt3.ggpht.com/J9o_UlAepKKtmc0JGTsilOzFuUquclgCUPy_QJiQCUNFKrY197x_AvCJm_E5p0nqKR-7yBEX=s88-c-k-c0x00ffffff-no-rj",
  "bannerUrl": "https://yt3.googleusercontent.com/sxiSnExPhaFVIGD9dTiO7Ou5y0dr-tDMFb3x-HBMq12S3EzzU7AyPhVAa7AZ5YpeeAkKYLeE",
  "country": "VN",
  "crawlDate": "2025-05-09T11:33:50.560Z",
  "customUrl": "@samuriceaudio",
  "playlistId": "UUydsSDQJvn5GdUg5bgt2eUg",
  "subscriberCount": 2730,
  "topics": [],
  "videoCount": 41,
  "viewCount": 47110
}
send_to_kafka("youtube.channel.crawler.raw", channel_data)