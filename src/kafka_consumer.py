import json
from kafka import KafkaProducer, KafkaConsumer
from src.config.config import KAFKA_BOOTSTRAP_SERVERS
from typing import Dict, Any
from src.utils.logger import CustomLogger
from src.controller.crawler_to_kafka import *

logger = CustomLogger("kafka_consumer")

class KafkaRequestConsumer:
    def __init__(self, bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS, topics=None):
        self.bootstrap_servers = bootstrap_servers
        print("KAFKA SERVER", bootstrap_servers)
        self.topics = topics or [
            'youtube.video.info.crawler.request',
            'youtube.channel.info.crawler.request'
        ]
        self.consumer = KafkaConsumer(
            *self.topics,
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='youtube-crawler-group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        self.logger = logger

    def process_message(self, topic, message: Dict[str, Any]):
        try:
            self.logger.info(f"Received message from topic {topic}: {message}")
            action = message.get("action")
            if action == "CHANNEL_INFO":
                channel_id = message.get("body", {}).get("channelId")
                custom_urls = message.get("body", {}).get("customUrl")
                if not channel_id and not custom_urls:
                    self.logger.error("No channel_id or custom_urls found in message")
                    return
                if channel_id:
                    self.logger.info(f"Processing channel: {channel_id}")
                    result = crawl_channel_by_id([channel_id])
                    self.logger.info(f"Crawled channel {channel_id}")
                elif custom_urls:
                    self.logger.info(f"Processing custom_urls: {custom_urls}")
                    result = crawl_channel_by_custom_urls([custom_urls])
                    self.logger.info(f"Crawled channel {custom_urls}")
            elif action == "VIDEO_INFO":
                video_id = message.get("body", {}).get("videoId")
                url = message.get("body", {}).get("url")
                if not video_id and not url:
                    self.logger.error("No video_id or url found in message")
                    return
                if video_id:
                    self.logger.info(f"Processing video: {video_id}")
                    result = crawl_video_by_ids([video_id])
                    self.logger.info(f"Crawled video {video_id}")
                
        except Exception as e:
            self.logger.error(f"Error processing message: {str(e)}")

    def start_consuming(self):
        self.logger.info(f"Started consuming topics: {', '.join(self.topics)}")
        for msg in self.consumer:
            self.process_message(msg.topic, msg.value)

def main():
    consumer = KafkaRequestConsumer()
    try:
        consumer.start_consuming()
    except KeyboardInterrupt:
        consumer.logger.info("Stopping consumer...")
    except Exception as e:
        consumer.logger.error(f"Error in main: {str(e)}")

if __name__ == "__main__":
    main()
