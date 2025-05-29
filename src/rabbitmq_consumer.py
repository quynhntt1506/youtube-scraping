import pika
import json
from typing import Dict, Any, List
from src.utils.logger import CustomLogger
from src.controller.crawler_by_request import *
from src.controller.send_to_data_controller import *

logger = CustomLogger("rabbitmq_consumer")

class RabbitMQConsumer:
    def __init__(self, host: str = 'localhost', queues: List[str] = None):
        """Initialize RabbitMQ consumer.
        
        Args:
            host (str): RabbitMQ host
            queues (List[str]): List of queue names to listen to
        """
        self.host = host
        self.queues = queues or [
            'youtube.video.info.crawler.queue',
            'youtube.channel.info.crawler.queue'
        ]
        self.connection = None
        self.channel = None
        self.logger = CustomLogger("rabbitmq_consumer")
        
    def connect(self):
        """Connect to RabbitMQ server."""
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            self.channel = self.connection.channel()
            
            # Declare all queues
            for queue in self.queues:
                self.channel.queue_declare(queue=queue, durable=True)
                
            self.logger.info(f"Connected to RabbitMQ server at {self.host}")
            self.logger.info(f"Listening to queues: {', '.join(self.queues)}")
            
        except Exception as e:
            self.logger.error(f"Error connecting to RabbitMQ: {str(e)}")
            raise
            
    def process_message(self, ch, method, properties, body):
        """Process received message.
        
        Args:
            ch: Channel
            method: Delivery method
            properties: Message properties
            body: Message body
        """
        try:
            # Parse message body
            message = json.loads(body.decode('utf-8'))
            self.logger.info(f"Received message from queue {method.routing_key}: {message}")
            
            action = message.get("action")
            if action == "CHANNEL_INFO":
                channel_id = message.get("body", {}).get("channelId")
                custom_urls = message.get("body", {}).get("customUrl")
                
                if not channel_id and not custom_urls:
                    self.logger.error("No channel_id or custom_urls found in message")
                    ch.basic_nack(delivery_tag=method.delivery_tag)
                    return
                
                if channel_id:
                    self.logger.info(f"Processing channel: {channel_id}")
                    result = crawl_channel_by_id(channel_id)
                    self.logger.info(f"Crawled channel {channel_id}")
                elif custom_urls:
                    self.logger.info(f"Processing custom_urls: {custom_urls}")
                    result = crawl_channel_by_custom_urls(custom_urls)
                    self.logger.info(f"Crawled channel {custom_urls}")
                    
            elif action == "VIDEO_INFO":
                video_id = message.get("body", {}).get("videoId")
                url = message.get("body", {}).get("url")
                if not video_id and not url:
                    self.logger.error("No video_id or url found in message")
                    ch.basic_nack(delivery_tag=method.delivery_tag)
                    return
                if video_id:
                    self.logger.info(f"Processing video: {video_id}")
                    result = crawl_video_by_ids(video_id)
                    self.logger.info(f"Crawled video {video_id}")
                elif url:
                    self.logger.info(f"Processing video: {url}")
                    result = crawl_video_by_urls(url)
                    self.logger.info(f"Crawled video {url}")
            # Acknowledge message
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        except json.JSONDecodeError as e:
            self.logger.error(f"Error decoding message: {str(e)}")
            # ch.basic_nack(delivery_tag=method.delivery_tag)
        except Exception as e:
            self.logger.error(f"Error processing message: {str(e)}")
            # ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            
    def start_consuming(self):
        """Start consuming messages from all queues."""
        try:
            # Set up consumer for each queue
            for queue in self.queues:
                self.channel.basic_consume(
                    queue=queue,
                    on_message_callback=self.process_message,
                    auto_ack=False
                )
            
            self.logger.info("Started consuming messages")
            self.channel.start_consuming()
            
        except Exception as e:
            self.logger.error(f"Error consuming messages: {str(e)}")
            if self.connection and not self.connection.is_closed:
                self.connection.close()

def main():
    """Main function to run the consumer."""
    consumer = RabbitMQConsumer()
    try:
        consumer.connect()
        consumer.start_consuming()
    except KeyboardInterrupt:
        consumer.logger.info("Stopping consumer...")
        if consumer.connection and not consumer.connection.is_closed:
            consumer.connection.close()
    except Exception as e:
        consumer.logger.error(f"Error in main: {str(e)}")
        if consumer.connection and not consumer.connection.is_closed:
            consumer.connection.close()

if __name__ == "__main__":
    main() 