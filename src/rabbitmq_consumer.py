import pika
import json
from typing import Dict, Any
from src.utils.logger import CustomLogger
from src.controller.crawler_by_request import *
from src.controller.send_to_data_controller import *

logger = CustomLogger("rabbitmq_consumer")

class RabbitMQConsumer:
    def __init__(self, host: str = 'localhost', queue_name: str = 'youtube.video.info.crawler.queue'):
        """Initialize RabbitMQ consumer.
        
        Args:
            host (str): RabbitMQ host address
            queue_name (str): Name of the queue to consume from
        """
        self.host = host
        self.queue_name = queue_name
        self.connection = None
        self.channel = None
        
    def connect(self):
        """Establish connection to RabbitMQ server."""
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=self.host)
            )
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue_name, durable=True)
            logger.info(f"Connected to RabbitMQ at {self.host}")
        except Exception as e:
            logger.error(f"Error connecting to RabbitMQ: {str(e)}")
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
            print(message)
            action = message.get("action")
            if action == "CHANNEL_INFO":
                channel_id = message.get("body", {}).get("channelId")
                custom_urls = message.get("body", {}).get("customUrl")
                
                if not channel_id and not custom_urls:
                    logger.error("No channel_id or custom_urls found in message")
                    ch.basic_nack(delivery_tag=method.delivery_tag)
                    return
                
                if channel_id:
                    logger.info(f"Processing channel: {channel_id}")
                    result = crawl_channel_by_id(channel_id)
                    # if result.get("new_channels"):
                    #     send_channel_to_data_controller(result.get("new_channels"))
                    logger.info(f"Crawled channel {channel_id}")
                elif custom_urls:
                    logger.info(f"Processing custom_urls: {custom_urls}")
                    result = crawl_channel_by_custom_urls(custom_urls)
                    # if result.get("new_channels"):
                    #     send_channel_to_data_controller(result.get("new_channels"))
                    logger.info(f"Crawled channel {custom_urls}")
            elif action == "VIDEO_INFO":
                video_id = message.get("body", {}).get("videoId")
                url = message.get("body", {}).get("url")
                if not video_id and not url:
                    logger.error("No video_id or url found in message")
                    ch.basic_nack(delivery_tag=method.delivery_tag)
                    return
                if video_id:
                    logger.info(f"Processing video: {video_id}")
                    result = crawl_video_by_ids(video_id)
                    # if result.get("new_videos"):
                    #     send_video_to_data_controller(result.get("new_videos"))
                    logger.info(f"Crawled video {video_id}")
                # elif url:
                #     logger.info(f"Processing video: {url}")
                #     result = crawl_video_by_url(url)
                #     logger.info(f"Crawled video {video_id if video_id else url}")
            # logger.info(f"Processing channel: {channel_id}")
            
            # # Crawl channel
            # result = crawl_channel_by_id(channel_id)
            # if result.get("new_channels"):
            #     send_channel_to_data_controller(result.get("new_channels"))
            
            # logger.info(f"Crawled channel {channel_id}")
            
            # Acknowledge message
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding message: {str(e)}")
            # ch.basic_nack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            # Reject message and requeue
            # ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            
    def start_consuming(self):
        """Start consuming messages from queue."""
        try:
            # Set prefetch count
            self.channel.basic_qos(prefetch_count=1)
            
            # Start consuming
            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=self.process_message
            )
            
            logger.info(f"Started consuming from queue: {self.queue_name}")
            self.channel.start_consuming()
            
        except Exception as e:
            logger.error(f"Error consuming messages: {str(e)}")
            raise
        finally:
            if self.connection and self.connection.is_open:
                self.connection.close()
                
def main():
    """Main function to run consumer."""
    try:
        # Initialize consumer
        consumer = RabbitMQConsumer()
        
        # Connect to RabbitMQ
        consumer.connect()
        
        # Start consuming
        consumer.start_consuming()
        
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
    except Exception as e:
        logger.error(f"Error running consumer: {str(e)}")
        
if __name__ == "__main__":
    main() 