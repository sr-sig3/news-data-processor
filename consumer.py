from kafka import KafkaConsumer
import json
import os
from dotenv import load_dotenv
import logging
import signal
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class NewsConsumer:
    def __init__(self):
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.topic = os.getenv('KAFKA_TOPIC', 'news-topic')
        self.group_id = os.getenv('KAFKA_GROUP_ID', 'news-data-processor-group')
        
        # Initialize Kafka consumer
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            enable_auto_commit=True,
            auto_commit_interval_ms=5000
        )
        
        logger.info(f"Initialized Kafka consumer for topic: {self.topic}")

    def process_message(self, message):
        try:
            # Log the received message
            logger.info(f"Received message: {message.value}")
            
            # TODO: Add your message processing logic here
            
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")

    def start_consuming(self):
        logger.info("Starting to consume messages...")
        try:
            for message in self.consumer:
                self.process_message(message)
        except KeyboardInterrupt:
            logger.info("Stopping consumer...")
        finally:
            self.consumer.close()

def signal_handler(sig, frame):
    logger.info("Received shutdown signal")
    sys.exit(0)

if __name__ == "__main__":
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start consumer
    consumer = NewsConsumer()
    consumer.start_consuming() 