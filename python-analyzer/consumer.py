from kafka import KafkaConsumer
import json
import os
from dotenv import load_dotenv
from analyzer.text_analyzer import TextAnalyzer
from analyzer.database import Database
import logging

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
        self.analyzer = TextAnalyzer()
        self.db = Database()
        
        # Initialize Kafka consumer
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        logger.info(f"Initialized Kafka consumer for topic: {self.topic}")

    def process_message(self, message):
        try:
            # Extract news content from message
            news_content = message.value.get('content', '')
            if not news_content:
                logger.warning("Received empty news content")
                return

            # Perform text analysis
            analysis_result = self.analyzer.analyze(news_content)
            
            # Save analysis result to database
            self.db.save_analysis(news_content, analysis_result)
            
            # Log analysis result
            logger.info(f"Analysis completed and saved for news: {analysis_result}")
            
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")

    def start_consuming(self):
        logger.info("Starting to consume messages...")
        try:
            for message in self.consumer:
                logger.info(f"Received message: {message.value}")
                self.process_message(message)
        except KeyboardInterrupt:
            logger.info("Stopping consumer...")
        finally:
            self.consumer.close()

if __name__ == "__main__":
    consumer = NewsConsumer()
    consumer.start_consuming() 