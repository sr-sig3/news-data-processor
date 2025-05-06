from kafka import KafkaConsumer
import json
from typing import Dict, Any
import structlog
from database import Database
from keyword_extractor import KeywordExtractor

logger = structlog.get_logger()

class NewsConsumer:
    def __init__(self, bootstrap_servers: str, topic: str, group_id: str):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        self.db = Database()
        self.keyword_extractor = KeywordExtractor()
        
    def process_message(self, message: Dict[str, Any]):
        """메시지 처리"""
        try:
            # 뉴스 데이터 저장
            news_id = self.db.save_news(message)
            
            # 키워드 추출
            keywords = self.keyword_extractor.extract_keywords({
                **message,
                'id': news_id
            })
            
            # 키워드 저장
            if keywords:
                self.db.save_keywords(keywords)
                
            logger.info("message_processed",
                       news_id=news_id,
                       keyword_count=len(keywords))
            
        except Exception as e:
            logger.error("message_processing_failed",
                        error=str(e))
            
    def consume(self):
        """메시지 소비 시작"""
        logger.info("starting_consumer")
        
        try:
            for message in self.consumer:
                self.process_message(message.value)
                
        except KeyboardInterrupt:
            logger.info("consumer_stopped")
        finally:
            self.consumer.close()

if __name__ == "__main__":
    # 환경 변수에서 설정 로드
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    TOPIC = os.getenv('KAFKA_TOPIC', 'news')
    GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'news-processor')
    
    consumer = NewsConsumer(BOOTSTRAP_SERVERS, TOPIC, GROUP_ID)
    consumer.consume() 