from kafka import KafkaConsumer
import json
from typing import Dict, Any
import structlog
from database import Database
from keyword_extractor import KeywordExtractor

logger = structlog.get_logger()

class NewsConsumer:
    def __init__(self, bootstrap_servers: str, topic: str, group_id: str):
        logger.info("initializing_consumer",
                   bootstrap_servers=bootstrap_servers,
                   topic=topic,
                   group_id=group_id)
        
        try:
            self.consumer = KafkaConsumer(
                bootstrap_servers=bootstrap_servers,
                group_id=group_id,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            logger.info("consumer_initialized_successfully")
            
            # 토픽 목록 확인
            topics = self.consumer.topics()
            logger.info("available_topics", topics=list(topics))
            
            # 파티션 정보 확인
            partitions = self.consumer.partitions_for_topic(topic)
            logger.info("topic_partitions", partitions=partitions)
            
            # 토픽 구독
            self.consumer.subscribe([topic])
            logger.info("subscribed_to_topic", topic=topic)
            
        except Exception as e:
            logger.error("consumer_initialization_failed", error=str(e))
            raise
            
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
                logger.info("received_message",
                          topic=message.topic,
                          partition=message.partition,
                          offset=message.offset)
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
    
    BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'news-stream-broker-kafka-1:9092')
    TOPIC = os.getenv('KAFKA_TOPIC', 'news-topic')
    GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'news-processor')
    
    logger.info("starting_application",
                bootstrap_servers=BOOTSTRAP_SERVERS,
                topic=TOPIC,
                group_id=GROUP_ID)
    
    consumer = NewsConsumer(BOOTSTRAP_SERVERS, TOPIC, GROUP_ID)
    consumer.consume() 