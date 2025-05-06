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
                enable_auto_commit=False,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=1000
            )
            logger.info("consumer_initialized_successfully")
            
            # 토픽 구독
            self.consumer.subscribe([topic])
            logger.info("subscribed_to_topic", topic=topic)
            
            # 토픽 목록 확인
            topics = self.consumer.topics()
            logger.info("available_topics", topics=list(topics))
            
            if topic not in topics:
                logger.error("topic_not_found", topic=topic)
                return
                
            # 파티션 정보 확인
            partition_ids = self.consumer.partitions_for_topic(topic)
            logger.info("topic_partitions", partitions=partition_ids)
            
        except Exception as e:
            logger.error("consumer_initialization_failed", error=str(e))
            raise
            
        self.db = Database()
        self.keyword_extractor = KeywordExtractor()
        
    def process_message(self, message: Dict[str, Any]):
        """메시지 처리"""
        try:
            logger.info("processing_message", message=message.value)
            
            # 뉴스 데이터 저장
            news_id = self.db.save_news(message.value)
            logger.info("news_saved", news_id=news_id)
            
            # 키워드 추출
            keywords = self.keyword_extractor.extract_keywords({
                **message.value,
                'id': news_id
            })
            logger.info("keywords_extracted", keywords=keywords)
            
            # 키워드 저장
            if keywords:
                self.db.save_keywords(keywords)
                logger.info("keywords_saved", keyword_count=len(keywords))
                
            logger.info("message_processed",
                       news_id=news_id,
                       keyword_count=len(keywords))
            
            # 메시지 처리 후 수동으로 커밋
            self.consumer.commit()
            logger.info("message_committed", 
                       topic=message.topic,
                       partition=message.partition,
                       offset=message.offset)
            
        except Exception as e:
            logger.error("message_processing_failed",
                        error=str(e),
                        error_type=type(e).__name__)
            raise
            
    def consume(self):
        """메시지 소비 시작"""
        logger.info("starting_consumer")
        
        try:
            while True:
                try:
                    # 메시지 폴링 (1초 타임아웃)
                    messages = self.consumer.poll(timeout_ms=1000)
                    
                    for tp, msgs in messages.items():
                        for message in msgs:
                            logger.info("received_message",
                                      topic=message.topic,
                                      partition=message.partition,
                                      offset=message.offset)
                            self.process_message(message)
                            
                except Exception as e:
                    logger.error("message_consumption_failed", error=str(e))
                    continue
                
        except KeyboardInterrupt:
            logger.info("consumer_stopped")
        finally:
            self.consumer.close()

if __name__ == "__main__":
    # 환경 변수에서 설정 로드
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    TOPIC = os.getenv('KAFKA_TOPIC', 'news-topic')
    GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'news-processor')
    
    logger.info("starting_application",
                bootstrap_servers=BOOTSTRAP_SERVERS,
                topic=TOPIC,
                group_id=GROUP_ID)
    
    consumer = NewsConsumer(BOOTSTRAP_SERVERS, TOPIC, GROUP_ID)
    consumer.consume() 