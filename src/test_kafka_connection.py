from kafka import KafkaConsumer
import structlog
import os
from dotenv import load_dotenv

logger = structlog.get_logger()

def test_kafka_connection():
    try:
        # 환경 변수 로드
        load_dotenv()
        
        # Kafka 설정
        bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'news-stream-broker-kafka-1:9092')
        topic = os.getenv('KAFKA_TOPIC', 'news-topic')
        group_id = os.getenv('KAFKA_GROUP_ID', 'news-processor')
        
        logger.info("attempting_kafka_connection",
                   bootstrap_servers=bootstrap_servers,
                   topic=topic,
                   group_id=group_id)
        
        # Kafka Consumer 생성
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset='earliest',
            consumer_timeout_ms=5000  # 5초 후 타임아웃
        )
        
        # 토픽 목록 확인
        topics = consumer.topics()
        logger.info("available_topics", topics=topics)
        
        # 메시지 수신 시도
        for message in consumer:
            logger.info("message_received", 
                       topic=message.topic,
                       partition=message.partition,
                       offset=message.offset)
            
        logger.info("kafka_connection_successful")
        
    except Exception as e:
        logger.error("kafka_connection_failed", error=str(e))
        raise

if __name__ == "__main__":
    test_kafka_connection() 