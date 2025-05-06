from kafka import KafkaConsumer
import structlog
import os
from dotenv import load_dotenv
import json

logger = structlog.get_logger()

def test_kafka_connection():
    # 환경 변수 로드
    load_dotenv()
    
    # Kafka 설정
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'news-stream-broker-kafka-1:9092')
    topic = os.getenv('KAFKA_TOPIC', 'news-topic')
    group_id = os.getenv('KAFKA_GROUP_ID', 'news-processor')
    
    logger.info("attempting_to_connect_to_kafka", 
                bootstrap_servers=bootstrap_servers,
                topic=topic,
                group_id=group_id)
    
    try:
        # Kafka 컨슈머 생성
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=1000
        )
        
        # 사용 가능한 토픽 목록 확인
        topics = consumer.topics()
        logger.info("available_topics", topics=list(topics))
        
        if topic not in topics:
            logger.error("topic_not_found", topic=topic)
            return
        
        # 토픽 구독
        consumer.subscribe([topic])
        logger.info("subscribed_to_topic", topic=topic)
        
        # 파티션 정보 확인
        partitions = consumer.partitions_for_topic(topic)
        logger.info("topic_partitions", partitions=partitions)
        
        # 메시지 수신 시도
        logger.info("attempting_to_receive_messages")
        message_count = 0
        while message_count < 5:  # 최대 5개의 메시지만 받기
            try:
                for message in consumer:
                    logger.info("received_message", 
                              topic=message.topic,
                              partition=message.partition,
                              offset=message.offset,
                              value=message.value)
                    message_count += 1
                    if message_count >= 5:
                        break
            except Exception as e:
                logger.error("message_receive_error", error=str(e))
                break
                
        logger.info("test_completed", messages_received=message_count)
            
    except Exception as e:
        logger.error("kafka_connection_error", error=str(e))
        raise

if __name__ == "__main__":
    test_kafka_connection() 