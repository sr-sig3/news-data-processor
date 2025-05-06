from kafka import KafkaConsumer
import structlog
import os
from dotenv import load_dotenv

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
            auto_offset_reset='earliest',  # 가장 오래된 메시지부터 읽기
            enable_auto_commit=True,
            value_deserializer=lambda x: x.decode('utf-8')
        )
        
        # 사용 가능한 토픽 목록 확인
        topics = consumer.topics()
        logger.info("available_topics", topics=list(topics))
        
        if topic not in topics:
            logger.error("topic_not_found", topic=topic)
            return
        
        # 토픽 구독
        consumer.subscribe([topic])
        
        # 파티션 정보 확인
        partitions = consumer.partitions_for_topic(topic)
        logger.info("topic_partitions", partitions=partitions)
        
        # 메시지 수신 시도
        logger.info("attempting_to_receive_messages")
        for message in consumer:
            logger.info("received_message", 
                       topic=message.topic,
                       partition=message.partition,
                       offset=message.offset,
                       value=message.value)
            
    except Exception as e:
        logger.error("kafka_connection_error", error=str(e))
        raise

if __name__ == "__main__":
    test_kafka_connection() 