# News Data Processor

Python 기반의 뉴스 데이터 처리 시스템입니다. Kafka에서 뉴스 메시지를 consume하여 텍스트 분석을 수행하고 결과를 저장합니다.

## 주요 기능

- Kafka에서 뉴스 메시지 consume
- 텍스트 분석 (키워드 추출, 감성 분석, 핵심 구문 추출)
- 분석 결과 데이터베이스 저장

## 설치 및 실행

1. 필요한 패키지 설치:
```bash
pip install -r requirements.txt
```

2. 환경 변수 설정 (.env 파일 수정):
```
KAFKA_BOOTSTRAP_SERVERS=your-kafka-server:9092
KAFKA_TOPIC=news-topic
KAFKA_GROUP_ID=news-data-processor-group
```

3. 애플리케이션 실행:
```bash
python consumer.py
```

## 프로젝트 구조

```
news-data-processor/
├── consumer.py           # Kafka Consumer 및 메인 로직
├── requirements.txt      # Python 의존성
├── .env                  # 환경 변수 설정
└── analyzer/            # 분석 모듈
    ├── __init__.py
    ├── text_analyzer.py # 텍스트 분석 로직
    └── database.py      # 데이터베이스 처리
```
