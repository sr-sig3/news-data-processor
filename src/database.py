from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, Float, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
import os
from dotenv import load_dotenv
import structlog

logger = structlog.get_logger()

# 환경 변수 로드
load_dotenv()

# 데이터베이스 연결 설정
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://user:password@localhost:5432/news_db')
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

class News(Base):
    __tablename__ = "news"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String, index=True)
    content = Column(Text)
    source = Column(String)
    url = Column(String, unique=True)
    published_at = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # 키워드와의 관계
    keywords = relationship("Keyword", back_populates="news")

class Keyword(Base):
    __tablename__ = "keywords"

    id = Column(Integer, primary_key=True, index=True)
    keyword = Column(String, index=True)
    score = Column(Float)
    news_id = Column(Integer, ForeignKey("news.id"))
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # 뉴스와의 관계
    news = relationship("News", back_populates="keywords")

class Database:
    def __init__(self):
        self.engine = engine
        self.SessionLocal = SessionLocal
        self.create_tables()
        
    def create_tables(self):
        """테이블 생성"""
        Base.metadata.create_all(bind=self.engine)
        
    def save_news(self, news_data: dict):
        """뉴스 데이터 저장"""
        session = self.SessionLocal()
        try:
            news = News(
                title=news_data['title'],
                content=news_data['content'],
                source=news_data['source'],
                url=news_data['url'],
                published_at=news_data['published_at']
            )
            session.add(news)
            session.commit()
            session.refresh(news)
            logger.info("news_saved", news_id=news.id)
            return news.id
        except Exception as e:
            session.rollback()
            logger.error("news_save_failed", error=str(e))
            raise
        finally:
            session.close()
            
    def save_keywords(self, keywords: list):
        """키워드 데이터 저장"""
        session = self.SessionLocal()
        try:
            for keyword_data in keywords:
                keyword = Keyword(
                    keyword=keyword_data['keyword'],
                    score=keyword_data['score'],
                    news_id=keyword_data['news_id']
                )
                session.add(keyword)
            session.commit()
            logger.info("keywords_saved", count=len(keywords))
        except Exception as e:
            session.rollback()
            logger.error("keywords_save_failed", error=str(e))
            raise
        finally:
            session.close()
            
    def get_news_by_id(self, news_id: int):
        """ID로 뉴스 조회"""
        session = self.SessionLocal()
        try:
            news = session.query(News).filter(News.id == news_id).first()
            return news
        finally:
            session.close()
            
    def get_keywords_by_news_id(self, news_id: int):
        """뉴스 ID로 키워드 조회"""
        session = self.SessionLocal()
        try:
            keywords = session.query(Keyword).filter(Keyword.news_id == news_id).all()
            return keywords
        finally:
            session.close() 