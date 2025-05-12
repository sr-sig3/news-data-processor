import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from sklearn.feature_extraction.text import TfidfVectorizer
import pandas as pd
import numpy as np
from typing import List, Dict, Any
import structlog

logger = structlog.get_logger()

class KeywordExtractor:
    def __init__(self):
        # NLTK 초기화
        nltk.download('punkt')
        nltk.download('stopwords')
        nltk.download('wordnet')
        
        self.stop_words = set(stopwords.words('english'))
        self.lemmatizer = WordNetLemmatizer()
        self.vectorizer = TfidfVectorizer(
            max_features=1000,
            stop_words='english',
            ngram_range=(1, 2)  # 단일 단어와 2-gram 모두 고려
        )
        
    def preprocess_text(self, text: str) -> str:
        """텍스트 전처리"""
        # 소문자 변환
        text = text.lower()
        
        # 토큰화
        tokens = word_tokenize(text)
        
        # 불용어 제거 및 표제어 추출
        processed_tokens = [
            self.lemmatizer.lemmatize(token)
            for token in tokens
            if token.isalnum() and token not in self.stop_words
        ]
        
        return ' '.join(processed_tokens)
    
    def extract_keywords(self, news_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """뉴스 데이터에서 키워드 추출"""
        try:
            # 입력 데이터 로깅
            logger.info("extracting_keywords_from_data",
                       news_id=news_data['id'],
                       title=news_data.get('title', ''),
                       description=news_data.get('description', ''),
                       content=news_data.get('content', ''))
            
            # 제목과 본문을 결합하여 분석
            combined_text = f"{news_data['title']} {news_data.get('description', '')}"
            processed_text = self.preprocess_text(combined_text)
            
            # 전처리된 텍스트 로깅
            logger.info("processed_text", text=processed_text)
            
            # TF-IDF 벡터화
            tfidf_matrix = self.vectorizer.fit_transform([processed_text])
            feature_names = self.vectorizer.get_feature_names_out()
            
            # TF-IDF 점수가 높은 상위 키워드 추출
            tfidf_scores = tfidf_matrix.toarray()[0]
            top_indices = np.argsort(tfidf_scores)[-10:][::-1]  # 상위 10개 키워드
            
            # 점수 정보 로깅
            logger.info("tfidf_scores_info",
                       max_score=float(np.max(tfidf_scores)),
                       min_score=float(np.min(tfidf_scores)),
                       mean_score=float(np.mean(tfidf_scores)))
            
            keywords = []
            for idx in top_indices:
                if tfidf_scores[idx] > 0:  # 0보다 큰 점수만 고려
                    keywords.append({
                        'keyword': feature_names[idx],
                        'score': float(tfidf_scores[idx]),
                        'news_id': news_data['id']
                    })
            
            logger.info("keywords_extracted", 
                       news_id=news_data['id'],
                       keyword_count=len(keywords),
                       keywords=keywords)
            
            return keywords
            
        except Exception as e:
            logger.error("keyword_extraction_failed",
                        news_id=news_data['id'],
                        error=str(e),
                        error_type=type(e).__name__)
            return [] 