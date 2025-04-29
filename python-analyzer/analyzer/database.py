import sqlite3
import json
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class Database:
    def __init__(self, db_path='news_analysis.db'):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS news_analysis (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    original_content TEXT NOT NULL,
                    analysis_result TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.commit()

    def save_analysis(self, original_content, analysis_result):
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO news_analysis (original_content, analysis_result)
                    VALUES (?, ?)
                ''', (original_content, json.dumps(analysis_result, ensure_ascii=False)))
                conn.commit()
                logger.info("Analysis result saved successfully")
        except Exception as e:
            logger.error(f"Error saving analysis result: {str(e)}")
            raise

    def get_recent_analyses(self, limit=10):
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT id, original_content, analysis_result, created_at
                    FROM news_analysis
                    ORDER BY created_at DESC
                    LIMIT ?
                ''', (limit,))
                return cursor.fetchall()
        except Exception as e:
            logger.error(f"Error fetching recent analyses: {str(e)}")
            return [] 