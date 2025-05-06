from database import Database
import structlog

logger = structlog.get_logger()

def test_connection():
    try:
        # 데이터베이스 연결 시도
        db = Database()
        
        # 테이블 생성 확인
        db.create_tables()
        
        logger.info("database_connection_successful")
        print("데이터베이스 연결 성공!")
        print("테이블이 성공적으로 생성되었습니다.")
        
    except Exception as e:
        logger.error("database_connection_failed", error=str(e))
        print(f"데이터베이스 연결 실패: {str(e)}")

if __name__ == "__main__":
    test_connection() 