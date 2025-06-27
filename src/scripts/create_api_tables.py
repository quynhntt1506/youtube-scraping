from sqlalchemy import create_engine, text
from src.config.config import POSTGRE_CONFIG
from src.utils.logger import CustomLogger

def create_youtube_api_keys_table():
    """Tạo bảng youtube_api_keys và youtube_keywords với cấu trúc đơn giản."""
    logger = CustomLogger("create_tables")
    try:
        database_url = f"postgresql://{POSTGRE_CONFIG['username']}:{POSTGRE_CONFIG['password']}@{POSTGRE_CONFIG['host']}:{POSTGRE_CONFIG['port']}/{POSTGRE_CONFIG['database']}"
        engine = create_engine(database_url)
        # Dùng transaction context để tự động commit
        with engine.begin() as conn:
            # Tạo bảng youtube_api_keys
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS youtube_api_keys (
                    id SERIAL PRIMARY KEY,
                    email VARCHAR(255),
                    api_key VARCHAR(100) UNIQUE NOT NULL,
                    remaining_quota INTEGER NOT NULL DEFAULT 10000,
                    status VARCHAR(20) NOT NULL DEFAULT 'active',
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_youtube_api_keys_api_key ON youtube_api_keys(api_key)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_youtube_api_keys_status ON youtube_api_keys(status)"))
            # Tạo bảng youtube_keywords
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS youtube_keywords (
                    id SERIAL PRIMARY KEY,
                    keyword VARCHAR(255) UNIQUE NOT NULL,
                    status VARCHAR(50) NOT NULL,
                    type VARCHAR(50) NOT NULL,
                    last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_youtube_keywords_keyword ON youtube_keywords(keyword)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_youtube_keywords_status ON youtube_keywords(status)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_youtube_keywords_type ON youtube_keywords(type)"))
            logger.info("youtube_api_keys and youtube_keywords tables created successfully")
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
        raise

def create_all_tables():
    logger = CustomLogger("create_tables")
    try:
        create_youtube_api_keys_table()
        logger.info("All tables created successfully")
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
        raise

if __name__ == "__main__":
    create_all_tables() 