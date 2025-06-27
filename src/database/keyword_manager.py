from typing import List, Optional, Dict
from datetime import datetime
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from src.config.config import POSTGRE_CONFIG
from src.models.keyword_search import KeywordSearch
from src.utils.logger import CustomLogger

class KeywordManager:
    def __init__(self):
        self.logger = CustomLogger("keyword_manager")
        self.engine = None
        self.SessionLocal = None
        self._init_database()

    def _init_database(self):
        try:
            database_url = f"postgresql://{POSTGRE_CONFIG['username']}:{POSTGRE_CONFIG['password']}@{POSTGRE_CONFIG['host']}:{POSTGRE_CONFIG['port']}/{POSTGRE_CONFIG['database']}"
            self.engine = create_engine(database_url)
            self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
            with self.engine.connect() as conn:
                conn.execute("SELECT 1")
            self.logger.info("PostgreSQL connection established successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize PostgreSQL connection: {e}")
            raise

    def add_keyword(self, keyword: str, status: str, type_: str) -> Optional[KeywordSearch]:
        try:
            with self.SessionLocal() as session:
                existing = session.query(KeywordSearch).filter(KeywordSearch.keyword == keyword).first()
                if existing:
                    self.logger.info(f"Keyword already exists: {keyword}")
                    return existing
                new_kw = KeywordSearch(
                    keyword=keyword,
                    status=status,
                    type=type_,
                    last_update=datetime.utcnow()
                )
                session.add(new_kw)
                session.commit()
                session.refresh(new_kw)
                self.logger.info(f"Added new keyword: {keyword}")
                return new_kw
        except SQLAlchemyError as e:
            self.logger.error(f"Database error adding keyword: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error adding keyword: {e}")
            return None

    def update_keyword(self, keyword: str, status: Optional[str] = None, type_: Optional[str] = None) -> bool:
        try:
            with self.SessionLocal() as session:
                kw = session.query(KeywordSearch).filter(KeywordSearch.keyword == keyword).first()
                if not kw:
                    self.logger.warning(f"Keyword not found: {keyword}")
                    return False
                if status:
                    kw.status = status
                if type_:
                    kw.type = type_
                kw.last_update = datetime.utcnow()
                session.commit()
                self.logger.info(f"Updated keyword: {keyword}")
                return True
        except SQLAlchemyError as e:
            self.logger.error(f"Database error updating keyword: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Error updating keyword: {e}")
            return False

    def get_keywords(self, status: Optional[str] = None, type_: Optional[str] = None) -> List[Dict]:
        try:
            with self.SessionLocal() as session:
                query = session.query(KeywordSearch)
                if status:
                    query = query.filter(KeywordSearch.status == status)
                if type_:
                    query = query.filter(KeywordSearch.type == type_)
                keywords = query.all()
                return [kw.to_dict() for kw in keywords]
        except SQLAlchemyError as e:
            self.logger.error(f"Database error getting keywords: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Error getting keywords: {e}")
            return []

    def get_keyword_by_keyword(self, keyword: str) -> Optional[Dict]:
        """
        Get a specific keyword by its text value.
        
        Args:
            keyword (str): The keyword text to find
            
        Returns:
            Optional[Dict]: Keyword document if found, None otherwise
        """
        try:
            with self.SessionLocal() as session:
                kw = session.query(KeywordSearch).filter(KeywordSearch.keyword == keyword).first()
                if kw:
                    return kw.to_dict()
                return None
        except SQLAlchemyError as e:
            self.logger.error(f"Database error getting keyword by keyword: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error getting keyword by keyword: {e}")
            return None

    def get_keyword_stats(self) -> Dict[str, int]:
        try:
            with self.SessionLocal() as session:
                total = session.query(KeywordSearch).count()
                by_status = session.query(KeywordSearch.status, func.count(KeywordSearch.id)).group_by(KeywordSearch.status).all()
                by_type = session.query(KeywordSearch.type, func.count(KeywordSearch.id)).group_by(KeywordSearch.type).all()
                stats = {
                    "total": total,
                    "by_status": {s: c for s, c in by_status},
                    "by_type": {t: c for t, c in by_type}
                }
                return stats
        except SQLAlchemyError as e:
            self.logger.error(f"Database error getting keyword stats: {e}")
            return {}
        except Exception as e:
            self.logger.error(f"Error getting keyword stats: {e}")
            return {}

    def close(self):
        if self.engine:
            self.engine.dispose()
            self.logger.info("PostgreSQL connection closed") 