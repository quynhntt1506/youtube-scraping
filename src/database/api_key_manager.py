from typing import Dict, Any, List, Optional
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from src.config.config import POSTGRE_CONFIG
from src.models.youtube_api_key import YouTubeApiKey
from src.utils.logger import CustomLogger

class APIKeyManager:
    """Manager class cho YouTube API keys sử dụng SQLAlchemy ORM."""
    
    def __init__(self):
        self.logger = CustomLogger("api_key_manager")
        self.engine = None
        self.SessionLocal = None
        self._init_database()

    def _init_database(self):
        """Khởi tạo kết nối PostgreSQL."""
        try:
            database_url = f"postgresql://{POSTGRE_CONFIG['username']}:{POSTGRE_CONFIG['password']}@{POSTGRE_CONFIG['host']}:{POSTGRE_CONFIG['port']}/{POSTGRE_CONFIG['database']}"
            
            self.engine = create_engine(database_url)
            self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute("SELECT 1")
            self.logger.info("PostgreSQL connection established successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize PostgreSQL connection: {e}")
            raise

    def _get_status(self, quota: int) -> str:
        """Get status based on remaining quota."""
        return "active" if quota > 0 else "unactive"

    def add_api_key(self, email: str, api_key: str, quota: int = 10000) -> Dict[str, Any]:
        """
        Add a new API key to the database.
        
        Args:
            email (str): Email associated with the API key
            api_key (str): The API key
            quota (int): Initial quota value (default: 10000)
            
        Returns:
            Dict[str, Any]: The created API key document
        """
        try:
            with self.SessionLocal() as session:
                # Kiểm tra xem API key đã tồn tại chưa
                existing = session.query(YouTubeApiKey).filter(YouTubeApiKey.api_key == api_key).first()
                
                if existing:
                    self.logger.warning(f"API key already exists for email: {email}")
                    return {}
                
                # Tạo API key mới
                new_key = YouTubeApiKey(
                    email=email,
                    api_key=api_key,
                    remaining_quota=quota,
                    status='active'
                )
                
                session.add(new_key)
                session.commit()
                session.refresh(new_key)
                
                api_key_doc = {
                    "id": new_key.id,
                    "api_key": new_key.api_key,
                    "remaining_quota": new_key.remaining_quota,
                    "status": new_key.status,
                    "last_updated": new_key.last_updated,
                }
                
                self.logger.info(f"Added new API key for email: {email}")
                return api_key_doc
                
        except SQLAlchemyError as e:
            self.logger.error(f"Database error adding API key: {e}")
            return {}
        except Exception as e:
            self.logger.error(f"Error adding API key: {e}")
            return {}

    def get_api_key(self, email: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Get an API key with remaining quota.
        If email is provided, get the specific API key for that email.
        
        Args:
            email (Optional[str]): Email to get specific API key
            
        Returns:
            Optional[Dict[str, Any]]: API key document or None if not found
        """
        try:
            with self.SessionLocal() as session:
                if email:
                    # Tìm API key theo email
                    api_key = session.query(YouTubeApiKey).filter(
                        YouTubeApiKey.email == email,
                        YouTubeApiKey.status == 'active',
                        YouTubeApiKey.remaining_quota > 0
                    ).first()
                else:
                    # Lấy API key active đầu tiên có quota
                    api_key = session.query(YouTubeApiKey).filter(
                        YouTubeApiKey.status == 'active',
                        YouTubeApiKey.remaining_quota > 0
                    ).order_by(YouTubeApiKey.last_updated.asc()).first()
                
                if not api_key:
                    return None
                
                return {
                    "id": api_key.id,
                    "api_key": api_key.api_key,
                    "remaining_quota": api_key.remaining_quota,
                    "status": api_key.status,
                    "last_updated": api_key.last_updated
                }
                
        except SQLAlchemyError as e:
            self.logger.error(f"Database error getting API key: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error getting API key: {e}")
            return None

    def update_quota(self, api_key: str, quota_used: int) -> bool:
        """
        Update the remaining quota for an API key and its status.
        
        Args:
            api_key (str): The API key to update
            quota_used (int): Amount of quota used
            
        Returns:
            bool: True if update successful, False otherwise
        """
        try:
            with self.SessionLocal() as session:
                # Tìm API key
                key_obj = session.query(YouTubeApiKey).filter(YouTubeApiKey.api_key == api_key).first()
                
                if not key_obj:
                    self.logger.warning(f"API key not found: {api_key}")
                    return False
                
                # Sử dụng quota
                if key_obj.use_quota(quota_used):
                    session.commit()
                    self.logger.info(f"Updated quota for API key: {api_key}, used: {quota_used}")
                    return True
                else:
                    self.logger.warning(f"Insufficient quota for API key: {api_key}")
                    return False
                    
        except SQLAlchemyError as e:
            self.logger.error(f"Database error updating quota: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Error updating quota: {e}")
            return False

    def get_api_key_stats(self, api_key: str) -> Dict[str, Any]:
        """
        Get statistics for an API key.
        
        Args:
            api_key (str): The API key
            
        Returns:
            Dict[str, Any]: Statistics including quota and usage history
        """
        try:
            with self.SessionLocal() as session:
                key_obj = session.query(YouTubeApiKey).filter(YouTubeApiKey.api_key == api_key).first()
                
                if not key_obj:
                    return {}
            
                return {
                    "api_key": key_obj.api_key,
                    "remaining_quota": key_obj.remaining_quota,
                    "status": key_obj.status,
                    "last_updated": key_obj.last_updated,
                }
                
        except SQLAlchemyError as e:
            self.logger.error(f"Database error getting API key stats: {e}")
            return {}
        except Exception as e:
            self.logger.error(f"Error getting API key stats: {e}")
            return {}

    def get_active_api_keys(self) -> List[Dict[str, Any]]:
        try:
            with self.SessionLocal() as session:
                active_keys = session.query(YouTubeApiKey).filter(
                    YouTubeApiKey.status == 'active',
                    YouTubeApiKey.remaining_quota > 0
                ).order_by(YouTubeApiKey.last_updated.asc()).all()
                
                return [key.to_dict() for key in active_keys]
                
        except SQLAlchemyError as e:
            self.logger.error(f"Database error getting active API keys: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Error getting active API keys: {e}")
            return []


    def reset_quota(self, api_key: str, new_quota: int = 10000) -> bool:
        """
        Reset quota cho API key.
        
        Args:
            api_key (str): API key
            new_quota (int): Quota mới
            
        Returns:
            bool: True nếu reset thành công, False nếu không
        """
        try:
            with self.SessionLocal() as session:
                key_obj = session.query(YouTubeApiKey).filter(YouTubeApiKey.api_key == api_key).first()
                
                if not key_obj:
                    self.logger.warning(f"API key not found: {api_key}")
                    return False
                
                key_obj.reset_quota(new_quota)
                session.commit()
                
                self.logger.info(f"Reset quota for API key: {api_key}")
                return True
                
        except SQLAlchemyError as e:
            self.logger.error(f"Database error resetting quota: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Error resetting quota: {e}")
            return False

    def get_all_keys(self) -> List[Dict[str, Any]]:
        """
        Lấy tất cả API keys.
        
        Returns:
            List[Dict[str, Any]]: Danh sách tất cả API keys
        """
        try:
            with self.SessionLocal() as session:
                all_keys = session.query(YouTubeApiKey).all()
                return [key.to_dict() for key in all_keys]
                
        except SQLAlchemyError as e:
            self.logger.error(f"Database error getting all keys: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Error getting all keys: {e}")
            return []

    def deactivate_key(self, api_key: str) -> bool:
        """
        Deactivate API key.
        
        Args:
            api_key (str): API key
            
        Returns:
            bool: True nếu deactivate thành công, False nếu không
        """
        try:
            with self.SessionLocal() as session:
                key_obj = session.query(YouTubeApiKey).filter(YouTubeApiKey.api_key == api_key).first()
                
                if not key_obj:
                    self.logger.warning(f"API key not found: {api_key}")
                    return False
                
                key_obj.status = 'unactive'
                key_obj.last_updated = datetime.utcnow()
                session.commit()
                
                self.logger.info(f"Deactivated API key: {api_key}")
                return True
                
        except SQLAlchemyError as e:
            self.logger.error(f"Database error deactivating key: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Error deactivating key: {e}")
            return False

    def activate_key(self, api_key: str) -> bool:
        """
        Activate API key.
        
        Args:
            api_key (str): API key
            
        Returns:
            bool: True nếu activate thành công, False nếu không
        """
        try:
            with self.SessionLocal() as session:
                key_obj = session.query(YouTubeApiKey).filter(YouTubeApiKey.api_key == api_key).first()
                
                if not key_obj:
                    self.logger.warning(f"API key not found: {api_key}")
                    return False
                
                key_obj.status = 'active'
                key_obj.last_updated = datetime.utcnow()
                session.commit()
                
                self.logger.info(f"Activated API key: {api_key}")
                return True
                
        except SQLAlchemyError as e:
            self.logger.error(f"Database error activating key: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Error activating key: {e}")
            return False

    def close(self):
        """Đóng kết nối database."""
        if self.engine:
            self.engine.dispose()
            self.logger.info("PostgreSQL connection closed")


   
   