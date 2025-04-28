from typing import Dict, Any, List, Optional
from datetime import datetime
from bson import ObjectId
from .database import Database

class APIKeyManager:
    def __init__(self, db: Database):
        self.db = db
        self.collection = db.collections["api_keys"]

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
        api_key_doc = {
            "email": email,
            "api_key": api_key,
            "remaining_quota": quota,
            "status": self._get_status(quota),
            "last_updated": datetime.now(),
            "used_history": []
        }
        
        result = self.collection.insert_one(api_key_doc)
        api_key_doc["_id"] = result.inserted_id
        return api_key_doc

    def get_api_key(self, email: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Get an API key with remaining quota.
        If email is provided, get the specific API key for that email.
        
        Args:
            email (Optional[str]): Email to get specific API key
            
        Returns:
            Optional[Dict[str, Any]]: API key document or None if not found
        """
        query = {"status": "active"}
        if email:
            query["email"] = email
            
        return self.collection.find_one(query)

    def update_quota(self, api_key: str, quota_used: int) -> bool:
        """
        Update the remaining quota for an API key and its status.
        
        Args:
            api_key (str): The API key to update
            quota_used (int): Amount of quota used
            
        Returns:
            bool: True if update successful, False otherwise
        """
        # Get current quota
        doc = self.collection.find_one({"api_key": api_key})
        if not doc:
            return False
            
        new_quota = doc["remaining_quota"] - quota_used
        new_status = self._get_status(new_quota)
        
        result = self.collection.update_one(
            {"api_key": api_key},
            {
                "$inc": {"remaining_quota": -quota_used},
                "$set": {
                    "status": new_status,
                    "last_updated": datetime.now()
                }
            }
        )
        return result.modified_count > 0

    def add_keyword_id(self, api_key: str, keyword_id: str, used_quota: int, crawl_date: datetime) -> bool:
        """
        Add a keyword usage history to the API key's used_history array.
        
        Args:
            api_key (str): The API key
            keyword_id (str): The _id of the keyword document
            used_quota (int): The amount of quota used
            crawl_date (datetime): The date when the keyword was crawled
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Validate inputs
            if not api_key or not keyword_id or not crawl_date:
                print("Error: API key, keyword ID and crawl date are required")
                return False
                
            # Check if API key exists
            api_key_doc = self.collection.find_one({"api_key": api_key})
            if not api_key_doc:
                print(f"Error: API key {api_key} not found")
                return False
                
            # Create usage history object
            usage_history = {
                "keyword_id": keyword_id,
                "used_quota": used_quota,
                "crawl_date": crawl_date
            }
                
            # Update the document
            result = self.collection.update_one(
                {"api_key": api_key},
                {
                    "$push": {"used_history": usage_history},
                    "$set": {"last_updated": datetime.now()}
                }
            )
            
            if result.modified_count > 0:
                print(f"Success: Added usage history for keyword ID {keyword_id} to API key {api_key}")
                return True
            else:
                print(f"Error: Failed to add usage history for keyword ID {keyword_id} to API key {api_key}")
                return False
                
        except Exception as e:
            print(f"Error adding usage history: {str(e)}")
            return False

    def get_keywords_by_api_key(self, api_key: str) -> List[Dict[str, Any]]:
        """
        Get all keyword usage history associated with an API key.
        
        Args:
            api_key (str): The API key
            
        Returns:
            List[Dict[str, Any]]: List of keyword usage history
        """
        doc = self.collection.find_one({"api_key": api_key})
        return doc.get("used_history", []) if doc else []

    def get_api_key_stats(self, api_key: str) -> Dict[str, Any]:
        """
        Get statistics for an API key.
        
        Args:
            api_key (str): The API key
            
        Returns:
            Dict[str, Any]: Statistics including quota and usage history
        """
        doc = self.collection.find_one({"api_key": api_key})
        if not doc:
            return {}
            
        # Calculate total quota used from history
        total_quota_used = sum(history.get("used_quota", 0) for history in doc.get("used_history", []))
            
        return {
            "email": doc["email"],
            "remaining_quota": doc["remaining_quota"],
            "status": doc["status"],
            "total_quota_used": total_quota_used,
            "keyword_count": len(doc.get("used_history", [])),
            "last_updated": doc["last_updated"]
        }

    def get_active_api_keys(self) -> List[Dict[str, Any]]:
        """
        Get all active API keys.
        
        Returns:
            List[Dict[str, Any]]: List of active API key documents
        """
        return list(self.collection.find({"status": "active"}))

    def get_unactive_api_keys(self) -> List[Dict[str, Any]]:
        """
        Get all unactive API keys.
        
        Returns:
            List[Dict[str, Any]]: List of unactive API key documents
        """
        return list(self.collection.find({"status": "unactive"}))

    def get_keyword_usage_by_date(self, api_key: str, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """
        Get keyword usage history for an API key within a date range.
        
        Args:
            api_key (str): The API key
            start_date (datetime): Start date
            end_date (datetime): End date
            
        Returns:
            List[Dict[str, Any]]: List of keyword usage history within the date range
        """
        doc = self.collection.find_one({"api_key": api_key})
        if not doc:
            return []
            
        return [
            history for history in doc.get("used_history", [])
            if start_date <= history.get("crawl_date", datetime.min) <= end_date
        ]

    def get_total_quota_used(self, api_key: str) -> int:
        """
        Get total quota used by an API key.
        
        Args:
            api_key (str): The API key
            
        Returns:
            int: Total quota used
        """
        doc = self.collection.find_one({"api_key": api_key})
        if not doc:
            return 0
            
        return sum(history.get("used_quota", 0) for history in doc.get("used_history", []))

    def get_keyword_count(self, api_key: str) -> int:
        """
        Get total number of keywords crawled by an API key.
        
        Args:
            api_key (str): The API key
            
        Returns:
            int: Number of keywords crawled
        """
        doc = self.collection.find_one({"api_key": api_key})
        return len(doc.get("used_history", [])) if doc else 0 