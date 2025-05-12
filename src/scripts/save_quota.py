import sys
import os
from typing import List, Dict, Any
from datetime import datetime

# Add src directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.database import Database
from utils.logger import CustomLogger

# Initialize logger
logger = CustomLogger("save_quota")

def add_api_keys(api_keys_data: List[Dict[str, Any]]) -> None:
    """
    Add multiple API keys to the database.
    
    Args:
        api_keys_data (List[Dict[str, Any]]): List of API key information
            Each item should contain:
            - email: str
            - api_key: str
            - quota: int (optional, default: 10000)
    """
    # Initialize database
    db = Database()
    
    # Add each API key
    for api_data in api_keys_data:
        try:
            email = api_data["email"]
            api_key = api_data["api_key"]
            quota = api_data.get("quota", 10000)  # Default quota if not specified
            current_time = datetime.now()
            
            # Check if email exists in api_key collection
            existing_key = db.collections["api_keys"].find_one({"email": email})
            
            if existing_key:
                # Update existing API key
                result = db.collections["api_keys"].update_one(
                    {"email": email},
                    {
                        "$set": {
                            "apiKey": api_key,
                            "remainingQuota": quota,
                            "status": "active",
                            "lastUpdated": current_time,
                        }
                    }
                )
                logger.info(f"Updated API key for {email}")
                logger.info(f"Status: Updated successfully")
                logger.info(f"Remaining quota: {quota}")
                logger.info(f"Last updated: {current_time}")
            else:
                # Insert new API key
                result = db.collections["api_keys"].insert_one({
                    "email": email,
                    "apiKey": api_key,
                    "remainingQuota": quota,
                    "status": "active",
                    "lastUpdated": current_time,
                })
                logger.info(f"Added new API key for {email}")
                logger.info(f"Status: Added successfully")
                logger.info(f"Remaining quota: {quota}")
                logger.info(f"Last updated: {current_time}")
            
            logger.info("-" * 50)
            
        except Exception as e:
            logger.error(f"Error processing API key for {api_data.get('email', 'unknown')}: {str(e)}")
            logger.error("-" * 50)
    
    # Close database connection
    db.close()

if __name__ == "__main__":
    # Example usage
    api_keys_to_add = [
        {
            "email": "johndoe.0374809044@gmail.com",
            "api_key": "AIzaSyAZfteIR0pWCNrs8bl9YY3KNDyCrhJwfk4",
            "quota": 10000
        },
    ]
    logger.info("Start adding API keys to database")
    add_api_keys(api_keys_to_add) 
    logger.info("Finish adding API keys to database")