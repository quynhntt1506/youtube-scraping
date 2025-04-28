import sys
import os
from typing import List, Dict, Any

# Add src directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.database import Database
from utils.api_key_manager import APIKeyManager

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
    # Initialize database and API key manager
    db = Database()
    api_manager = APIKeyManager(db)
    
    # Add each API key
    for api_data in api_keys_data:
        try:
            email = api_data["email"]
            api_key = api_data["api_key"]
            quota = api_data.get("quota", 10000)  # Default quota if not specified
            
            result = api_manager.add_api_key(email, api_key, quota)
            print(f"Successfully added API key for {email}")
            print(f"Status: {result['status']}")
            print(f"Remaining quota: {result['remaining_quota']}")
            print("-" * 50)
            
        except Exception as e:
            print(f"Error adding API key for {api_data.get('email', 'unknown')}: {str(e)}")
            print("-" * 50)
    
    # Close database connection
    db.close()

if __name__ == "__main__":
    # Example usage
    api_keys_to_add = [
        {
            "email": "quynhntt150602@gmail.com",
            "api_key": "AIzaSyCzereW1kHUnGvUDRKxAVkrbIYXhCLum4w",
            "quota": 10000
        },
        {
            "email": "20020117@vnu.edu.vn",
            "api_key": "AIzaSyDaYvVHnU50j_Ug4iCDzvUD500R2ylbz6c",
            "quota": 10000
        },
        {
            "email": "trantrunghieu0201@gmail.com",
            "api_key": "AIzaSyDaqCjg4PQJzyeHDClF1dx4rqCJNvmXo0c",
            "quota": 10000
        },
        {
            "email": "hoaian150602@gmail.com",
            "api_key": "AIzaSyCvpZQH3puwKemg72N4BM3NsR8lJ82Al_o",
            "quota": 10000
        }
    ]
    
    add_api_keys(api_keys_to_add) 