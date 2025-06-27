#!/usr/bin/env python3
"""
Demo script cho APIKeyManager với PostgreSQL.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.api_key_manager import APIKeyManager
from src.utils.logger import CustomLogger

def demo_api_key_manager():
    """Demo chức năng của APIKeyManager."""
    logger = CustomLogger("demo")
    
    try:
        # Khởi tạo manager
        manager = APIKeyManager()
        logger.info("APIKeyManager initialized")
        
        # Demo API keys
        demo_keys = [
            {
                "email": "gaosecret150602@gmail.com",
                "api_key": "AIzaSyAOs2_3bySOWc-7fEp0pU-7ZycFmLDZwOY",
                "quota": 10000
            },
            {
                "email": "htschti7@gmail.com",
                "api_key": "AIzaSyBgnvSfarUGzJuyPID1sDDJCePL6nMblvw",
                "quota": 10000
            },
            {
                "email": "htsccompany2@gmail.com",
                "api_key": "AIzaSyDnaEyIT9tknMunWiop9Ltuuw7kYN73fTg",
                "quota": 10000
            }
        ]

        
        # Tạo demo API keys
        logger.info("Creating demo API keys...")
        created_keys = []
        for key_data in demo_keys:
            result = manager.add_api_key(
                email=key_data["email"],
                api_key=key_data["api_key"],
                quota=key_data["quota"]
            )
            if result:
                created_keys.append(result)
                logger.info(f"Created API key: {result}")
            else:
                logger.warning(f"Failed to create API key for {key_data['email']}")
        
        # Lấy API key active
        logger.info("Getting active API key...")
        active_key = manager.get_api_key()
        if active_key:
            logger.info(f"Active API key: {active_key}")
        else:
            logger.warning("No active API key found")
        
        # Sử dụng quota
        if active_key:
            logger.info(f"Using 100 quota from API key {active_key['api_key'][:10]}...")
            success = manager.update_quota(active_key['api_key'], 100)
            if success:
                logger.info("Quota used successfully")
                # Lấy key đã cập nhật
                updated_key = manager.get_api_key()
                logger.info(f"Updated key: {updated_key}")
            else:
                logger.warning("Failed to use quota")
        
        # Lấy tất cả API keys active
        logger.info("Getting all active keys...")
        active_keys = manager.get_active_api_keys()
        logger.info(f"Found {len(active_keys)} active keys")
        for key in active_keys:
            logger.info(f"  - {key}")
        
        # Test quota exhaustion
        if active_key:
            logger.info(f"Testing quota exhaustion for key {active_key['api_key'][:10]}...")
            # Sử dụng tất cả remaining quota
            remaining = active_key['remaining_quota']
            if remaining > 0:
                success = manager.update_quota(active_key['api_key'], remaining)
                if success:
                    logger.info("Quota exhausted successfully")
                    exhausted_key = manager.get_api_key()
                    logger.info(f"Exhausted key: {exhausted_key}")
                else:
                    logger.warning("Failed to exhaust quota")
        
        # Reset quota
        if active_key:
            logger.info(f"Resetting quota for key {active_key['api_key'][:10]}...")
            success = manager.reset_quota(active_key['api_key'], 10000)
            if success:
                logger.info("Quota reset successfully")
                reset_key = manager.get_api_key()
                logger.info(f"Reset key: {reset_key}")
            else:
                logger.warning("Failed to reset quota")
        
        # Test deactivation và activation
        if active_key:
            logger.info(f"Testing deactivation for key {active_key['api_key'][:10]}...")
            success = manager.deactivate_key(active_key['api_key'])
            if success:
                logger.info("Key deactivated successfully")
                deactivated_key = manager.get_api_key()
                logger.info(f"Deactivated key: {deactivated_key}")
                
                # Reactivate
                logger.info("Reactivating key...")
                success = manager.activate_key(active_key['api_key'])
                if success:
                    logger.info("Key reactivated successfully")
                    reactivated_key = manager.get_api_key()
                    logger.info(f"Reactivated key: {reactivated_key}")
                else:
                    logger.warning("Failed to reactivate key")
            else:
                logger.warning("Failed to deactivate key")
        
        # Lấy tất cả keys
        logger.info("Getting all keys...")
        all_keys = manager.get_all_keys()
        logger.info(f"Found {len(all_keys)} total keys")
        for key in all_keys:
            logger.info(f"  - {key}")
        
        # Lấy thống kê
        if active_key:
            logger.info("Getting API key statistics...")
            stats = manager.get_api_key_stats(active_key['api_key'])
            logger.info(f"Statistics: {stats}")
        
        # Đóng kết nối
        manager.close()
        logger.info("Demo completed successfully")
        
    except Exception as e:
        logger.error(f"Demo failed: {e}")
        raise

if __name__ == "__main__":
    demo_api_key_manager() 