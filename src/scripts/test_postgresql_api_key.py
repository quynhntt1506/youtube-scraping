#!/usr/bin/env python3
"""
Test script cho hệ thống APIKeyManager với PostgreSQL.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.api_key_manager import APIKeyManager
from src.utils.api import YouTubeAPI
from src.utils.logger import CustomLogger

def test_api_key_manager():
    """Test chức năng của APIKeyManager."""
    logger = CustomLogger("test")
    
    try:
        # Khởi tạo manager
        manager = APIKeyManager()
        logger.info("✅ APIKeyManager initialized successfully")
        
        # Test tạo API key
        test_key = manager.add_api_key(
            email="test@example.com",
            api_key="AIzaSyTestKey1234567890abcdefghijklmnopqrstuvwxyz",
            quota=5000
        )
        
        if test_key:
            logger.info(f"✅ Created test key: {test_key}")
            
            # Test lấy API key
            active_key = manager.get_api_key()
            if active_key:
                logger.info(f"✅ Retrieved active key: {active_key}")
                
                # Test sử dụng quota
                success = manager.update_quota(active_key['apiKey'], 100)
                if success:
                    logger.info("✅ Quota usage successful")
                    updated_key = manager.get_api_key()
                    logger.info(f"   Updated key: {updated_key}")
                else:
                    logger.error("❌ Quota usage failed")
                
                # Test lấy thống kê
                stats = manager.get_api_key_stats(active_key['apiKey'])
                logger.info(f"✅ API key stats: {stats}")
                
                # Test lấy tất cả keys
                all_keys = manager.get_all_keys()
                logger.info(f"✅ All keys: {len(all_keys)} found")
                
                # Test lấy active keys
                active_keys = manager.get_active_api_keys()
                logger.info(f"✅ Active keys: {len(active_keys)} found")
                
            else:
                logger.error("❌ Failed to retrieve active key")
        else:
            logger.error("❌ Failed to create test key")
        
        # Đóng kết nối
        manager.close()
        logger.info("✅ APIKeyManager test completed successfully")
        
    except Exception as e:
        logger.error(f"❌ APIKeyManager test failed: {e}")
        raise

def test_youtube_api():
    """Test chức năng của YouTubeAPI với APIKeyManager mới."""
    logger = CustomLogger("test")
    
    try:
        # Khởi tạo YouTubeAPI
        api = YouTubeAPI()
        logger.info("✅ YouTubeAPI initialized successfully")
        
        # Test load API keys
        api_keys = api._load_api_keys()
        logger.info(f"✅ Loaded {len(api_keys)} API keys")
        
        # Test build service
        if api.youtube:
            logger.info("✅ YouTube service built successfully")
        else:
            logger.warning("⚠️ YouTube service not available")
        
        # Test lấy API key từ manager
        active_key = api.api_manager.get_api_key()
        if active_key:
            logger.info(f"✅ Retrieved API key from manager: {active_key['apiKey'][:10]}...")
        else:
            logger.warning("⚠️ No active API key found")
        
        # Đóng kết nối
        api.close()
        logger.info("✅ YouTubeAPI test completed successfully")
        
    except Exception as e:
        logger.error(f"❌ YouTubeAPI test failed: {e}")
        raise

def test_integration():
    """Test tích hợp giữa APIKeyManager và YouTubeAPI."""
    logger = CustomLogger("test")
    
    try:
        # Khởi tạo cả hai
        manager = APIKeyManager()
        api = YouTubeAPI()
        logger.info("✅ Both managers initialized successfully")
        
        # Test tạo API key và sử dụng trong YouTubeAPI
        test_key = manager.add_api_key(
            email="integration@example.com",
            api_key="AIzaSyIntegrationKey1234567890abcdefghijklmnopqrstuvwxyz",
            quota=10000
        )
        
        if test_key:
            logger.info(f"✅ Created integration test key: {test_key}")
            
            # Reload API keys trong YouTubeAPI
            api.api_keys = api._load_api_keys()
            logger.info(f"✅ Reloaded {len(api.api_keys)} API keys in YouTubeAPI")
            
            # Test switch API key
            if len(api.api_keys) > 1:
                success = api._switch_api_key()
                logger.info(f"✅ API key switch: {success}")
            
            # Test quota tracking
            api._update_quota_usage(50)
            logger.info("✅ Quota usage updated successfully")
            
            # Kiểm tra quota đã được cập nhật
            updated_key = manager.get_api_key()
            if updated_key:
                logger.info(f"✅ Updated quota: {updated_key['remainingQuota']}")
        
        # Đóng kết nối
        manager.close()
        api.close()
        logger.info("✅ Integration test completed successfully")
        
    except Exception as e:
        logger.error(f"❌ Integration test failed: {e}")
        raise

def main():
    """Chạy tất cả các test."""
    logger = CustomLogger("test_main")
    
    try:
        logger.info("🚀 Starting PostgreSQL API Key tests...")
        
        # Test APIKeyManager
        logger.info("\n📋 Testing APIKeyManager...")
        test_api_key_manager()
        
        # Test YouTubeAPI
        logger.info("\n📋 Testing YouTubeAPI...")
        test_youtube_api()
        
        # Test integration
        logger.info("\n📋 Testing Integration...")
        test_integration()
        
        logger.info("\n🎉 All tests completed successfully!")
        
    except Exception as e:
        logger.error(f"💥 Test suite failed: {e}")
        raise

if __name__ == "__main__":
    main() 