import pymongo
import time
from src.database.database import Database
from src.utils.logger import CustomLogger
from pymongo import MongoClient, ASCENDING, DESCENDING, TEXT

logger = CustomLogger("create_index")

def create_indexes():
    """Create indexes for collections."""
    try:
        db = Database()
        
        # Tạo index cho collection channels
        logger.info("\n=== Creating indexes for channels collection ===")
        logger.info("Creating unique index on channelId...")
        db.collections["channels"].create_index(
            [('channelId', ASCENDING)], 
            unique=True, 
            background=False,
            sparse=True  # Chỉ index các document có field channelId
        )
        logger.info("Successfully created unique index on channelId")
        
        logger.info("Creating text index on name and description...")
        db.collections["channels"].create_index(
            [('name', TEXT), ('description', TEXT)], 
            background=False,
            weights={'name': 10, 'description': 5}  # Ưu tiên tìm kiếm theo name hơn description
        )
        logger.info("Successfully created text index on name and description")
        
        # Tạo index cho collection videos
        logger.info("\n=== Creating indexes for videos collection ===")
        logger.info("Creating unique index on videoId...")
        db.collections["videos"].create_index(
            [('videoId', ASCENDING)], 
            unique=True, 
            background=True,
            sparse=True
        )
        logger.info("Successfully created unique index on videoId")
        
        logger.info("Creating index on channelId...")
        db.collections["videos"].create_index(
            [('channelId', ASCENDING)], 
            background=True,
            sparse=True
        )
        logger.info("Successfully created index on channelId")
        
        logger.info("Creating descending index on publishedAt...")
        db.collections["videos"].create_index(
            [('publishedAt', DESCENDING)], 
            background=True,
            sparse=True
        )
        logger.info("Successfully created descending index on publishedAt")
        
        logger.info("Creating text index on title...")
        db.collections["videos"].create_index(
            [('title', TEXT)], 
            background=True,
            weights={'title': 10}  # Ưu tiên tìm kiếm theo title
        )
        logger.info("Successfully created text index on title")

        # Tạo index cho collection comments
        logger.info("\n=== Creating indexes for comments collection ===")
        logger.info("Creating index on videoId...")
        db.collections["comments"].create_index(
            [('videoId', ASCENDING)], 
            background=True,
            sparse=True
        )
        logger.info("Successfully created index on videoId")
        
        logger.info("Creating compound index on authorChannelId and publishedAt...")
        db.collections["comments"].create_index(
            [('authorChannelId', ASCENDING), ('publishedAt', ASCENDING)], 
            background=True,
            sparse=True
        )
        logger.info("Successfully created compound index on authorChannelId and publishedAt")
        
        logger.info("Creating descending index on publishedAt...")
        db.collections["comments"].create_index(
            [('publishedAt', DESCENDING)], 
            background=True,
            sparse=True
        )
        logger.info("Successfully created descending index on publishedAt")
        
        logger.info("Creating text index on textDisplay...")
        db.collections["comments"].create_index(
            [('textDisplay', TEXT)], 
            background=True,
            weights={'textDisplay': 10}  # Ưu tiên tìm kiếm theo content
        )
        logger.info("Successfully created text index on textDisplay")
        
        logger.info("\n=== All indexes created successfully ===")
        
    except Exception as e:
        logger.error(f"Error creating indexes: {str(e)}")
        raise
    finally:
        db.close()

if __name__ == "__main__":
    create_indexes() 