from src.database.database import Database
from src.utils.logger import CustomLogger

logger = CustomLogger("drop_indexes")

def drop_indexes():
    """Drop all existing indexes in the database."""
    try:
        db = Database()
        
        # Get all collections
        collections = db.db.list_collection_names()
        
        for collection_name in collections:
            logger.info(f"\n=== Dropping indexes in collection: {collection_name} ===")
            collection = db.db[collection_name]
            
            # Get current indexes
            current_indexes = collection.index_information()
            logger.info(f"Current indexes: {list(current_indexes.keys())}")
            
            # Drop all indexes except _id index
            try:
                collection.drop_indexes()
                logger.info("Successfully dropped all indexes")
            except Exception as e:
                logger.error(f"Error dropping indexes: {str(e)}")
            
            # Verify indexes were dropped
            remaining_indexes = collection.index_information()
            logger.info(f"Remaining indexes: {list(remaining_indexes.keys())}")
                
    except Exception as e:
        logger.error(f"Error dropping indexes: {str(e)}")
        raise
    finally:
        db.close()

if __name__ == "__main__":
    drop_indexes() 