import schedule
import time
from datetime import datetime, timedelta
import pytz
import sys
from pathlib import Path

# Add parent directory to path to import utils
sys.path.append(str(Path(__file__).parent.parent))
from src.database.api_key_manager import APIKeyManager
from src.utils.logger import CustomLogger

# Initialize logger
logger = CustomLogger("quota_reset")

def get_next_reset_time():
    """Calculate the next reset time in local time."""
    now = datetime.now()
    
    # Log current time
    logger.info(f"Current local time: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Set target time for today
    target_time = now.replace(hour=7, minute=00, second=0, microsecond=0)
    
    # Calculate time difference
    time_diff = (target_time - now).total_seconds()
    logger.info(f"Time until next reset: {time_diff} seconds")
    
    # If target time has passed today, schedule for tomorrow
    if time_diff <= 0:
        next_reset = target_time + timedelta(days=1)
        logger.info("Target time has passed, scheduling for tomorrow")
    else:
        next_reset = target_time
        logger.info("Target time is today")
    
    logger.info(f"Next reset time: {next_reset.strftime('%Y-%m-%d %H:%M:%S')}")
    return next_reset

def reset_quota():
    """Reset quota of all API keys to 10000 at 7:00 local time daily."""
    try:
        api_manager = APIKeyManager()
        current_time = datetime.now()
        
        logger.info(f"Starting quota reset at {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Get all API keys
        all_keys = api_manager.get_all_keys()
        reset_count = 0
        
        # Reset quota for each API key
        for key in all_keys:
            api_key = key.get("api_key")
            if api_key:
                success = api_manager.reset_quota(api_key, 10000)
                if success:
                    reset_count += 1
                    logger.info(f"Reset quota for API key: {api_key[:20]}...")
                else:
                    logger.warning(f"Failed to reset quota for API key: {api_key[:20]}...")
        
        logger.info(f"Reset quota completed at {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Updated {reset_count} API keys")
        
        # Schedule next reset
        next_reset = get_next_reset_time()
        schedule.clear()  # Clear all existing jobs
        schedule.every().day.at(next_reset.strftime("%H:%M")).do(reset_quota)
        logger.info(f"Next reset scheduled for {next_reset.strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        logger.error(f"Error resetting quota: {str(e)}")
    finally:
        api_manager.close()

def main():
    # Calculate initial reset time
    next_reset = get_next_reset_time()
    
    # Clear any existing jobs
    schedule.clear()
    
    # Schedule the first reset
    schedule.every().day.at(next_reset.strftime("%H:%M")).do(reset_quota)
    
    logger.info("Quota reset scheduler started")
    logger.info(f"Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Next reset scheduled for: {next_reset.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Run the scheduler
    while True:
        try:
            current_time = datetime.now()
            logger.info(f"Checking schedule at {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
            schedule.run_pending()
            time.sleep(5)  # Check every 5 seconds for more precise timing
        except Exception as e:
            logger.error(f"Error in scheduler loop: {str(e)}")
            time.sleep(60)  # Wait a bit longer if there's an error

if __name__ == "__main__":
    main() 