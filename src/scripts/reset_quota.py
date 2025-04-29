import schedule
import time
from datetime import datetime
import pytz
import sys
from pathlib import Path

# Add parent directory to path to import utils
sys.path.append(str(Path(__file__).parent.parent))
from utils.database import Database

def reset_quota():
    """Reset quota of all API keys to 10000 at 00:00 PT time daily."""
    try:
        db = Database()
        current_time = datetime.now()
        
        # Update all API keys' remaining_quota to 10000
        result = db.collections["api_keys"].update_many(
            {},
            {
                "$set": {
                    "remaining_quota": 10000,
                    "status": "active",
                    "last_updated": current_time,
                    "updated_at": current_time
                }
            }
        )
        
        # Get current time in PT
        pt_time = datetime.now(pytz.timezone('US/Pacific'))
        print(f"✅ Reset quota completed at {pt_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        print(f"✅ Updated {result.modified_count} API keys")
        
    except Exception as e:
        print(f"❌ Error resetting quota: {str(e)}")
    finally:
        db.close()

def main():
    # Get Pacific timezone
    pt = pytz.timezone('US/Pacific')
    
    # Schedule the job to run at 00:00 PT time every day
    schedule.every().day.at("00:00").do(reset_quota).timezone = pt
    
    print("🔄 Quota reset scheduler started. Will reset at 00:00 PT time daily.")
    print(f"Current PT time: {datetime.now(pt).strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    # Run the scheduler
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

if __name__ == "__main__":
    main() 