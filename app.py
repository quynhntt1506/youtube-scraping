import os
import sys

# Add the 'src' directory to the Python path
# This allows us to import modules from the 'src' directory as if they were top-level packages
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))

from src.main import main as run_main
from src.utils.logger import CustomLogger

# Initialize logger for the application entry point
logger = CustomLogger("app")

if __name__ == '__main__':
    """
    Application entry point.
    
    This script initializes the application and calls the main function
    from src/main.py, which handles command-line arguments and starts
    the appropriate service.
    
    Usage examples:
    - python app.py --service kafka
    - python app.py --service kafka-auto --num-keywords 5
    - python app.py --service kafka-channel --channel-url "https://www.youtube.com/@SonTungMTP"
    """
    logger.info("🚀 Starting YouTube Crawler Application...")
    try:
        run_main()
        logger.info("✅ Application finished successfully.")
    except Exception as e:
        logger.error(f"💥 An error occurred in the application: {e}", exc_info=True)
        sys.exit(1)
