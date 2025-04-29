import logging
import os
from datetime import datetime
from typing import Optional

class CustomLogger:
    def __init__(self, name: str, log_dir: str = "logs"):
        """Initialize logger with file and console handlers.
        
        Args:
            name (str): Name of the logger
            log_dir (str): Directory to store log files
        """
        # Create logs directory if it doesn't exist
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
            
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        
        # Create handlers
        current_date = datetime.now().strftime("%Y-%m-%d")
        file_handler = logging.FileHandler(f'{log_dir}/{name}_{current_date}.log')
        console_handler = logging.StreamHandler()
        
        # Create formatters and add it to handlers
        log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(log_format)
        console_handler.setFormatter(log_format)
        
        # Add handlers to the logger
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        # Store API key status
        self.api_key_status = {}
    
    def info(self, message: str, api_key: Optional[str] = None):
        """Log info message.
        
        Args:
            message (str): Message to log
            api_key (str, optional): API key identifier for tracking
        """
        if api_key:
            self._update_api_key_status(api_key, "info", message)
        self.logger.info(message)
    
    def error(self, message: str, api_key: Optional[str] = None):
        """Log error message.
        
        Args:
            message (str): Message to log
            api_key (str, optional): API key identifier for tracking
        """
        if api_key:
            self._update_api_key_status(api_key, "error", message)
        self.logger.error(message)
    
    def warning(self, message: str, api_key: Optional[str] = None):
        """Log warning message.
        
        Args:
            message (str): Message to log
            api_key (str, optional): API key identifier for tracking
        """
        if api_key:
            self._update_api_key_status(api_key, "warning", message)
        self.logger.warning(message)
    
    def _update_api_key_status(self, api_key: str, status_type: str, message: str):
        """Update API key status tracking.
        
        Args:
            api_key (str): API key identifier
            status_type (str): Type of status (info/error/warning)
            message (str): Status message
        """
        if api_key not in self.api_key_status:
            self.api_key_status[api_key] = {
                "last_used": datetime.now(),
                "status": "active",
                "errors": 0,
                "warnings": 0,
                "history": []
            }
        
        self.api_key_status[api_key]["last_used"] = datetime.now()
        self.api_key_status[api_key]["history"].append({
            "timestamp": datetime.now(),
            "type": status_type,
            "message": message
        })
        
        if status_type == "error":
            self.api_key_status[api_key]["errors"] += 1
            if "quota" in message.lower() or "exceeded" in message.lower():
                self.api_key_status[api_key]["status"] = "quota_exceeded"
        elif status_type == "warning":
            self.api_key_status[api_key]["warnings"] += 1
    
    def get_api_key_status(self, api_key: str) -> dict:
        """Get current status of an API key.
        
        Args:
            api_key (str): API key identifier
            
        Returns:
            dict: API key status information
        """
        return self.api_key_status.get(api_key, {})
    
    def get_all_api_key_statuses(self) -> dict:
        """Get status of all tracked API keys.
        
        Returns:
            dict: All API key statuses
        """
        return self.api_key_status 