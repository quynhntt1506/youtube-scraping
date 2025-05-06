import logging
from logging.handlers import RotatingFileHandler
from config.config import LOG_FORMAT, LOG_LEVEL, LOG_FILE
import os
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional

class CustomLogger:
    def __init__(self, name: str, log_dir: str = "logs"):
        """Initialize logger with file and console handlers.
        
        Args:
            name (str): Name of the logger
            log_dir (str): Directory to store log files
        """
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # Create logger
        self.logger = logging.getLogger(name)
        self.logger.setLevel(LOG_LEVEL)
        
        # Create formatter
        formatter = logging.Formatter(LOG_FORMAT)
        
        # Create console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        # Create file handler
        log_file = self.log_dir / f"{name}_{datetime.now().strftime('%Y%m%d')}.log"
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        
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
        try:
            self.logger.info(message)
        except UnicodeEncodeError:
            # If there's an encoding error, try to encode the message
            encoded_message = message.encode('utf-8', errors='replace').decode('utf-8')
            self.logger.info(encoded_message)
    
    def error(self, message: str, api_key: Optional[str] = None):
        """Log error message.
        
        Args:
            message (str): Message to log
            api_key (str, optional): API key identifier for tracking
        """
        if api_key:
            self._update_api_key_status(api_key, "error", message)
        try:
            self.logger.error(message)
        except UnicodeEncodeError:
            encoded_message = message.encode('utf-8', errors='replace').decode('utf-8')
            self.logger.error(encoded_message)
    
    def warning(self, message: str, api_key: Optional[str] = None):
        """Log warning message.
        
        Args:
            message (str): Message to log
            api_key (str, optional): API key identifier for tracking
        """
        if api_key:
            self._update_api_key_status(api_key, "warning", message)
        try:
            self.logger.warning(message)
        except UnicodeEncodeError:
            encoded_message = message.encode('utf-8', errors='replace').decode('utf-8')
            self.logger.warning(encoded_message)
    
    def debug(self, message: str):
        """Log debug message.
        
        Args:
            message (str): Message to log
        """
        self.logger.debug(message)
    
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