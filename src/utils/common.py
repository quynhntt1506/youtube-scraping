from datetime import datetime
from typing import Optional, Union
from dateutil import parser

def convert_to_datetime(date_str: str) -> Optional[datetime]:
    """
    Convert string to datetime object.
    Supports multiple date formats including ISO format.
    
    Args:
        date_str (str): Date string to convert
        
    Returns:
        Optional[datetime]: Datetime object if conversion successful, None otherwise
    """
    if not date_str:
        return None
        
    try:
        # Try ISO format first (YYYY-MM-DDThh:mm:ss.sZ)
        if "T" in date_str and "Z" in date_str:
            return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            
        # Try using dateutil parser for other formats
        return parser.parse(date_str)
    except (ValueError, TypeError) as e:
        print(f"Error converting date string '{date_str}': {str(e)}")
        return None

def format_datetime(dt: datetime, format_str: str = "%Y-%m-%d %H:%M:%S") -> str:
    """
    Format datetime object to string.
    
    Args:
        dt (datetime): Datetime object to format
        format_str (str): Format string (default: "%Y-%m-%d %H:%M:%S")
        
    Returns:
        str: Formatted date string
    """
    if not dt:
        return ""
    return dt.strftime(format_str)

def get_current_datetime() -> datetime:
    """
    Get current datetime.
    
    Returns:
        datetime: Current datetime
    """
    return datetime.now()

def get_datetime_from_timestamp(timestamp: Union[int, float]) -> datetime:
    """
    Convert timestamp to datetime.
    
    Args:
        timestamp (Union[int, float]): Unix timestamp
        
    Returns:
        datetime: Datetime object
    """
    return datetime.fromtimestamp(timestamp)

def get_timestamp_from_datetime(dt: datetime) -> float:
    """
    Convert datetime to timestamp.
    
    Args:
        dt (datetime): Datetime object
        
    Returns:
        float: Unix timestamp
    """
    return dt.timestamp() 