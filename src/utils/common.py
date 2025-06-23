from datetime import datetime
from typing import Optional, Union, Dict
from dateutil import parser
import re
from urllib.parse import urlparse, unquote

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

def format_datetime_to_iso(published_at: str) -> str:
    if "." in published_at:
        if "Z" in published_at:
            base = published_at.split(".")[0]
            published_at = f"{base}Z"
        elif "+" in published_at:
            base, tz = published_at.split("+")
            base = base.split(".")[0]
            published_at = f"{base}+{tz}"
    return published_at

def parse_youtube_channel_url(url: str) -> Dict[str, Optional[str]]:
    """Parse YouTube channel URL and extract channel ID or username.
    
    Args:
        url (str): YouTube channel URL
        
    Returns:
        Dict[str, Optional[str]]: Dictionary containing:
            - type: 'channel_id', 'username', 'custom_url', or 'customUrl'
            - value: channel ID or username
            - original_url: original URL
            
    Examples:
        >>> parse_youtube_channel_url("https://www.youtube.com/channel/UCxxxxxxxx")
        {'type': 'channel_id', 'value': 'UCxxxxxxxx', 'original_url': 'https://www.youtube.com/channel/UCxxxxxxxx'}
        
        >>> parse_youtube_channel_url("https://www.youtube.com/@username")
        {'type': 'customUrl', 'value': '@username', 'original_url': 'https://www.youtube.com/@username'}
        
        >>> parse_youtube_channel_url("https://www.youtube.com/user/username")
        {'type': 'username', 'value': 'username', 'original_url': 'https://www.youtube.com/user/username'}
    """
    # Clean and normalize URL
    url = url.strip()
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
        
    try:
        parsed_url = urlparse(url)
        
        # Check if it's a YouTube URL
        if not any(domain in parsed_url.netloc.lower() for domain in ['youtube.com', 'www.youtube.com', 'youtu.be']):
            return {
                'type': None,
                'value': None,
                'original_url': url
            }
            
        # Split path into parts
        path_parts = parsed_url.path.strip('/').split('/')
        
        if len(path_parts) < 2:
            return {
                'type': None,
                'value': None,
                'original_url': url
            }
            
        # Handle different URL formats
        if path_parts[0] == 'channel':
            # Format: /channel/UCxxxxxxxx
            channel_id = path_parts[1]
            if re.match(r'^UC[a-zA-Z0-9_-]{22}$', channel_id):
                return {
                    'type': 'channel_id',
                    'value': channel_id,
                    'original_url': url
                }
                
        elif path_parts[0].startswith('@'):
            # Format: /@username
            username = path_parts[0]  # Keep @ symbol
            return {
                'type': 'customUrl',
                'value': username,
                'original_url': url
            }
            
        elif path_parts[0] == 'user':
            # Format: /user/username
            username = path_parts[1]
            return {
                'type': 'username',
                'value': username,
                'original_url': url
            }
            
        elif path_parts[0] == 'c':
            # Format: /c/username
            username = path_parts[1]
            return {
                'type': 'custom_url',
                'value': username,
                'original_url': url
            }
            
    except Exception as e:
        print(f"Error parsing URL: {str(e)}")
        
    return {
        'type': None,
        'value': None,
        'original_url': url
    }

def convert_datetime_to_timestamp(dt_str: str) -> int:
    """
    Chuyển chuỗi datetime (ISO 8601 hoặc định dạng phổ biến) về unix timestamp (giây).
    Args:
        dt_str (str): Chuỗi datetime (ví dụ: '2023-09-26T02:31:51.000Z')
    Returns:
        int: Unix timestamp (giây)
    """
    try:
        dt = parser.parse(dt_str)
        return int(dt.timestamp())
    except Exception:
        raise ValueError(f"Error processing convert datetime: {dt_str}")
