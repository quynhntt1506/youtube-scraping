# Models Documentation

## Overview

This directory contains the data models for the YouTube crawler application, supporting both Pydantic models for data validation and SQLAlchemy models for PostgreSQL database operations.

## Model Structure

### Core Models

#### 1. Channel Models
- **`Channel`** (Pydantic): Data validation model for channel information
- **`ChannelSQL`** (SQLAlchemy): PostgreSQL table model for channels

**Key Fields:**
- `channel_id`: Unique YouTube channel ID
- `title`: Channel title
- `description`: Channel description
- `subscriber_count`, `video_count`, `view_count`: Statistics
- `topics`: JSON array of channel topics
- `status`: Crawling status

#### 2. Video Models
- **`Video`** (Pydantic): Data validation model for video information
- **`VideoSQL`** (SQLAlchemy): PostgreSQL table model for videos

**Key Fields:**
- `video_id`: Unique YouTube video ID
- `channel_id`: Foreign key to channels table
- `title`, `description`: Video metadata
- `view_count`, `like_count`, `comment_count`: Statistics
- `duration`, `definition`: Video properties
- `tags`: JSON array of video tags

#### 3. Comment Models
- **`Comment`** (Pydantic): Data validation model for comments
- **`CommentSQL`** (SQLAlchemy): PostgreSQL table model for comments

**Key Fields:**
- `comment_id`: Unique comment ID
- `video_id`: Foreign key to videos table
- `author_display_name`: Comment author
- `text_display`: Comment content
- `like_count`: Number of likes
- `total_reply_count`: Number of replies

#### 4. Reply Models
- **`Reply`** (Pydantic): Data validation model for comment replies
- **`ReplySQL`** (SQLAlchemy): PostgreSQL table model for replies

**Key Fields:**
- `comment_id`: Unique reply ID
- `parent_id`: Foreign key to comments table
- `author_display_name`: Reply author
- `text_display`: Reply content

### Additional Models

#### 5. API Key Management
- **`ApiKey`** (SQLAlchemy): Manages YouTube API keys and quotas

#### 6. Keyword Management
- **`YouTubeKeyword`** (SQLAlchemy): Manages search keywords
- **`KeywordUsage`** (SQLAlchemy): Tracks keyword usage per API key

## Database Relationships

```
channels (1) ←→ (many) videos
videos (1) ←→ (many) comments
comments (1) ←→ (many) replies
api_keys (1) ←→ (many) keyword_usage
```

## Usage Examples

### Creating a Channel from YouTube API Response

```python
from src.models import Channel

# YouTube API response
youtube_response = {...}

# Create Channel instance
channel = Channel.from_youtube_response(youtube_response)

# Convert to SQL format
sql_data = channel.to_sql_dict()
```

### Database Operations

```python
from src.models import ChannelSQL
from src.database.postgres_database import PostgresDatabase

# Initialize database
db = PostgresDatabase()

# Insert channel
channel_data = channel.to_sql_dict()
db.insert_channel(channel_data)

# Query channels
channels = db.get_channels_by_status("crawled_channel")
```

## Migration from MongoDB

The models have been updated to support PostgreSQL:

1. **ID Fields**: Changed from MongoDB ObjectId to PostgreSQL Integer
2. **Timestamps**: Unix timestamps converted to PostgreSQL DateTime
3. **JSON Fields**: Arrays stored as JSON strings in PostgreSQL
4. **Foreign Keys**: Proper relationships between tables
5. **Indexes**: Optimized for common queries

## File Structure

```
src/models/
├── __init__.py              # Exports all models
├── channel.py              # Channel models
├── video.py                # Video models
├── comment.py              # Comment and Reply models
├── postgres_models.py      # Additional PostgreSQL models
└── README.md              # This documentation
```

## Database Schema

### Tables Created

1. **channels**: YouTube channel information
2. **videos**: Video metadata and statistics
3. **comments**: Video comments
4. **replies**: Comment replies
5. **api_keys**: API key management
6. **youtube_keywords**: Search keywords
7. **keyword_usage**: Keyword usage tracking

### Indexes

- Primary keys on all tables
- Foreign key indexes for relationships
- Status indexes for filtering
- Timestamp indexes for date-based queries
- Unique indexes on YouTube IDs

## Running Database Setup

```bash
# Create all tables and indexes
python src/scripts/create_postgres_tables.py
```

This script will:
1. Connect to PostgreSQL database
2. Create all tables if they don't exist
3. Create optimized indexes
4. Display table schema information 