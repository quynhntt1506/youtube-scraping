#!/usr/bin/env python3
"""
Script to create PostgreSQL tables from SQLAlchemy models.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from src.models import (
    ChannelSQL, VideoSQL, CommentSQL, ReplySQL,
    ApiKey, KeywordUsage, YouTubeKeyword
)
from src.config.config import POSTGRES_URI

def create_tables():
    """Create all tables in PostgreSQL database."""
    try:
        # Get PostgreSQL configuration
        
        
        # Create engine
        engine = create_engine(POSTGRES_URI, echo=True)
        
        # Test connection
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            print("✅ Database connection successful!")
        
        # Create all tables
        print("Creating tables...")
        ChannelSQL.__table__.create(engine, checkfirst=True)
        VideoSQL.__table__.create(engine, checkfirst=True)
        CommentSQL.__table__.create(engine, checkfirst=True)
        ReplySQL.__table__.create(engine, checkfirst=True)
        ApiKey.__table__.create(engine, checkfirst=True)
        KeywordUsage.__table__.create(engine, checkfirst=True)
        YouTubeKeyword.__table__.create(engine, checkfirst=True)
        
        print("✅ All tables created successfully!")
        
        # Create indexes
        print("Creating indexes...")
        with engine.connect() as conn:
            # Channel indexes
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_channels_channel_id ON channels(channel_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_channels_status ON channels(status)"))
            
            # Video indexes
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_videos_video_id ON videos(video_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_videos_channel_id ON videos(channel_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_videos_status ON videos(status)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_videos_published_at ON videos(published_at)"))
            
            # Comment indexes
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_comments_comment_id ON comments(comment_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_comments_video_id ON comments(video_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_comments_published_at ON comments(published_at)"))
            
            # Reply indexes
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_replies_comment_id ON replies(comment_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_replies_parent_id ON replies(parent_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_replies_published_at ON replies(published_at)"))
            
            # API Key indexes
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_api_keys_api_key ON api_keys(api_key)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_api_keys_is_active ON api_keys(is_active)"))
            
            # Keyword indexes
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_youtube_keywords_keyword ON youtube_keywords(keyword)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_youtube_keywords_status ON youtube_keywords(status)"))
            
            # Keyword Usage indexes
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_keyword_usage_api_key_id ON keyword_usage(api_key_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_keyword_usage_keyword ON keyword_usage(keyword)"))
            
            conn.commit()
        
        print("✅ All indexes created successfully!")
        
        # Show table information
        print("\n📊 Database Tables Created:")
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT table_name, column_name, data_type, is_nullable
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                ORDER BY table_name, ordinal_position
            """))
            
            current_table = None
            for row in result:
                if row.table_name != current_table:
                    current_table = row.table_name
                    print(f"\n📋 Table: {current_table}")
                print(f"  - {row.column_name}: {row.data_type} ({'NULL' if row.is_nullable == 'YES' else 'NOT NULL'})")
        
        return True
        
    except Exception as e:
        print(f"❌ Error creating tables: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Starting PostgreSQL table creation...")
    success = create_tables()
    
    if success:
        print("\n🎉 Database setup completed successfully!")
    else:
        print("\n💥 Database setup failed!")
        sys.exit(1) 