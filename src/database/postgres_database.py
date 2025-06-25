from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from typing import Dict, Any, List, Optional
from datetime import datetime
import json
import time
from functools import wraps

from src.models.postgres_models import Base, Channel, Video, Comment, Reply, ApiKey, KeywordUsage, YouTubeKeyword
from src.config.config import POSTGRES_URI, STATUS_ENTITY

def retry_postgres_operation(max_retries: int = 3, delay: float = 1.0):
    """Decorator to retry PostgreSQL operations on failure."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (SQLAlchemyError, IntegrityError) as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        time.sleep(delay * (attempt + 1))
                        continue
                    raise last_exception
            return None
        return wrapper
    return decorator

class PostgresDatabase:
    def __init__(self):
        self.engine = create_engine(POSTGRES_URI)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        
    def create_tables(self):
        """Create all tables if they don't exist."""
        Base.metadata.create_all(bind=self.engine)
        
    def get_session(self) -> Session:
        """Get a new database session."""
        return self.SessionLocal()
        
    def close(self):
        """Close database connection."""
        self.engine.dispose()

    @retry_postgres_operation()
    def channel_exists(self, channel_id: str) -> bool:
        """Check if a channel exists in the database."""
        session = self.get_session()
        try:
            return session.query(Channel).filter(Channel.channel_id == channel_id).first() is not None
        finally:
            session.close()

    @retry_postgres_operation()
    def video_exists(self, video_id: str) -> bool:
        """Check if a video exists in the database."""
        session = self.get_session()
        try:
            return session.query(Video).filter(Video.video_id == video_id).first() is not None
        finally:
            session.close()

    @retry_postgres_operation()
    def insert_channel(self, channel_data: Dict[str, Any]) -> None:
        """Insert a channel document if it doesn't exist."""
        if not self.channel_exists(channel_data["channelId"]):
            session = self.get_session()
            try:
                channel = Channel(
                    channel_id=channel_data["channelId"],
                    title=channel_data.get("title", ""),
                    description=channel_data.get("description"),
                    custom_url=channel_data.get("customUrl"),
                    published_at=channel_data.get("publishedAt"),
                    country=channel_data.get("country"),
                    subscriber_count=channel_data.get("subscriberCount", 0),
                    video_count=channel_data.get("videoCount", 0),
                    view_count=channel_data.get("viewCount", 0),
                    topics=json.dumps(channel_data.get("topics", [])) if channel_data.get("topics") else None,
                    email=channel_data.get("email"),
                    avatar_url=channel_data.get("avatarUrl"),
                    banner_url=channel_data.get("bannerUrl"),
                    playlist_id=channel_data.get("playlistId"),
                    status=channel_data.get("status", "to_crawl")
                )
                session.add(channel)
                session.commit()
            finally:
                session.close()

    @retry_postgres_operation()
    def insert_video(self, video_data: Dict[str, Any]) -> None:
        """Insert a video document if it doesn't exist."""
        if not self.video_exists(video_data["videoId"]):
            session = self.get_session()
            try:
                video = Video(
                    video_id=video_data["videoId"],
                    title=video_data.get("title", ""),
                    description=video_data.get("description"),
                    published_at=video_data.get("publishedAt"),
                    channel_id=video_data.get("channelId"),
                    channel_title=video_data.get("channelTitle"),
                    thumbnail_url=video_data.get("thumbnailUrl"),
                    view_count=video_data.get("viewCount", 0),
                    like_count=video_data.get("likeCount", 0),
                    comment_count=video_data.get("commentCount", 0),
                    duration=video_data.get("duration"),
                    dimension=video_data.get("dimension"),
                    definition=video_data.get("definition"),
                    projection=video_data.get("projection"),
                    caption=video_data.get("caption"),
                    licensed_content=video_data.get("licensedContent"),
                    live_broadcast_content=video_data.get("liveBroadcastContent"),
                    topics=json.dumps(video_data.get("topics", [])) if video_data.get("topics") else None,
                    category_id=video_data.get("categoryId"),
                    tags=json.dumps(video_data.get("tags", [])) if video_data.get("tags") else None,
                    playlist_id=video_data.get("playlistId"),
                    position=video_data.get("position"),
                    crawl_date=video_data.get("crawlDate", datetime.utcnow()),
                    update_status=video_data.get("updateStatus"),
                    privacy_status=video_data.get("privacyStatus"),
                    public_stats_viewable=video_data.get("publicStatsViewable"),
                    embeddable=video_data.get("embeddable"),
                    made_for_kids=video_data.get("madeForKids"),
                    license=video_data.get("license"),
                    status=video_data.get("status", "to_crawl")
                )
                session.add(video)
                session.commit()
            finally:
                session.close()

    @retry_postgres_operation()
    def insert_many_channels(self, channels: List[dict]) -> Dict[str, Any]:
        """Insert multiple channels using upsert logic."""
        if not channels:
            return {
                "new_channels_count": 0,
                "updated_channels_count": 0,
                "new_channel_ids": []
            }
            
        session = self.get_session()
        try:
            new_count = 0
            updated_count = 0
            new_channel_ids = []
            
            for channel_data in channels:
                channel_id = channel_data.get("channelId")
                if not channel_id:
                    continue
                    
                existing_channel = session.query(Channel).filter(Channel.channel_id == channel_id).first()
                
                if existing_channel:
                    # Update existing channel
                    for key, value in channel_data.items():
                        if hasattr(existing_channel, key.lower()) and value is not None:
                            setattr(existing_channel, key.lower(), value)
                    updated_count += 1
                else:
                    # Insert new channel
                    channel = Channel(
                        channel_id=channel_id,
                        title=channel_data.get("title", ""),
                        description=channel_data.get("description"),
                        custom_url=channel_data.get("customUrl"),
                        published_at=channel_data.get("publishedAt"),
                        country=channel_data.get("country"),
                        subscriber_count=channel_data.get("subscriberCount", 0),
                        video_count=channel_data.get("videoCount", 0),
                        view_count=channel_data.get("viewCount", 0),
                        topics=json.dumps(channel_data.get("topics", [])) if channel_data.get("topics") else None,
                        email=channel_data.get("email"),
                        avatar_url=channel_data.get("avatarUrl"),
                        banner_url=channel_data.get("bannerUrl"),
                        playlist_id=channel_data.get("playlistId"),
                        status=channel_data.get("status", "to_crawl")
                    )
                    session.add(channel)
                    new_count += 1
                    new_channel_ids.append(channel_id)
            
            session.commit()
            
            return {
                "new_channels_count": new_count,
                "updated_channels_count": updated_count,
                "new_channel_ids": new_channel_ids
            }
            
        except Exception as e:
            session.rollback()
            raise
        finally:
            session.close()

    @retry_postgres_operation()
    def insert_many_videos(self, videos: List[dict]) -> Dict[str, Any]:
        """Insert multiple videos using upsert logic."""
        if not videos:
            return {
                "new_videos_count": 0,
                "updated_videos_count": 0,
                "new_video_ids": []
            }
            
        session = self.get_session()
        try:
            new_count = 0
            updated_count = 0
            new_video_ids = []
            
            for video_data in videos:
                video_id = video_data.get("videoId")
                if not video_id:
                    continue
                    
                existing_video = session.query(Video).filter(Video.video_id == video_id).first()
                
                if existing_video:
                    # Update existing video
                    for key, value in video_data.items():
                        if hasattr(existing_video, key.lower()) and value is not None:
                            setattr(existing_video, key.lower(), value)
                    updated_count += 1
                else:
                    # Insert new video
                    video = Video(
                        video_id=video_id,
                        title=video_data.get("title", ""),
                        description=video_data.get("description"),
                        published_at=video_data.get("publishedAt"),
                        channel_id=video_data.get("channelId"),
                        channel_title=video_data.get("channelTitle"),
                        thumbnail_url=video_data.get("thumbnailUrl"),
                        view_count=video_data.get("viewCount", 0),
                        like_count=video_data.get("likeCount", 0),
                        comment_count=video_data.get("commentCount", 0),
                        duration=video_data.get("duration"),
                        dimension=video_data.get("dimension"),
                        definition=video_data.get("definition"),
                        projection=video_data.get("projection"),
                        caption=video_data.get("caption"),
                        licensed_content=video_data.get("licensedContent"),
                        live_broadcast_content=video_data.get("liveBroadcastContent"),
                        topics=json.dumps(video_data.get("topics", [])) if video_data.get("topics") else None,
                        category_id=video_data.get("categoryId"),
                        tags=json.dumps(video_data.get("tags", [])) if video_data.get("tags") else None,
                        playlist_id=video_data.get("playlistId"),
                        position=video_data.get("position"),
                        crawl_date=video_data.get("crawlDate", datetime.utcnow()),
                        update_status=video_data.get("updateStatus"),
                        privacy_status=video_data.get("privacyStatus"),
                        public_stats_viewable=video_data.get("publicStatsViewable"),
                        embeddable=video_data.get("embeddable"),
                        made_for_kids=video_data.get("madeForKids"),
                        license=video_data.get("license"),
                        status=video_data.get("status", "to_crawl")
                    )
                    session.add(video)
                    new_count += 1
                    new_video_ids.append(video_id)
            
            session.commit()
            
            return {
                "new_videos_count": new_count,
                "updated_videos_count": updated_count,
                "new_video_ids": new_video_ids
            }
            
        except Exception as e:
            session.rollback()
            raise
        finally:
            session.close()

    @retry_postgres_operation()
    def insert_many_comments(self, comments: List[dict]) -> Dict[str, Any]:
        """Insert multiple comments using upsert logic."""
        if not comments:
            return {
                "new_comments_count": 0,
                "updated_comments_count": 0,
                "new_comment_ids": [],
                "error_count": 0,
                "errors": []
            }
            
        session = self.get_session()
        try:
            new_count = 0
            updated_count = 0
            new_comment_ids = []
            error_count = 0
            errors = []
            
            for comment_data in comments:
                comment_id = comment_data.get("commentId")
                if not comment_id:
                    error_count += 1
                    errors.append("Comment missing commentId")
                    continue
                    
                existing_comment = session.query(Comment).filter(Comment.comment_id == comment_id).first()
                
                if existing_comment:
                    # Update existing comment
                    for key, value in comment_data.items():
                        if hasattr(existing_comment, key.lower()) and value is not None:
                            setattr(existing_comment, key.lower(), value)
                    updated_count += 1
                else:
                    # Insert new comment
                    comment = Comment(
                        comment_id=comment_id,
                        video_id=comment_data.get("videoId"),
                        author_display_name=comment_data.get("authorDisplayName", ""),
                        author_profile_image_url=comment_data.get("authorProfileImageUrl"),
                        author_channel_id=comment_data.get("authorChannelId"),
                        author_channel_url=comment_data.get("authorChannelUrl"),
                        text_display=comment_data.get("textDisplay", ""),
                        like_count=comment_data.get("likeCount", 0),
                        is_public=comment_data.get("isPublic", True),
                        can_reply=comment_data.get("canReply", True),
                        published_at=comment_data.get("publishedAt"),
                        updated_at=comment_data.get("updatedAt"),
                        total_reply_count=comment_data.get("totalReplyCount", 0),
                        crawl_date=comment_data.get("crawlDate", datetime.utcnow())
                    )
                    session.add(comment)
                    new_count += 1
                    new_comment_ids.append(comment_id)
            
            session.commit()
            
            return {
                "new_comments_count": new_count,
                "updated_comments_count": updated_count,
                "new_comment_ids": new_comment_ids,
                "error_count": error_count,
                "errors": errors
            }
            
        except Exception as e:
            session.rollback()
            error_msg = f"Error processing comments: {str(e)}"
            return {
                "new_comments_count": 0,
                "updated_comments_count": 0,
                "new_comment_ids": [],
                "error_count": len(comments),
                "errors": [error_msg]
            }
        finally:
            session.close()

    @retry_postgres_operation()
    def get_keyword_by_keyword(self, keyword: str) -> Optional[Dict[str, Any]]:
        """Get keyword document from youtube_keywords table."""
        session = self.get_session()
        try:
            keyword_obj = session.query(YouTubeKeyword).filter(YouTubeKeyword.keyword == keyword).first()
            if keyword_obj:
                return {
                    "keyword": keyword_obj.keyword,
                    "status": keyword_obj.status,
                    "last_updated": keyword_obj.last_updated,
                    "created_at": keyword_obj.created_at
                }
            return None
        finally:
            session.close()

    @retry_postgres_operation()
    def update_keyword_status(self, keyword: str, status: str) -> bool:
        """Update status of a keyword in youtube_keywords table."""
        if status not in ["to_crawl", "crawling", "crawled"]:
            return False
        
        session = self.get_session()
        try:
            keyword_obj = session.query(YouTubeKeyword).filter(YouTubeKeyword.keyword == keyword).first()
            if keyword_obj:
                keyword_obj.status = status
                keyword_obj.last_updated = datetime.utcnow()
                session.commit()
                return True
            return False
        except Exception:
            session.rollback()
            return False
        finally:
            session.close()

    @retry_postgres_operation()
    def update_channels_status_by_playlist_ids(self, playlist_ids: List[str]) -> Dict[str, Any]:
        """Update status of channels that have the given playlist IDs."""
        if not playlist_ids:
            return {
                "updated_count": 0,
                "playlist_ids": [],
                "not_found_playlist_ids": []
            }
        
        session = self.get_session()
        try:
            # Find channels with matching playlist IDs
            channels = session.query(Channel).filter(Channel.playlist_id.in_(playlist_ids)).all()
            
            found_playlist_ids = set()
            updated_count = 0
            
            for channel in channels:
                if channel.playlist_id:
                    found_playlist_ids.add(channel.playlist_id)
                    channel.status = "crawled_video"
                    updated_count += 1
            
            session.commit()
            
            not_found_playlist_ids = set(playlist_ids) - found_playlist_ids
            
            return {
                "updated_count": updated_count,
                "playlist_ids": list(found_playlist_ids),
                "not_found_playlist_ids": list(not_found_playlist_ids)
            }
            
        except Exception as e:
            session.rollback()
            return {
                "updated_count": 0,
                "playlist_ids": [],
                "not_found_playlist_ids": playlist_ids
            }
        finally:
            session.close()

    @retry_postgres_operation()
    def update_videos_status_by_video_ids(self, video_ids: List[str]) -> Dict[str, Any]:
        """Update status of videos that have the given video IDs."""
        if not video_ids:
            return {
                "updated_count": 0,
                "video_ids": [],
                "not_found_video_ids": []
            }
        
        session = self.get_session()
        try:
            # Find videos with matching video IDs
            videos = session.query(Video).filter(Video.video_id.in_(video_ids)).all()
            
            found_video_ids = set()
            updated_count = 0
            
            for video in videos:
                if video.video_id:
                    found_video_ids.add(video.video_id)
                    video.status = STATUS_ENTITY["crawled_comment"]
                    updated_count += 1
            
            session.commit()
            
            not_found_video_ids = set(video_ids) - found_video_ids
            
            return {
                "updated_count": updated_count,
                "video_ids": list(found_video_ids),
                "not_found_video_ids": list(not_found_video_ids)
            }
            
        except Exception as e:
            session.rollback()
            return {
                "updated_count": 0,
                "video_ids": [],
                "not_found_video_ids": video_ids
            }
        finally:
            session.close() 