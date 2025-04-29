import random
from typing import List, Dict
from datetime import datetime
from pymongo import MongoClient
from config.config import MONGODB_URI, MONGODB_DB
from .logger import CustomLogger

class KeywordGenerator:
    def __init__(self):
        # Initialize logger
        self.logger = CustomLogger('keyword_generator')
        
        # Base keywords related to faces
        self.face_keywords = [
            "khuôn mặt", "chân dung", "ảnh chân dung", "ảnh thẻ", "ảnh cận mặt",
            "gương mặt", "đầu người", "người", "con người", "nhân vật",
            "mặt người", "chân dung người", "ảnh selfie", "ảnh cá nhân"
        ]
        
        # Keywords for content types
        self.content_keywords = [
            "vlog", "livestream", "phỏng vấn", "tiktok", "reels",
            "short", "video ngắn", "video dài", "podcast", "talkshow",
            "review", "hướng dẫn", "tutorial", "unboxing", "thử thách",
            "challenge", "trải nghiệm", "review sản phẩm", "makeup", "trang điểm",
            "thời trang", "fashion", "style", "phong cách", "outfit"
        ]
        
        # Keywords for trends and popular content
        self.trend_keywords = [
            "hot trend", "xu hướng", "trào lưu", "trending", "viral",
            "nổi tiếng", "hot", "đình đám", "bùng nổ", "sốt",
            "mới nhất", "mới ra", "mới cập nhật", "mới phát hành",
            "đang hot", "đang trend", "đang viral", "đang nổi"
        ]
        
        # Keywords for demographics
        self.demographic_keywords = [
            "nam", "nữ", "trẻ em", "người lớn", "người già",
            "thanh niên", "người lớn tuổi", "thiếu niên", "trẻ nhỏ", "cao tuổi",
            "học sinh", "sinh viên", "người đi làm", "người trẻ", "người trưởng thành",
            "bé trai", "bé gái", "thanh niên", "người trung niên", "người cao tuổi"
        ]
        
        # Keywords for emotions and expressions
        self.expression_keywords = [
            "vui vẻ", "buồn", "tức giận", "ngạc nhiên", "bình thường",
            "nghiêm túc", "cười lớn", "khóc", "suy nghĩ", "bối rối",
            "hạnh phúc", "thất vọng", "lo lắng", "sợ hãi", "tự tin",
            "ngại ngùng", "xấu hổ", "tự hào", "thích thú", "chán nản"
        ]
        
        # Keywords for settings and backgrounds
        self.setting_keywords = [
            "studio", "ngoài trời", "trong nhà", "tự nhiên", "nhân tạo",
            "ánh sáng", "bóng tối", "phông nền", "không gian", "cảnh",
            "phố", "công viên", "quán cà phê", "trung tâm thương mại", "bãi biển",
            "núi", "rừng", "sông", "hồ", "thành phố"
        ]
        
        # Keywords for quality and style
        self.quality_keywords = [
            "chất lượng cao", "chuyên nghiệp", "nghiệp dư", "tự nhiên",
            "tạo dáng", "nghệ thuật", "chân thực", "chi tiết",
            "đẹp", "ấn tượng", "độc đáo", "sáng tạo", "mới lạ",
            "cổ điển", "hiện đại", "trẻ trung", "năng động", "thời thượng"
        ]
        
        # Action verbs
        self.action_verbs = [
            "cười", "khóc", "suy nghĩ", "nói chuyện", "hát",
            "nhảy", "chạy", "đi bộ", "đọc", "viết",
            "vẽ", "chụp ảnh", "quay phim", "biểu diễn", "trình diễn",
            "thuyết trình", "phỏng vấn", "review", "hướng dẫn", "giới thiệu",
            "trải nghiệm", "thử", "test", "đánh giá", "bình luận"
        ]
        
        # Common sentence patterns
        self.sentence_patterns = [
            "{demographic} {action} {content}",
            "{demographic} {action} {trend}",
            "{demographic} {action} {content} {trend}",
            "{content} {demographic} {action}",
            "{trend} {content} {demographic}",
            "{quality} {content} {demographic}",
            "{demographic} {content} {setting}",
            "{demographic} {action} {content} {setting}",
            "{trend} {content} {demographic} {setting}",
            "{demographic} {action} {face} {content}",
            "{demographic} {action} {face} {trend}",
            "{content} {demographic} {action} {face}",
            "{trend} {content} {demographic} {face}",
            "{quality} {content} {demographic} {face}",
            "{demographic} {content} {face} {setting}"
        ]
        
        # Connect to MongoDB
        self.client = MongoClient(MONGODB_URI)
        self.db = self.client[MONGODB_DB]
        self.collection = self.db["keyword_generation"]

    def generate_keywords(self, num_keywords: int = 100) -> List[str]:
        """Generate a list of meaningful Vietnamese keywords related to human faces and content.
        
        Args:
            num_keywords (int): Number of keywords to generate
            
        Returns:
            List[str]: List of generated keywords
        """
        self.logger.info(f"Starting keyword generation for {num_keywords} keywords")
        keywords = set()
        max_attempts = num_keywords * 2  # Limit number of attempts to avoid infinite loop
        
        try:
            # Get existing keywords from MongoDB
            self.logger.info("Fetching existing keywords from MongoDB")
            existing_keywords = set(doc["keyword"] for doc in self.collection.find({}, {"keyword": 1}))
            self.logger.info(f"Found {len(existing_keywords)} existing keywords")
            
            # Generate combinations until we have enough unique keywords
            attempts = 0
            while len(keywords) < num_keywords and attempts < max_attempts:
                attempts += 1
                
                # Generate using sentence patterns
                for pattern in self.sentence_patterns:
                    if len(keywords) >= num_keywords:
                        break
                        
                    # Replace placeholders with random words
                    keyword = pattern.format(
                        demographic=random.choice(self.demographic_keywords),
                        action=random.choice(self.action_verbs),
                        content=random.choice(self.content_keywords),
                        trend=random.choice(self.trend_keywords),
                        setting=random.choice(self.setting_keywords),
                        face=random.choice(self.face_keywords),
                        quality=random.choice(self.quality_keywords)
                    )
                    
                    # Only add if keyword doesn't exist in MongoDB
                    if keyword not in existing_keywords:
                        keywords.add(keyword)
                        existing_keywords.add(keyword)
                        self.logger.info(f"Generated new keyword: {keyword}")
                
                # Add some simple combinations
                if len(keywords) < num_keywords:
                    # Content + Demographic
                    keyword = f"{random.choice(self.content_keywords)} {random.choice(self.demographic_keywords)}"
                    if keyword not in existing_keywords:
                        keywords.add(keyword)
                        existing_keywords.add(keyword)
                        self.logger.info(f"Generated new keyword: {keyword}")
                    
                    # Trend + Content
                    keyword = f"{random.choice(self.trend_keywords)} {random.choice(self.content_keywords)}"
                    if keyword not in existing_keywords:
                        keywords.add(keyword)
                        existing_keywords.add(keyword)
                        self.logger.info(f"Generated new keyword: {keyword}")
                    
                    # Demographic + Content + Trend
                    keyword = f"{random.choice(self.demographic_keywords)} {random.choice(self.content_keywords)} {random.choice(self.trend_keywords)}"
                    if keyword not in existing_keywords:
                        keywords.add(keyword)
                        existing_keywords.add(keyword)
                        self.logger.info(f"Generated new keyword: {keyword}")
                    
                    # Action + Content
                    keyword = f"{random.choice(self.action_verbs)} {random.choice(self.content_keywords)}"
                    if keyword not in existing_keywords:
                        keywords.add(keyword)
                        existing_keywords.add(keyword)
                        self.logger.info(f"Generated new keyword: {keyword}")
            
            # Convert to list and limit to requested number
            keyword_list = list(keywords)[:num_keywords]
            
            # Save to MongoDB
            self._save_to_mongodb(keyword_list)
            
            self.logger.info(f"Successfully generated {len(keyword_list)} keywords")
            return keyword_list
            
        except Exception as e:
            self.logger.error(f"Error during keyword generation: {str(e)}")
            raise

    def _save_to_mongodb(self, keywords: List[str]) -> None:
        """Save generated keywords to MongoDB.
        
        Args:
            keywords (List[str]): List of keywords to save
        """
        try:
            current_time = datetime.now()
            
            # Prepare documents for bulk insert
            documents = []
            for keyword in keywords:
                # Check if keyword already exists
                existing = self.collection.find_one({"keyword": keyword})
                if existing:
                    # Update existing document
                    self.collection.update_one(
                        {"keyword": keyword},
                        {
                            "$set": {
                                "last_updated": current_time
                            }
                        }
                    )
                    self.logger.info(f"Updated existing keyword: {keyword}")
                else:
                    # Create new document
                    documents.append({
                        "keyword": keyword,
                        "crawl_date": current_time,
                        "is_crawled": False,
                        "crawl_count": 0,
                        "last_updated": current_time
                    })
            
            # Bulk insert new keywords
            if documents:
                self.collection.insert_many(documents)
                self.logger.info(f"Saved {len(documents)} new keywords to MongoDB")
                
        except Exception as e:
            self.logger.error(f"Error saving keywords to MongoDB: {str(e)}")
            raise

    def get_keyword_stats(self) -> Dict[str, int]:
        """Get statistics about available keywords in each category.
        
        Returns:
            Dict[str, int]: Dictionary with category names and counts
        """
        try:
            stats = {
                "face_keywords": len(self.face_keywords),
                "content_keywords": len(self.content_keywords),
                "trend_keywords": len(self.trend_keywords),
                "demographic_keywords": len(self.demographic_keywords),
                "expression_keywords": len(self.expression_keywords),
                "setting_keywords": len(self.setting_keywords),
                "quality_keywords": len(self.quality_keywords),
                "action_verbs": len(self.action_verbs),
                "sentence_patterns": len(self.sentence_patterns)
            }
            self.logger.info(f"Keyword stats: {stats}")
            return stats
        except Exception as e:
            self.logger.error(f"Error getting keyword stats: {str(e)}")
            raise
        
    def close(self):
        """Close MongoDB connection."""
        try:
            self.client.close()
            self.logger.info("Closed MongoDB connection")
        except Exception as e:
            self.logger.error(f"Error closing MongoDB connection: {str(e)}")
            raise 