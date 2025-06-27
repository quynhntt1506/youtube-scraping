import random
from typing import List, Dict
from datetime import datetime
from src.database.keyword_manager import KeywordManager
from src.config.config import STATUS_ENTITY
from src.utils.logger import CustomLogger

class KeywordGenerator:
    def __init__(self):
        # Initialize logger
        self.logger = CustomLogger('keyword_generator')
        self.keyword_manager = KeywordManager()
        
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
            "bé trai", "bé gái", "thanh niên", "người trung niên", "người cao tuổi", "gen z"
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
            "núi", "rừng", "sông", "hồ", "thành phố", "công sở"
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
            "trải nghiệm", "thử", "test", "đánh giá", "bình luận", "trang điểm"
        ]
        
        # Common sentence patterns
        self.sentence_patterns = [
            "{content} {demographic}",
            "{demographic} {content}",
            "{trend} {content}",
            "{content} {trend}",
            "{quality} {content}",
            "{content} {setting}",
            "{setting} {content}",
            "{demographic} {action}",
            "{action} {demographic}",
            "{content} {face}",
            "{face} {content}",
            "{trend} {demographic}",
            "{demographic} {trend}",
            "{quality} {demographic}",
            "{demographic} {quality}"
        ]
            

    def generate_keywords(self, num_keywords: int = 100) -> List[str]:
        """Generate a list of meaningful Vietnamese keywords related to human faces and content, and save to PostgreSQL.
        
        Args:
            num_keywords (int): Number of keywords to generate
            
        Returns:
            List[str]: List of generated keywords
        """
        self.logger.info(f"Starting keyword generation for {num_keywords} keywords (PostgreSQL)")
        keywords = set()
        max_attempts = num_keywords * 2
        try:
            # Lấy tất cả keyword đã có trong DB
            existing_keywords = set()
            all_db_keywords = self.keyword_manager.get_keywords()
            for kw in all_db_keywords:
                existing_keywords.add(kw["keyword"])
            self.logger.info(f"Found {len(existing_keywords)} existing keywords in PostgreSQL")
            attempts = 0
            while len(keywords) < num_keywords and attempts < max_attempts:
                attempts += 1
                for pattern in self.sentence_patterns:
                    if len(keywords) >= num_keywords:
                        break
                    keyword = pattern.format(
                        demographic=random.choice(self.demographic_keywords),
                        action=random.choice(self.action_verbs),
                        content=random.choice(self.content_keywords),
                        trend=random.choice(self.trend_keywords),
                        setting=random.choice(self.setting_keywords),
                        face=random.choice(self.face_keywords),
                        quality=random.choice(self.quality_keywords)
                    )
                    if keyword not in existing_keywords:
                        keywords.add(keyword)
                        existing_keywords.add(keyword)
                        self.logger.info(f"Generated new keyword: {keyword}")
                # Các tổ hợp đơn giản
                if len(keywords) < num_keywords:
                    keyword = f"{random.choice(self.content_keywords)} {random.choice(self.demographic_keywords)}"
                    if keyword not in existing_keywords:
                        keywords.add(keyword)
                        existing_keywords.add(keyword)
                        self.logger.info(f"Generated new keyword: {keyword}")
                if len(keywords) < num_keywords:
                    keyword = f"{random.choice(self.trend_keywords)} {random.choice(self.content_keywords)}"
                    if keyword not in existing_keywords:
                        keywords.add(keyword)
                        existing_keywords.add(keyword)
                        self.logger.info(f"Generated new keyword: {keyword}")
                if len(keywords) < num_keywords:
                    keyword = f"{random.choice(self.demographic_keywords)} {random.choice(self.content_keywords)} {random.choice(self.trend_keywords)}"
                    if keyword not in existing_keywords:
                        keywords.add(keyword)
                        existing_keywords.add(keyword)
                        self.logger.info(f"Generated new keyword: {keyword}")
                if len(keywords) < num_keywords:
                    keyword = f"{random.choice(self.action_verbs)} {random.choice(self.content_keywords)}"
                    if keyword not in existing_keywords:
                        keywords.add(keyword)
                        existing_keywords.add(keyword)
                        self.logger.info(f"Generated new keyword: {keyword}")
            # Lưu vào PostgreSQL
            self._save_to_postgres(list(keywords))
            keyword_list = list(keywords)[:num_keywords]
            self.logger.info(f"Successfully generated {len(keyword_list)} keywords (PostgreSQL)")
            return keyword_list
        except Exception as e:
            self.logger.error(f"Error during keyword generation: {str(e)}")
            raise

    def _save_to_postgres(self, keywords: List[str], status: str = None, type_: str = None) -> None:
        """Save generated keywords to PostgreSQL.
        
        Args:
            keywords (List[str]): List of keywords to save
            status (str): Status for new keywords (default: STATUS_ENTITY["to_crawl"])
            type_ (str): Type for new keywords (default: "auto_generated")
        """
        if status is None:
            status = STATUS_ENTITY["to_crawl"]
        if type_ is None:
            type_ = "auto_generated"
        for keyword in keywords:
            self.keyword_manager.add_keyword(keyword, status, type_)

    def get_keyword_stats(self) -> Dict[str, int]:
        """Get statistics about available keywords in each category (PostgreSQL)."""
        try:
            stats = self.keyword_manager.get_keyword_stats()
            self.logger.info(f"Keyword stats (PostgreSQL): {stats}")
            return stats
        except Exception as e:
            self.logger.error(f"Error getting keyword stats: {str(e)}")
            raise

    def close(self):
        self.keyword_manager.close()
        self.logger.info("Closed PostgreSQL connection")

    def generate_keywords_stateless(self, num_keywords: int = 100) -> List[str]:
        """
        Generates a list of keywords without checking or saving to the database.
        
        Args:
            num_keywords (int): The number of keywords to generate.
            
        Returns:
            List[str]: A list of generated keywords.
        """
        self.logger.info(f"Starting stateless keyword generation for {num_keywords} keywords.")
        keywords = set()
        
        try:
            # Generate combinations until we have enough unique keywords
            while len(keywords) < num_keywords:
                
                # Use sentence patterns for more complex keywords
                pattern = random.choice(self.sentence_patterns)
                keyword = pattern.format(
                    demographic=random.choice(self.demographic_keywords),
                    action=random.choice(self.action_verbs),
                    content=random.choice(self.content_keywords),
                    trend=random.choice(self.trend_keywords),
                    setting=random.choice(self.setting_keywords),
                    face=random.choice(self.face_keywords),
                    quality=random.choice(self.quality_keywords)
                )
                keywords.add(keyword)

                # Add some simpler, two-word combinations
                if len(keywords) < num_keywords:
                    k1 = random.choice(self.content_keywords)
                    k2 = random.choice(self.demographic_keywords)
                    keywords.add(f"{k1} {k2}")

                if len(keywords) < num_keywords:
                    k1 = random.choice(self.trend_keywords)
                    k2 = random.choice(self.content_keywords)
                    keywords.add(f"{k1} {k2}")

            keyword_list = list(keywords)[:num_keywords]
            self.logger.info(f"Successfully generated {len(keyword_list)} stateless keywords.")
            return keyword_list
            
        except Exception as e:
            self.logger.error(f"Error during stateless keyword generation: {e}", exc_info=True)
            return [] 