# YouTube API Key Management với PostgreSQL

## Tổng quan

Hệ thống quản lý YouTube API keys đã được chuyển từ MongoDB sang PostgreSQL với cấu trúc đơn giản và dễ bảo trì.

## Cấu trúc Model

### YouTubeApiKey Model

Model `YouTubeApiKey` có cấu trúc đơn giản với các trường:

```python
class YouTubeApiKey(Base):
    __tablename__ = 'youtube_api_keys'
    
    id = Column(Integer, primary_key=True)
    email = Column(String(255), nullable=True)  # Email liên kết với key
    api_key = Column(String(100), unique=True, nullable=False, index=True)
    remaining_quota = Column(Integer, default=10000, nullable=False)
    status = Column(String(20), default='active', nullable=False, index=True)  # 'active' hoặc 'unactive'
    last_updated = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
```

### Các phương thức của Model

- `is_active`: Kiểm tra xem API key có active không
- `is_exhausted`: Kiểm tra xem API key có hết quota không
- `use_quota(amount)`: Sử dụng quota và cập nhật status
- `reset_quota(new_quota)`: Reset quota và kích hoạt key
- `to_dict()`: Chuyển đổi thành dictionary

## APIKeyManager

### Khởi tạo

```python
from src.database.api_key_manager import APIKeyManager

manager = APIKeyManager()
```

### Các phương thức chính

#### 1. Thêm API key mới

```python
result = manager.add_api_key(
    email="user@example.com",
    api_key="AIzaSyYourAPIKeyHere",
    quota=10000
)
```

#### 2. Lấy API key active

```python
# Lấy API key đầu tiên có quota
active_key = manager.get_api_key()

# Lấy API key theo email
specific_key = manager.get_api_key(email="user@example.com")
```

#### 3. Cập nhật quota

```python
success = manager.update_quota("AIzaSyYourAPIKeyHere", 100)
```

#### 4. Reset quota

```python
success = manager.reset_quota("AIzaSyYourAPIKeyHere", 10000)
```

#### 5. Kích hoạt/Deactivate key

```python
# Deactivate
success = manager.deactivate_key("AIzaSyYourAPIKeyHere")

# Activate
success = manager.activate_key("AIzaSyYourAPIKeyHere")
```

#### 6. Lấy thống kê

```python
# Lấy tất cả keys
all_keys = manager.get_all_keys()

# Lấy active keys
active_keys = manager.get_active_api_keys()

# Lấy thống kê của key cụ thể
stats = manager.get_api_key_stats("AIzaSyYourAPIKeyHere")
```

## Tích hợp với YouTubeAPI

### Khởi tạo YouTubeAPI

```python
from src.utils.api import YouTubeAPI

api = YouTubeAPI()  # Tự động sử dụng APIKeyManager
```

### Tự động quản lý quota

YouTubeAPI sẽ tự động:
- Load API keys từ database
- Switch giữa các keys khi hết quota
- Cập nhật quota usage trong database
- Deactivate keys khi hết quota

## Tạo bảng Database

### Sử dụng script

```bash
python src/scripts/create_api_tables.py
```

### SQL thủ công

```sql
CREATE TABLE IF NOT EXISTS youtube_api_keys (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255),
    api_key VARCHAR(100) UNIQUE NOT NULL,
    remaining_quota INTEGER NOT NULL DEFAULT 10000,
    status VARCHAR(20) NOT NULL DEFAULT 'active',
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_youtube_api_keys_api_key ON youtube_api_keys(api_key);
CREATE INDEX IF NOT EXISTS idx_youtube_api_keys_status ON youtube_api_keys(status);
```

## Testing

### Demo script

```bash
python src/scripts/demo_api_key_manager.py
```

### Test script

```bash
python src/scripts/test_postgresql_api_key.py
```

## Lợi ích của hệ thống mới

1. **Cấu trúc đơn giản**: Chỉ 6 trường cần thiết
2. **Dễ bảo trì**: Sử dụng SQLAlchemy ORM
3. **Tự động quản lý**: Tự động switch keys và cập nhật quota
4. **Hiệu suất cao**: Indexes trên các trường quan trọng
5. **Tích hợp tốt**: Hoạt động seamless với YouTubeAPI

## Migration từ MongoDB

Hệ thống đã được thiết kế để tương thích với logic MongoDB cũ:
- Các phương thức có cùng tên và signature
- Trả về cùng format dữ liệu
- Tương thích ngược với code hiện tại

## Troubleshooting

### Lỗi kết nối database

```python
# Kiểm tra config
from src.config.config import POSTGRE_CONFIG
print(POSTGRE_CONFIG)
```

### Lỗi quota

```python
# Kiểm tra quota của key
stats = manager.get_api_key_stats("your_api_key")
print(f"Remaining quota: {stats['remainingQuota']}")
```

### Lỗi API key không tìm thấy

```python
# Kiểm tra tất cả keys
all_keys = manager.get_all_keys()
for key in all_keys:
    print(f"Key: {key['api_key'][:10]}..., Status: {key['status']}")
``` 