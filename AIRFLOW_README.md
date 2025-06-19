# YouTube Crawler với Airflow (PythonOperator)

Hướng dẫn chạy service crawl-data trên Airflow sử dụng PythonOperator - tối ưu cho việc deploy lên Kubernetes.

## Yêu cầu hệ thống

- Docker và Docker Compose
- Ít nhất 4GB RAM
- Ít nhất 2 CPU cores
- Ít nhất 10GB disk space

## Cấu trúc dự án

```
youtube-crawl/
├── docker-compose-airflow.yml    # Cấu hình Airflow với Docker Compose
├── dags/
│   └── youtube_crawler_dag.py    # DAG sử dụng PythonOperator
├── logs/                         # Logs của Airflow
├── plugins/                      # Plugins của Airflow
├── start_airflow.ps1            # Script khởi động Airflow
├── stop_airflow.ps1             # Script dừng Airflow
├── check_status.ps1             # Script kiểm tra trạng thái
├── airflow.env                  # File cấu hình môi trường
├── k8s-deployment-example.yaml  # Ví dụ deployment cho Kubernetes
└── AIRFLOW_README.md            # Hướng dẫn này
```

## Ưu điểm của PythonOperator

- **Kubernetes Ready**: Dễ dàng deploy lên K8s
- **Performance**: Chạy trực tiếp trong Airflow container, không cần Docker-in-Docker
- **Debugging**: Dễ debug và monitor hơn
- **Resource Efficiency**: Sử dụng ít tài nguyên hơn
- **Code Integration**: Code YouTube crawler được mount trực tiếp vào container

## Cách sử dụng

### 1. Khởi động Airflow

Chạy script PowerShell để khởi động Airflow:

```powershell
.\start_airflow.ps1
```

Script này sẽ:
- Khởi động các service Airflow (PostgreSQL, Web Server, Scheduler)
- Tự động cài đặt dependencies của YouTube crawler
- Mount code YouTube crawler vào container
- Hiển thị thông tin đăng nhập

### 2. Truy cập Airflow Web UI

- URL: http://localhost:8080
- Username: `admin`
- Password: `admin`

### 3. Kiểm tra DAG

Trong Airflow Web UI:
1. Vào tab "DAGs"
2. Tìm DAG có tên `youtube_crawler_dag`
3. Bật DAG bằng cách click vào toggle switch
4. DAG sẽ tự động chạy theo lịch trình (mỗi 6 giờ)

### 4. Chạy DAG thủ công

Để chạy DAG ngay lập tức:
1. Click vào DAG `youtube_crawler_dag`
2. Click nút "Trigger DAG"
3. Chọn "Trigger" để chạy

### 5. Xem logs

Để xem logs của các task:
1. Click vào task trong DAG
2. Click "Log" để xem chi tiết

### 6. Kiểm tra trạng thái

```powershell
.\check_status.ps1
```

### 7. Dừng Airflow

```powershell
.\stop_airflow.ps1
```

## Cấu hình

### Thay đổi lịch trình chạy

Trong file `dags/youtube_crawler_dag.py`, thay đổi `schedule_interval`:

```python
# Chạy mỗi giờ
schedule_interval=timedelta(hours=1)

# Chạy mỗi ngày lúc 2:00 AM
schedule_interval='0 2 * * *'

# Chạy mỗi 30 phút
schedule_interval=timedelta(minutes=30)
```

### Thay đổi tham số crawl

Trong DAG, bạn có thể thay đổi các tham số trong các function:

```python
# Trong crawl_data_task function
sys.argv = ['main.py', '--service', 'crawl-data', '--num-keywords', '5', '--max-workers', '10']
```

### Cấu hình MongoDB

Thay đổi thông tin MongoDB trong file `airflow.env` hoặc trong DAG:

```python
# Trong các task function
os.environ['MONGODB_URI'] = 'your_mongodb_uri'
os.environ['MONGODB_DB'] = 'your_database_name'
```

## Các task có sẵn

DAG này bao gồm các task:

1. **crawl_data**: Chạy service crawl-data chính
2. **reset_quota**: Reset quota (có thể bật/tắt)
3. **create_indexes**: Tạo indexes cho database (có thể bật/tắt)
4. **crawl_video**: Crawl videos từ channels đã crawl
5. **crawl_comment**: Crawl comments từ videos đã crawl

Để kích hoạt các task bổ sung, uncomment các dòng trong DAG:

```python
# crawl_data_operator >> reset_quota_operator
# crawl_data_operator >> create_indexes_operator
# crawl_data_operator >> crawl_video_operator
# crawl_video_operator >> crawl_comment_operator
```

## Deploy lên Kubernetes

### 1. Chuẩn bị

- Có cluster Kubernetes
- kubectl đã được cấu hình
- Helm (tùy chọn)

### 2. Sử dụng file example

File `k8s-deployment-example.yaml` cung cấp cấu hình cơ bản:

```bash
kubectl apply -f k8s-deployment-example.yaml
```

### 3. Sử dụng Helm (Khuyến nghị)

```bash
# Add Airflow Helm repository
helm repo add apache-airflow https://airflow.apache.org/charts
helm repo update

# Install Airflow
helm install airflow apache-airflow/airflow \
  --set webserver.defaultUser.enabled=true \
  --set webserver.defaultUser.username=admin \
  --set webserver.defaultUser.password=admin \
  --set webserver.defaultUser.email=admin@example.com \
  --set webserver.defaultUser.firstName=admin \
  --set webserver.defaultUser.lastName=admin \
  --set webserver.defaultUser.role=Admin
```

### 4. Cấu hình DAG cho K8s

Trong Kubernetes, bạn có thể:
- Sử dụng ConfigMap để mount code
- Sử dụng PersistentVolume cho logs
- Sử dụng Secrets cho sensitive data
- Scale scheduler và webserver độc lập

## Troubleshooting

### Kiểm tra trạng thái services

```powershell
docker-compose -f docker-compose-airflow.yml ps
```

### Xem logs

```powershell
docker-compose -f docker-compose-airflow.yml logs -f
```

### Xem logs của service cụ thể

```powershell
docker-compose -f docker-compose-airflow.yml logs -f airflow-scheduler
docker-compose -f docker-compose-airflow.yml logs -f airflow-webserver
```

### Restart service

```powershell
docker-compose -f docker-compose-airflow.yml restart airflow-scheduler
```

### Xóa và tạo lại database

```powershell
docker-compose -f docker-compose-airflow.yml down -v
docker-compose -f docker-compose-airflow.yml up -d
```

### Kiểm tra dependencies

Nếu có lỗi về dependencies, kiểm tra:

```powershell
docker-compose -f docker-compose-airflow.yml exec airflow-scheduler pip list
```

## Lưu ý

- **Code Mount**: Code YouTube crawler được mount trực tiếp vào `/opt/airflow/youtube-crawler`
- **Dependencies**: Tự động cài đặt qua `_PIP_ADDITIONAL_REQUIREMENTS`
- **Environment**: Biến môi trường được set trong từng task
- **K8s Ready**: Cấu hình sẵn sàng cho Kubernetes deployment
- **Monitoring**: Logs được tích hợp với Airflow logging system
- **Scalability**: Có thể scale scheduler và webserver độc lập trên K8s

## Migration từ DockerOperator

Nếu bạn đang sử dụng DockerOperator, việc chuyển sang PythonOperator sẽ:

1. **Tăng performance**: Không cần Docker-in-Docker
2. **Giảm complexity**: Ít layer hơn
3. **Dễ debug**: Logs trực tiếp trong Airflow
4. **K8s friendly**: Không cần Docker socket
5. **Resource efficient**: Sử dụng ít memory và CPU hơn 