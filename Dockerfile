# Use Python 3.9 as base image
FROM python:3.9-slim

# Set working directory
WORKDIR /youtube-crawler

ENV MONGODB_URI=mongodb://192.168.161.230:27011,192.168.161.230:27012,192.168.161.230:27013/?replicaSet=rs0
ENV MONGODB_DB=youtube_data

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy only requirements first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies with no cache and parallel processing
RUN pip install --no-cache-dir -r requirements.txt


# Copy the rest of the application
COPY . .

# Create necessary directories
RUN mkdir -p /mnt/data/youtube/images/channels \
    /mnt/data/youtube/images/thumbnailvideos \
    /mnt/data/youtube/logs

# Set environment variables
ENV PYTHONPATH=/youtube-crawler

# Command to run the application
ENTRYPOINT ["python", "src/main.py"]
CMD ["--service", "crawl-data", "--num-keywords", "1"] 