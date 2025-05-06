# Use Python 3.9 as base image
FROM python:3.9-slim

# Set working directory
WORKDIR /youtube-crawler

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
RUN mkdir -p data/images/channels data/images/thumbnailvideos

# Set environment variables
ENV PYTHONPATH=/youtube-crawler
ENV PYTHONUNBUFFERED=1

# Command to run the application
CMD ["python", "src/generate_keywords.py"] 