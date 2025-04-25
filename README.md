# YouTube Data Crawler

A Python application for crawling YouTube channel and video data using the YouTube Data API.

## Features

- Search for YouTube channels and videos by keywords
- Download channel avatars and banners
- Download video thumbnails
- Store data in MongoDB
- Save results in JSON format
- Support for multiple API keys with automatic rotation

## Project Structure

```
youtube-crawl/
├── src/
│   ├── api/           # API-related code
│   ├── utils/         # Utility functions
│   ├── models/        # Data models
│   ├── config/        # Configuration files
│   └── main.py        # Main application entry point
├── data/
│   ├── raw/          # Raw API responses
│   ├── processed/    # Processed data in JSON format
│   └── images/       # Downloaded images
│       ├── channels/ # Channel avatars and banners
│       └── videos/   # Video thumbnails
├── requirements.txt  # Python dependencies
├── setup.py         # Package installation script
└── README.md        # This file
```

## Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd youtube-crawl
```

2. Install the package in development mode:
```bash
pip install -e .
```

3. Create a file named `apikey.txt` in the project root and add your YouTube API keys (one per line)

4. Create a file named `keywords.txt` in the project root and add your search keywords (one per line)

5. Make sure MongoDB is running on localhost:27017

## Usage

Run the main script:
```bash
python src/main.py
```

The script will:
1. Read keywords from `keywords.txt`
2. Search for channels and videos for each keyword
3. Download channel avatars and banners
4. Download video thumbnails
5. Save results to MongoDB and JSON files

## Output

- Channel and video data is stored in MongoDB
- JSON files are saved in `data/processed/<date>/<keyword>.json`
- Images are saved in `data/images/channels/<date>/` and `data/images/videos/<date>/`

## Requirements

- Python 3.8+
- MongoDB
- YouTube Data API keys 