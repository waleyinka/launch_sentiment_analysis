import os
from typing import Optional
from dotenv import load_dotenv


# Load environment variables from .env file
load_dotenv()


# ========================================
# DATABASE CONFIGURATION
# ========================================
DB_HOST: Optional[str] = os.getenv("PG_HOST")
DB_PORT: Optional[int] = os.getenv("PG_PORT")
DB_NAME: Optional[str] = os.getenv("PG_DB")
DB_USER: Optional[str] = os.getenv("PG_USER")
DB_PASSWORD: Optional[str] = os.getenv("PG_PASSWORD")


# ========================================
# WIKIMEDIA CONFIGURATION
# ========================================
WIKIMEDIA_BASE_URL = "https://dumps.wikimedia.org/other/pageviews/"


# ========================================
# LOGGING CONFIG
# ========================================
LOG_DIRECTORY = "launch_sentiment/logs"
LOG_FILE_NAME = "launch_sentiment.log"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")