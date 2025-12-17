"""Configuration management"""
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
BRIGHTSKY_API_URL = os.getenv("BRIGHTSKY_API_URL", "https://api.brightsky.dev")
