"""
Main configuration file for the project.
"""
from dotenv import load_dotenv
import pytz
import os

load_dotenv()

LIVEAGENT_API_KEY = os.getenv('LIVEAGENT_API_KEY')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')

missing_keys = []
if not LIVEAGENT_API_KEY:
    missing_keys.append("LIVEAGENT_API_KEY")
if not OPENAI_API_KEY:
    missing_keys.append("OPENAI_API_KEY")
if not GEMINI_API_KEY:
    missing_keys.append("GEMINI_API_KEY")

if missing_keys:
    print(f"Missing API keys: {', '.join(missing_keys)}")

MNL_TZ = pytz.timezone("Asia/Manila")