"""
Main configuration file for the project.
"""
import os
from dotenv import load_dotenv

load_dotenv()

LIVEAGENT_API_KEY = os.getenv('LIVEAGENT_API_KEY')