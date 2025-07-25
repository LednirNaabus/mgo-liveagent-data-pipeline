"""
Main configuration file for the project.
"""
from dotenv import load_dotenv
import os

load_dotenv()

# TO DO: Add API key validation
LIVEAGENT_API_KEY = os.getenv('LIVEAGENT_API_KEY')