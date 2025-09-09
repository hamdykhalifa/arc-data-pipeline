import os
from dotenv import load_dotenv

load_dotenv()

def get_config():
    return {
        'API_BASE_URL': os.getenv('API_BASE_URL'),
        # Optional filter
        'API_USER_ID': os.getenv('API_USER_ID'),  
    }