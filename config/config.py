import os
from dotenv import load_dotenv

load_dotenv()

def str_to_bool(val):
    return str(val).strip().lower() in {'1', 'true', 'yes', 'on'}

def get_config():
    part_cols = os.getenv("PARTITION_COLS", "")
    part_cols = [x.strip() for x in part_cols.split(",") if x.strip()]
    return {
        'API_BASE_URL': os.getenv('API_BASE_URL'),
        # Optional filter
        'API_USER_ID': os.getenv('API_USER_ID'), 
        'USE_S3': str_to_bool(os.getenv("USE_S3", "False")), # if set to True in .env variables, upload to AWS
        'AWS_S3_BUCKET': os.getenv("AWS_S3_BUCKET"),
        'S3_PREFIX': os.getenv("S3_PREFIX"),
        'AWS_REGION': os.getenv("AWS_DEFAULT_REGION"),
        'OUTPUT_DIR': os.getenv("OUTPUT_DIR"),
        'PARTITION_COLS': part_cols, # can be comma sperated in .env file
    }