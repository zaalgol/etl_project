import os
from dotenv import load_dotenv

# Get the project root directory
project_root = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the .env file in the root directory
dotenv_path = os.path.join(project_root, '..', '.env') 

# Load variables from the .env file
load_dotenv(dotenv_path=dotenv_path)

# AWS Configuration
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
S3_BUCKET = os.getenv('S3_BUCKET')
S3_OUTPUT_PATH = f's3a://{S3_BUCKET}/output/'

# API Configuration
API_URL = os.getenv('API_URL')

# Spark Configuration
SPARK_APP_NAME = os.getenv('SPARK_APP_NAME')
SPARK_LOCAL_DIR = os.getenv('SPARK_LOCAL_DIR')

