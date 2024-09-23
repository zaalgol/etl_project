import os
from dotenv import load_dotenv

# Load variables from the .dev file
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path=dotenv_path)

# AWS Configuration
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
S3_BUCKET = os.getenv('S3_BUCKET')
S3_OUTPUT_PATH = f's3a://{S3_BUCKET}/output11/'

# API Configuration
API_URL = os.getenv('API_URL')

# Spark Configuration
SPARK_APP_NAME = os.getenv('SPARK_APP_NAME')
