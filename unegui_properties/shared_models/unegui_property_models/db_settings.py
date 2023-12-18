import os
from dotenv import load_dotenv
from sqlalchemy.engine.url import URL
load_dotenv()

# Define the database configuration using environment variables
DATABASE = {
    'drivername': os.getenv('DB_DRIVERNAME', 'postgresql'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'username': os.getenv('DB_USERNAME', 'postgres'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_DATABASE', 'turbo_car')
}

