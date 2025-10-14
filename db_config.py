# Establish and return a PostgreSQL database connection.
import os
from dotenv import load_dotenv
import psycopg2

# Load environment variables from .env file
load_dotenv()

def get_connection():
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )
        print("Database connection established successfully.")
        return conn
    except Exception as e:
        print("Database connection failed:", e)
        raise
