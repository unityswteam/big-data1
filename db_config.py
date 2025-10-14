import psycopg2

# Establish and return a PostgreSQL database connection.
def get_connection():
    try:
        conn = psycopg2.connect(
            dbname="Big_Data",
            user="postgres",
            password="postabnet",
            host="localhost",
            port="5432"
        )
        print("Database connection established successfully.")
        return conn
    except Exception as e:
        print(" Database connection failed:", e)
        raise
    