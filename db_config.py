try:
    import psycopg2
except ImportError:
    # Friendly message to help the user install the missing dependency.
    import sys

    print("\nERROR: Required Python package 'psycopg2' is not installed.")
    print("Recommended: install the binary wheel which avoids build tools:")
    print("    pip install psycopg2-binary\n")
    print("If you intentionally use 'psycopg2' (source build), ensure build tools are available.")
    # Exit so the rest of the script doesn't fail with a raw traceback.
    sys.exit(1)


# Establish and return a PostgreSQL database connection.
def get_connection():
    try:
        conn = psycopg2.connect(
            dbname="Big_Data",
            user="postgres",
            password="dani0079",
            host="localhost",
            port="5432"
        )
        print("Database connection established successfully.")
        return conn
    except Exception as e:
        print("Database connection failed:", e)
        raise
