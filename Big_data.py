from db_config import get_connection

conn = get_connection()
cursor = conn.cursor()

# create table (if not exists)
create_table_query = """
CREATE TABLE IF NOT EXISTS sales_transactions (
    transaction_id INT PRIMARY KEY,
    date VARCHAR(20),
    product VARCHAR(100),
    category VARCHAR(50),
    price FLOAT,
    quantity INT,
    total FLOAT,
    payment_method VARCHAR(50),
    country VARCHAR(50)
);
"""
cursor.execute(create_table_query)
conn.commit()
print("✅ Table created successfully.")
