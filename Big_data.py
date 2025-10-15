from db_config import get_connection
from psycopg2.extras import execute_batch
import pandas as pd

# Prompt user for table name
table_name = input("Enter the table name to use for loading data: ").strip()

conn = get_connection()
cursor = conn.cursor()

# create table (if not exists)
create_table_query = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
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
print(f"Table '{table_name}' created successfully.")


# Load the file and Extract
csv_path = "sales_transactions_3200000.csv"
chunk_size = 100000
print("Reading full CSV in chunks...")

chunks = []
for chunk in pd.read_csv(csv_path, chunksize=chunk_size):
    chunks.append(chunk)
df = pd.concat(chunks, ignore_index=True)

before = len(df)
print(f"Total rows before duplicated rows are removed: {before}")

# Remove duplicates globally (no saving to file)
df.drop_duplicates(inplace=True)
after = len(df)
print(f"Removed {before - after} duplicate rows globally. Remaining: {after}")

# Ensure total = price × quantity
if {'price', 'quantity', 'total'}.issubset(df.columns):
    df['total'] = df.apply(
        lambda r: r['price'] * r['quantity'] if pd.isnull(r['total']) or r['total'] == 0 else r['total'],
        axis=1
    )
    
# Fill missing values, inconsistencies, and transformations
for col in df.columns:
    if df[col].isnull().sum() > 0:
        if df[col].dtype in ["float64", "int64"]:
            mean_value = df[col].mean()
            df[col] = df[col].fillna(mean_value)
            print(f"Filled missing numeric '{col}' with mean: {mean_value:.2f}")
        else:
            mode_value = df[col].mode()[0] if not df[col].mode().empty else "Unknown"
            df[col] = df[col].fillna(mode_value)
            print(f"Filled missing text '{col}' with mode: {mode_value}")

# Load into postgreSQL in chunks
chunk_size = 100000
print("Loading data into PostgreSQL...")

insert_query = f"""
INSERT INTO {table_name} (
    transaction_id, date, product, category, price, quantity, total, payment_method, country
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (transaction_id) DO NOTHING;
"""

for i in range(0, len(df), chunk_size):
    chunk = df.iloc[i:i + chunk_size]
    records = [tuple(x) for x in chunk.to_numpy()]
    execute_batch(cursor, insert_query, records)
    conn.commit()
    print(f" Loaded chunk {i // chunk_size + 1} ({len(records)} rows) into PostgreSQL")

# close connections
cursor.close()
conn.close()
print("\nETL Process completed successfully without creating extra files!")
