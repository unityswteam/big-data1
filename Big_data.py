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




# STEP 3: EXTRACT + DEDUPLICATE IN MEMORY

csv_path = "sales_transactions_3200000.csv"
print(" Reading full CSV into memory...")

# Load full dataset once
df = pd.read_csv(csv_path)
print(f"Total rows before deduplication: {len(df)}")

# Remove duplicates globally (no saving to file)
df.drop_duplicates(inplace=True)
print(f" Removed {len(df)} duplicate rows globally. Remaining: {len(df)}")


# STEP 4: TRANSFORM (Fill Missing Values, Fix Totals)

for col in df.columns:
    if df[col].isnull().sum() > 0:
        if df[col].dtype in ["float64", "int64"]:
            mean_value = df[col].mean()
            df[col].fillna(mean_value, inplace=True)
            print(f"Filled missing numeric '{col}' with mean: {mean_value:.2f}")
        else:
            mode_value = df[col].mode()[0] if not df[col].mode().empty else "Unknown"
            df[col].fillna(mode_value, inplace=True)
            print(f"Filled missing text '{col}' with mode: {mode_value}")

# Ensure total = price × quantity
if {'price', 'quantity', 'total'}.issubset(df.columns):
    df['total'] = df.apply(
        lambda r: r['price'] * r['quantity'] if pd.isnull(r['total']) or r['total'] == 0 else r['total'],
        axis=1
    )
