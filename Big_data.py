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
    flow_id SERIAL PRIMARY KEY,
    destination_port INT,
    flow_duration BIGINT,
    total_fwd_packets BIGINT,
    total_length_of_fwd_packets BIGINT,
    fwd_packet_length_max BIGINT,
    fwd_packet_length_min BIGINT,
    bwd_packet_length_max BIGINT,
    bwd_packet_length_min BIGINT,
    flow_bytes DOUBLE PRECISION,
    flow_packets DOUBLE PRECISION,
    flow_iat_mean DOUBLE PRECISION,
    flow_iat_std DOUBLE PRECISION,
    flow_iat_min BIGINT,
    bwd_iat_total BIGINT,
    bwd_iat_mean DOUBLE PRECISION,
    bwd_iat_std DOUBLE PRECISION,
    bwd_iat_max BIGINT,
    fwd_psh_flags BIGINT,
    fwd_urg_flags BIGINT,
    bwd_packets DOUBLE PRECISION,
    min_packet_length BIGINT,
    fin_flag_count BIGINT,
    rst_flag_count BIGINT,
    psh_flag_count BIGINT,
    ack_flag_count BIGINT,
    urg_flag_count BIGINT,
    downup_ratio BIGINT,
    init_win_bytes_forward BIGINT,
    init_win_bytes_backward BIGINT,
    active_mean DOUBLE PRECISION,
    active_std DOUBLE PRECISION,
    active_max BIGINT,
    idle_std DOUBLE PRECISION,
    attack_type BIGINT
);
"""

cursor.execute(create_table_query)
conn.commit()
print(f"Table '{table_name}' created successfully.")


# Load the file and Extract
csv_path = 'cic_ids_2017.csv'
chunk_size = 100000
print("Reading full CSV in chunks...")

chunks = []
for chunk in pd.read_csv(csv_path, chunksize=chunk_size):
    chunks.append(chunk)
df = pd.concat(chunks, ignore_index=True)

print(df.info())
before = len(df)
print(f"Total rows before duplicated rows are removed: {before}")

# Finding missing_values for all features

missing_values = df.isnull().sum()
missing_cols = []
missing_values.sort_values(ascending=False).plot(kind='bar', figsize=(12, 6), color='tomato')
# plt.title("Missing Values Per Feature")
# plt.ylabel("Number of Missing Values")
# plt.xticks(rotation=90)
# plt.grid(True)
# plt.show()

for col in df.columns:
    if df[col].isnull().any():
        print(f"Column '{col}' has missing values. {df[col].isnull().sum()} with {df[col].dtype} data_type")
        missing_cols.append(col)

# Missing values treatment

if missing_values.sum() > 0:
    print("Missing values found. Proceeding with imputation.")
    for col in missing_cols:
        if df[col].dtype == 'object':
          df[col] = df[col].fillna(df[col].mode()[0])
        else:
          df[col] = df[col].fillna(df[col].mean())
else:
    print("No missing values found.")

# Check for duplicates for all dataset features
duplicates = df[df.duplicated(keep=False)]
if not duplicates.empty:
    print(f"{len(duplicates)} duplicate rows found on the dataset")
else:
    print("\nNo duplicates found.")
print(f"Current shape of the dataset: {df.shape}")

# Remove duplicated rows if any
if len(duplicates) > 0:
    print(f"Removing {len(duplicates)} duplicated rows...")
    df.drop_duplicates(inplace=True)
    print(f"Current shape of the dataset: {df.shape}")

# Load into postgreSQL in chunks
chunk_size = 100000
print("Loading data into PostgreSQL...")

insert_query = f"""
INSERT INTO {table_name} (
    destination_port,
    flow_duration,
    total_fwd_packets,
    total_length_of_fwd_packets,
    fwd_packet_length_max,
    fwd_packet_length_min,
    bwd_packet_length_max,
    bwd_packet_length_min,
    flow_bytes,
    flow_packets,
    flow_iat_mean,
    flow_iat_std,
    flow_iat_min,
    bwd_iat_total,
    bwd_iat_mean,
    bwd_iat_std,
    bwd_iat_max,
    fwd_psh_flags,
    fwd_urg_flags,
    bwd_packets,
    min_packet_length,
    fin_flag_count,
    rst_flag_count,
    psh_flag_count,
    ack_flag_count,
    urg_flag_count,
    downup_ratio,
    init_win_bytes_forward,
    init_win_bytes_backward,
    active_mean,
    active_std,
    active_max,
    idle_std,
    attack_type
)
VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s
);
"""

for i in range(0, len(df), chunk_size):
    chunk = df.iloc[i:i + chunk_size]
    records = [tuple(x) for x in chunk.astype(object).to_numpy()]
    execute_batch(cursor, insert_query, records)
    conn.commit()
    print(f" Loaded chunk {i // chunk_size + 1} ({len(records)} rows) into PostgreSQL")

# close connections
cursor.close()
conn.close()
print("\nETL Process completed successfully without creating extra files!")
