# 🧠 Big Data ETL Pipeline (PostgreSQL + Pandas + Python)

## 📋 Overview
This project demonstrates a **simple but scalable ETL (Extract, Transform, Load)** pipeline using **Python**, **Pandas**, and **PostgreSQL**.

It reads a large CSV dataset (`sales_transactions_3200000.csv`), cleans and deduplicates the data in memory, fills in missing values, computes totals, and loads the cleaned dataset into a PostgreSQL table efficiently in batches.

---

## ⚙️ Features
✅ Connects automatically to a PostgreSQL database  
✅ Creates a target table if it doesn’t exist  
✅ Reads a multi-million-row CSV file using **pandas**  
✅ Deduplicates records globally (in-memory)  
✅ Fills missing numeric values with the **mean**, and text fields with the **mode**  
✅ Recomputes `total = price × quantity` when missing or incorrect  
✅ Loads data efficiently in chunks using `psycopg2.extras.execute_batch()`  
✅ Skips duplicate `transaction_id` values using `ON CONFLICT DO NOTHING`

---

## 📁 Project Structure
Big_data/
│
├── Big_data.py # Main ETL script
├── db_config.py # Database connection setup
├── sales_transactions_3200000.csv # Input dataset (not included in repo)
└── README.md # Project documentation
#🗄️ Database Configuration

Update db_config.py with your PostgreSQL connection details:
import psycopg2

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
        print("Database connection failed:", e)
        raise
