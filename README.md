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
