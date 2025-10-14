# big-data1

Small project that connects to a PostgreSQL database and creates a table.

Quick start (Windows PowerShell):

1. Create and activate a virtual environment

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

2. Install dependencies

```powershell
pip install -r requirements.txt
```

3. Configure your PostgreSQL credentials if they differ (file: `db_config.py`).

4. Run the script

```powershell
python Big_data.py
```

If you see an error about `psycopg2` missing, run:

```powershell
pip install psycopg2-binary
```
