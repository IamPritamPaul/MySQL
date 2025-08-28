import pandas as pd
import psycopg2
from psycopg2 import IntegrityError, DataError
import sys
from pathlib import Path

# Robust CSV path
csv_file = Path(__file__).resolve().parents[1] / "CSV Files" / "Employees.csv"
if not csv_file.exists():
    print(f"CSV file not found: {csv_file}")
    sys.exit(0)

# Load the .csv file into a pandas dataframe
df = pd.read_csv(csv_file)
df = df.where(pd.notnull(df), None)

# Connect to the PostgreSQL database (fail gracefully)
try:
    conn = psycopg2.connect(
        dbname="w3schools",
        user="postgres",
        password="password",
        host="localhost",
        port="5433",
    )
except Exception as e:
    print(f"PostgreSQL connection failed: {e}")
    print("Skipping inserts for employees (no DB connection).")
    sys.exit(0)

# Create the employees table if it doesn't exist
cursor = conn.cursor()
create_table_sql = """
CREATE TABLE IF NOT EXISTS employees (
    EmployeeID INTEGER PRIMARY KEY,
    LastName VARCHAR(255),
    FirstName VARCHAR(255),
    BirthDate DATE,
    Photo VARCHAR(255),
    Notes TEXT
)
"""
cursor.execute(create_table_sql)
conn.commit()


# Insert the data into the PostgreSQL table
def _get(row, *keys):
    for k in keys:
        if k in row.index:
            return row[k]
    return None


for index, row in df.iterrows():
    sql = "INSERT INTO employees (EmployeeID, LastName, FirstName, BirthDate, Photo, Notes) VALUES (%s, %s, %s, %s, %s, %s)"
    val = (
        _get(row, "EmployeeID"),
        _get(row, "LastName"),
        _get(row, "FirstName"),
        _get(row, "BirthDate"),
        _get(row, "Photo"),
        _get(row, "Notes"),
    )
    try:
        cursor.execute(sql, val)
    except IntegrityError:
        conn.rollback()
        continue
    except DataError:
        conn.rollback()
        continue

conn.commit()
cursor.close()
conn.close()

print("Data inserted successfully into employees table.")
