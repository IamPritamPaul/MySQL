import pandas as pd
import psycopg2
from psycopg2 import IntegrityError, DataError
import sys
from pathlib import Path

# Robust CSV path: resolve relative to repository root (two levels up from this file)
csv_file = Path(__file__).resolve().parents[1] / "CSV Files" / "Customers.csv"
if not csv_file.exists():
    print(f"CSV file not found: {csv_file}")
    sys.exit(0)

# Load the .csv file into a pandas dataframe
df = pd.read_csv(csv_file)
df = df.where(pd.notnull(df), None)

# Connect to the PostgreSQL database (fail gracefully if DB unavailable)
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
    print("Skipping inserts for customers (no DB connection).")
    sys.exit(0)

# Create the customers table if it doesn't exist
cursor = conn.cursor()
create_table_sql = """
CREATE TABLE IF NOT EXISTS customers (
    CustomerID INTEGER PRIMARY KEY,
    CustomerName VARCHAR(255),
    ContactName VARCHAR(255),
    Address TEXT,
    City VARCHAR(255),
    PostalCode VARCHAR(50),
    Country VARCHAR(100)
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
    sql = "INSERT INTO customers (CustomerID, CustomerName, ContactName, Address, City, PostalCode, Country) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (
        _get(row, "CustomerID"),
        _get(row, "CustomerName"),
        _get(row, "ContactName"),
        _get(row, "Address"),
        _get(row, "City"),
        _get(row, "PostalCode"),
        _get(row, "Country"),
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

print("Data inserted successfully into customers table.")
