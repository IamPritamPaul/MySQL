import pandas as pd
import psycopg2
from psycopg2 import IntegrityError, DataError
import sys
from pathlib import Path

csv_file = Path(__file__).resolve().parents[1] / "CSV Files" / "Suppliers.csv"
if not csv_file.exists():
    print(f"CSV file not found: {csv_file}")
    sys.exit(0)

# Load the .csv file into a pandas dataframe
df = pd.read_csv(csv_file)
df = df.where(pd.notnull(df), None)

csv_file = Path(__file__).resolve().parents[1] / "CSV Files" / "Suppliers.csv"
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
    print("Skipping inserts for suppliers (no DB connection).")
    sys.exit(0)

# Create the suppliers table if it doesn't exist
cursor = conn.cursor()
create_table_sql = """
CREATE TABLE IF NOT EXISTS suppliers (
    SupplierID INTEGER PRIMARY KEY,
    SupplierName VARCHAR(255),
    ContactName VARCHAR(255),
    Address TEXT,
    City VARCHAR(255),
    PostalCode VARCHAR(50),
    Country VARCHAR(100),
    Phone VARCHAR(50)
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
    sql = "INSERT INTO suppliers (SupplierID, SupplierName, ContactName, Address, City, PostalCode, Country, Phone) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    val = (
        _get(row, "SupplierID", "SupplierId"),
        _get(row, "SupplierName"),
        _get(row, "ContactName"),
        _get(row, "Address"),
        _get(row, "City"),
        _get(row, "PostalCode"),
        _get(row, "Country"),
        _get(row, "Phone"),
    )
    try:
        cursor.execute(sql, val)
    except IntegrityError:
        conn.rollback()
        # skip duplicate primary key or constraint errors
        continue
    except DataError:
        conn.rollback()
        # skip rows with bad types
        continue

conn.commit()
cursor.close()
conn.close()

print("Data inserted successfully into suppliers table.")
