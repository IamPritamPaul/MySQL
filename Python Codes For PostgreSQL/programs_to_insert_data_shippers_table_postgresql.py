import pandas as pd
import psycopg2
from psycopg2 import IntegrityError, DataError
import sys
from pathlib import Path

csv_file = Path(__file__).resolve().parents[1] / "CSV Files" / "Shippers.csv"
if not csv_file.exists():
    print(f"CSV file not found: {csv_file}")
    sys.exit(0)

# Load the .csv file into a pandas dataframe
df = pd.read_csv(csv_file)
df = df.where(pd.notnull(df), None)

csv_file = Path(__file__).resolve().parents[1] / "CSV Files" / "Shippers.csv"
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
    print("Skipping inserts for shippers (no DB connection).")
    sys.exit(0)

# Create the shippers table if it doesn't exist
cursor = conn.cursor()
create_table_sql = """
CREATE TABLE IF NOT EXISTS shippers (
    ShipperID INTEGER PRIMARY KEY,
    ShipperName VARCHAR(255),
    Phone VARCHAR(50)
)
"""
cursor.execute(create_table_sql)
conn.commit()


def _get(row, *keys):
    for k in keys:
        if k in row.index:
            return row[k]
    return None


# Insert the data into the PostgreSQL table
for index, row in df.iterrows():
    sql = "INSERT INTO shippers (ShipperID, ShipperName, Phone) VALUES (%s, %s, %s)"
    val = (_get(row, "ShipperID"), _get(row, "ShipperName"), _get(row, "Phone"))
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

print("Data inserted successfully into shippers table.")
