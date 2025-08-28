import pandas as pd
import psycopg2
from psycopg2 import IntegrityError, DataError
import sys
from pathlib import Path

csv_file = Path(__file__).resolve().parents[1] / "CSV Files" / "Orders.csv"
if not csv_file.exists():
    print(f"CSV file not found: {csv_file}")
    sys.exit(0)

# Load the .csv file into a pandas dataframe
df = pd.read_csv(csv_file)
df = df.where(pd.notnull(df), None)

# Connect to the PostgreSQL database
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
    print("Skipping inserts for orders (no DB connection).")
    sys.exit(0)

# Create the orders table if it doesn't exist
cursor = conn.cursor()
create_table_sql = """
CREATE TABLE IF NOT EXISTS orders (
    OrderID INTEGER PRIMARY KEY,
    CustomerID INTEGER,
    EmployeeID INTEGER,
    OrderDate DATE,
    ShipperID INTEGER,
    FOREIGN KEY (CustomerID) REFERENCES customers(CustomerID),
    FOREIGN KEY (EmployeeID) REFERENCES employees(EmployeeID),
    FOREIGN KEY (ShipperID) REFERENCES shippers(ShipperID)
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
    sql = "INSERT INTO orders (OrderID, CustomerID, EmployeeID, OrderDate, ShipperID) VALUES (%s, %s, %s, %s, %s)"
    val = (
        _get(row, "OrderID"),
        _get(row, "CustomerID"),
        _get(row, "EmployeeID"),
        _get(row, "OrderDate"),
        _get(row, "ShipperID"),
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

print("Data inserted successfully into orders table.")
