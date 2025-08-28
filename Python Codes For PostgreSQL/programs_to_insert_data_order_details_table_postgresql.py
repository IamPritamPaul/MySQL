import pandas as pd
import psycopg2
from psycopg2 import IntegrityError, DataError
import sys
from pathlib import Path

csv_file = Path(__file__).resolve().parents[1] / "CSV Files" / "OrderDetails.csv"
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
    print("Skipping inserts for order_details (no DB connection).")
    sys.exit(0)

# Create the order_details table if it doesn't exist
cursor = conn.cursor()
create_table_sql = """
CREATE TABLE IF NOT EXISTS order_details (
    OrderDetailID INTEGER PRIMARY KEY,
    OrderID INTEGER,
    ProductID INTEGER,
    Quantity INTEGER,
    FOREIGN KEY (OrderID) REFERENCES orders(OrderID),
    FOREIGN KEY (ProductID) REFERENCES products(ProductID)
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
    sql = "INSERT INTO order_details (OrderDetailID, OrderID, ProductID, Quantity) VALUES (%s, %s, %s, %s)"
    try:
        val = (
            int(_get(row, "OrderDetailID")),
            int(_get(row, "OrderID")),
            int(_get(row, "ProductID")),
            int(_get(row, "Quantity")),
        )
    except (TypeError, ValueError):
        # skip rows with missing or non-integer fields
        continue
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

print("Data inserted successfully into order_details table.")
