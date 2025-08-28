import pandas as pd
import sqlite3

# Load the .csv file into a pandas dataframe
# Make sure the CSV file is in the same directory as this script, or provide the full path.
df = pd.read_csv("../CSV Files/Customers.csv")
df = df.where(pd.notnull(df), None)

# Connect to the SQLite database
conn = sqlite3.connect(
    "/Users/iampritampaul/Documents/Coding/Python/Tutorials/Django/django_orm_tut_bugsbyte/db.sqlite3_bugsbyte"
)

# Create the customers table if it doesn't exist
cursor = conn.cursor()
create_table_sql = """
CREATE TABLE IF NOT EXISTS customers (
    CustomerID INTEGER PRIMARY KEY,
    CustomerName TEXT,
    ContactName TEXT,
    Address TEXT,
    City TEXT,
    PostalCode TEXT,
    Country TEXT
)
"""
cursor.execute(create_table_sql)
conn.commit()

# Insert the data into the SQLite table
for index, row in df.iterrows():
    sql = "INSERT INTO customers (CustomerID, CustomerName, ContactName, Address, City, PostalCode, Country) VALUES (?, ?, ?, ?, ?, ?, ?)"
    val = (
        row["CustomerID"],
        row["CustomerName"],
        row["ContactName"],
        row["Address"],
        row["City"],
        row["PostalCode"],
        row["Country"],
    )
    try:
        cursor.execute(sql, val)
    except sqlite3.IntegrityError:
        # Skip duplicate entries
        continue

conn.commit()
conn.close()

print("Data inserted successfully.")
