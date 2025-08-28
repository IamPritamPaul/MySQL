import pandas as pd
import sqlite3

# Load the .csv file into a pandas dataframe
df = pd.read_csv("../CSV Files/Orders.csv")
df = df.where(pd.notnull(df), None)

# Connect to the SQLite database
conn = sqlite3.connect(
    "/Users/iampritampaul/Documents/Coding/Python/Tutorials/Django/django_orm_tut_bugsbyte/db.sqlite3_bugsbyte"
)

# Create the orders table if it doesn't exist
cursor = conn.cursor()
create_table_sql = """
CREATE TABLE IF NOT EXISTS orders (
    OrderID INTEGER PRIMARY KEY,
    CustomerID INTEGER,
    EmployeeID INTEGER,
    OrderDate TEXT,
    ShipperID INTEGER
)
"""
cursor.execute(create_table_sql)
conn.commit()

# Insert the data into the SQLite table
for index, row in df.iterrows():
    sql = "INSERT INTO orders (OrderID, CustomerID, EmployeeID, OrderDate, ShipperID) VALUES (?, ?, ?, ?, ?)"
    val = (
        row["OrderID"],
        row["CustomerID"],
        row["EmployeeID"],
        row["OrderDate"],
        row["ShipperID"],
    )
    cursor.execute(sql, val)

conn.commit()
conn.close()

print("Data inserted successfully into orders table.")
