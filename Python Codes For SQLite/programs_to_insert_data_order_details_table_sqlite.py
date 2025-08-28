import pandas as pd
import sqlite3

# Load the .csv file into a pandas dataframe
df = pd.read_csv("../CSV Files/OrderDetails.csv")
df = df.where(pd.notnull(df), None)

# Connect to the SQLite database
conn = sqlite3.connect(
    "/Users/iampritampaul/Documents/Coding/Python/Tutorials/Django/django_orm_tut_bugsbyte/db.sqlite3_bugsbyte"
)

# Create the order_details table if it doesn't exist
cursor = conn.cursor()
create_table_sql = """
CREATE TABLE IF NOT EXISTS order_details (
    OrderDetailID INTEGER PRIMARY KEY,
    OrderID INTEGER,
    ProductID INTEGER,
    Quantity INTEGER
)
"""
cursor.execute(create_table_sql)
conn.commit()

# Insert the data into the SQLite table
for index, row in df.iterrows():
    sql = "INSERT INTO order_details (OrderDetailID, OrderID, ProductID, Quantity) VALUES (?, ?, ?, ?)"
    val = (
        int(row["OrderDetailID"]),
        int(row["OrderID"]),
        int(row["ProductID"]),
        int(row["Quantity"]),
    )
    cursor.execute(sql, val)

conn.commit()
conn.close()

print("Data inserted successfully into order_details table.")
