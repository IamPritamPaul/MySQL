import pandas as pd
import sqlite3

# Load the .csv file into a pandas dataframe
df = pd.read_csv("../CSV Files/Shippers.csv")
df = df.where(pd.notnull(df), None)

# Connect to the SQLite database
conn = sqlite3.connect(
    "/Users/iampritampaul/Documents/Coding/Python/Tutorials/Django/django_orm_tut_bugsbyte/db.sqlite3_bugsbyte"
)

# Create the shippers table if it doesn't exist
cursor = conn.cursor()
create_table_sql = """
CREATE TABLE IF NOT EXISTS shippers (
    ShipperID INTEGER PRIMARY KEY,
    ShipperName TEXT,
    Phone TEXT
)
"""
cursor.execute(create_table_sql)
conn.commit()

# Insert the data into the SQLite table
for index, row in df.iterrows():
    sql = "INSERT INTO shippers (ShipperID, ShipperName, Phone) VALUES (?, ?, ?)"
    val = (row["ShipperID"], row["ShipperName"], row["Phone"])
    cursor.execute(sql, val)

conn.commit()
conn.close()

print("Data inserted successfully into shippers table.")
