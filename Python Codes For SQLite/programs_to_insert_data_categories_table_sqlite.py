import pandas as pd
import sqlite3

# Load the .csv file into a pandas dataframe
df = pd.read_csv("../CSV Files/Categories.csv")
df = df.where(pd.notnull(df), None)

# Connect to the SQLite database
conn = sqlite3.connect(
    "/Users/iampritampaul/Documents/Coding/Python/Tutorials/Django/django_orm_tut_bugsbyte/db.sqlite3_bugsbyte"
)

# Create the categories table if it doesn't exist
cursor = conn.cursor()
create_table_sql = """
CREATE TABLE IF NOT EXISTS categories (
    CategoryID INTEGER PRIMARY KEY,
    CategoryName TEXT,
    Description TEXT
)
"""
cursor.execute(create_table_sql)
conn.commit()

# Insert the data into the SQLite table
for index, row in df.iterrows():
    sql = "INSERT INTO categories (CategoryID, CategoryName, Description) VALUES (?, ?, ?)"
    val = (row["CategoryID"], row["CategoryName"], row["Description"])
    cursor.execute(sql, val)

conn.commit()
conn.close()

print("Data inserted successfully into categories table.")
