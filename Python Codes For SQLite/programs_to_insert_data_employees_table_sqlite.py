import pandas as pd
import sqlite3

# Load the .csv file into a pandas dataframe
df = pd.read_csv("../CSV Files/Employees.csv")
df = df.where(pd.notnull(df), None)

# Connect to the SQLite database
conn = sqlite3.connect(
    "/Users/iampritampaul/Documents/Coding/Python/Tutorials/Django/django_orm_tut_bugsbyte/db.sqlite3_bugsbyte"
)

# Create the employees table if it doesn't exist
cursor = conn.cursor()
create_table_sql = """
CREATE TABLE IF NOT EXISTS employees (
    EmployeeID INTEGER PRIMARY KEY,
    LastName TEXT,
    FirstName TEXT,
    BirthDate TEXT,
    Photo TEXT,
    Notes TEXT
)
"""
cursor.execute(create_table_sql)
conn.commit()

# Insert the data into the SQLite table
for index, row in df.iterrows():
    sql = "INSERT INTO employees (EmployeeID, LastName, FirstName, BirthDate, Photo, Notes) VALUES (?, ?, ?, ?, ?, ?)"
    val = (
        row["EmployeeID"],
        row["LastName"],
        row["FirstName"],
        row["BirthDate"],
        row["Photo"],
        row["Notes"],
    )
    cursor.execute(sql, val)

conn.commit()
conn.close()

print("Data inserted successfully into employees table.")
