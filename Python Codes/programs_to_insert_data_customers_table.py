import pandas as pd
import mysql.connector

# Load the .csv file into a pandas dataframe
df = pd.read_csv("Customers.csv")
df = df.where(pd.notnull(df), None)
# Connect to the MySQL database
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="password",
  database="w3schools"
)

# Insert the data into the MySQL table
cursor = mydb.cursor()
for index, row in df.iterrows():
    sql = "INSERT INTO customers (CustomerID, CustomerName, ContactName, Address, City, PostalCode, Country) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (row['CustomerID'], row['CustomerName'], row['ContactName'], row['Address'], row['City'], row['PostalCode'], row['Country'])
    cursor.execute(sql, val)

mydb.commit()
