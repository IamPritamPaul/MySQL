import pandas as pd
import mysql.connector

# Load the .csv file into a pandas dataframe
df = pd.read_csv("Orders.csv")

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
    sql = "INSERT INTO orders (OrderID,CustomerID,EmployeeID,OrderDate,ShipperID) VALUES (%s, %s, %s, %s, %s)"
    val = (row['OrderID'], row['CustomerID'], row['EmployeeID'], row['OrderDate'], row['ShipperID'])
    cursor.execute(sql, val)

mydb.commit()
