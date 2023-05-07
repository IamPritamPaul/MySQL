import pandas as pd
import numpy as np
import mysql.connector

# Load the .csv file into a pandas dataframe
df = pd.read_csv("Products.csv")
# df = df.astype({"OrderDetailID": int, "OrderID": int, "ProductID": int, "Quantity": int})
df = df.where(pd.notnull(df), None)
# print(df)

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
    sql = "INSERT INTO products (ProductID,ProductName,SupplierID,CategoryID,Unit,Price) VALUES (%s, %s, %s, %s, %s, %s)"
    val = (row['ProductID'], row['ProductName'], row['SupplierID'], row['CategoryID'], row['Unit'], row['Price'])
    cursor.execute(sql, val)
    # print(sql)

mydb.commit()
cursor.close()
mydb.close()
