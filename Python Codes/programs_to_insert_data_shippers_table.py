import pandas as pd
import mysql.connector

# Load the .csv file into a pandas dataframe
df = pd.read_csv("Shippers.csv")

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
    sql = "INSERT INTO shippers (ShipperID,ShipperName,Phone) VALUES (%s, %s, %s)"
    val = (row['ShipperID'], row['ShipperName'], row['Phone'])
    cursor.execute(sql, val)

mydb.commit()
