import pandas as pd
import mysql.connector

# Load the .csv file into a pandas dataframe
df = pd.read_csv("Employees.csv")
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
    sql = "INSERT INTO employees (EmployeeID, LastName, FirstName, BirthDate, Photo, Notes) VALUES (%s, %s, %s, %s, %s, %s)"
    val = (row['EmployeeID'], row['LastName'], row['FirstName'], row['BirthDate'], row['Photo'], row['Notes'])
    cursor.execute(sql, val)

mydb.commit()
