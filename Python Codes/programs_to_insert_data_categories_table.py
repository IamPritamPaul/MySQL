import pandas as pd
import mysql.connector

# Load the .csv file into a pandas dataframe
df = pd.read_csv("Categories.csv")

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
    sql = "INSERT INTO categories (CategoryID, CategoryName, Description) VALUES (%s, %s, %s)"
    val = (row['CategoryID'], row['CategoryName'], row['Description'])
    cursor.execute(sql, val)
    
# MYSQL Command to create the table insode the databsae 
# CREATE TABLE categories (CategoryID INT, CategoryName VARCHAR(255), Description VARCHAR(255))"
mydb.commit()
