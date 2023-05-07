import pandas as pd
import mysql.connector

# Load the .csv file into a pandas dataframe
df = pd.read_csv("Suppliers.csv")
# df = df.where(pd.notnull(df), None)

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
    sql = "INSERT INTO suppliers (SupplierId, SupplierName, ContactName, Address, City, PostalCode, Country, Phone) VALUES (%s, %s, %s, %s, %s, %s,%s, %s)"
    val = (int(row['SupplierId']), row['SupplierName'], row['ContactName'], row['Address'], row['City'], row['PostalCode'], row['Country'], row['Phone'])
    # try:
    #   val = (int(row['SupplierId']), row['SupplierName'], row['ContactName'], row['Address'], row['City'], row['PostalCode'], row['Country'], row['Phone'])
    #   print(val)
    # except Exception as e:
    #   print(f"error : {e}")
    try:
      cursor.execute(sql, val)
    except Exception as e:
      print(e)

mydb.commit()
cursor.close()
mydb.close()