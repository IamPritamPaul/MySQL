import csv as csv
import pandas as pd
import mysql.connector

df=pd.read_csv("OrderDetails.csv")


mydb=mysql.connector.connect(
    host="localhost",
    user="root",
    password="password",
    database="w3schools"
)

cursor=mydb.cursor()
for index,row in df.iterrows():
    sql="INSERT INTO order_details (OrderDetailID,OrderID,ProductID,Quantity) VALUES (%s,%s,%s,%s)"
    val=(int(row['OrderDetailID']),int(row['OrderID']),int(row['ProductID']),int(row['Quantity']))
    try:
        cursor.execute(sql,val)
    except Exception as e:
        print(e)
    
mydb.commit()
mydb.close()
mydb.close()