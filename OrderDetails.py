import mysql.connector
import pyodbc
import pymssql
from datetime import datetime


# Membuat objek koneksi ke MySQL
conn_mysql = mysql.connector.connect(
    host='localhost',
    user='root',
    password='',
    database='dwbit'
)

# Membuat objek koneksi ke SQL Server Azure
server = 'serverdemo-test.database.windows.net'
database = 'DWdemo'
username = 'sqladmin'
password = 'Khairul 190'
driver = '{SQL Server}'
conn_azure = f'SERVER={server};DATABASE={database};UID={username};PWD={password};DRIVER={driver}'
conn_azure_ = pyodbc.connect(conn_azure)


cursor = conn_mysql.cursor()
query = "SELECT last_synch from synch where table_name = 'orderdetails'"
cursor.execute(query)
lastSend = cursor.fetchone()[0]


query = "SELECT * FROM orderdetails WHERE OrderDate > %s"
cursor.execute(query, (lastSend,))
result = cursor.fetchall()

cursor_azure = conn_azure_.cursor()
if len(result) > 0:
    print("Transfer data: {} record\n".format(len(result)))

    for row in result:
        OrderID = row[0]
        OrderDate = row[1]
        PropertyID = row[2]
        ProductID = row[3]
        Quantity = row[4]

        source = "MYSQL01"
        synch_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        tsql = "INSERT INTO [dbo].[OrderDetails] (OrderID, OrderDate, PropertyID, ProductID, Quantity, source, synch_date) VALUES (?, ?, ?, ?, ?, ?, ?)"
        data = (OrderID, OrderDate, PropertyID,
                ProductID, Quantity, source, synch_date)

        cursor_azure.execute(tsql, data)
        conn_azure_.commit()

        # print(tsql + "\n")
else:
    print("0 result, no data need to sync\n")
