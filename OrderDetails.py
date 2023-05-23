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


# -- - - - - TAGET
# Membuat objek koneksi ke SQL Server Azure
server = 'serverdemo-test.database.windows.net'
database = 'DWdemo'
username = 'sqladmin'
password = 'Khairul 190'
# Driver ODBC yang sesuai untuk SQL Server versi terkini
driver = '{SQL Server}'

# Membuat string koneksi
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


# # Mendapatkan tanggal dan waktu hari ini
current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Membuat pernyataan SQL update
sql = "UPDATE synch SET last_synch = %s WHERE table_name = %s"

# Menjalankan pernyataan SQL dengan parameter tanggal dan waktu hari ini
params = (current_date, 'orderdetails')
cursor.execute(sql, params)

# Melakukan commit perubahan
conn_mysql.commit()

if cursor.execute(sql):
    print("Record updated successfully\n")
else:
    print("Error updating record\n")
