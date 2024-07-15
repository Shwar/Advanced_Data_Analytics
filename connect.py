import mysql.connector

try:
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='don05@Simon',
        database='stock_analysis'
    )
    
    if conn.is_connected():
        print("Connected to MySQL database")
    
    conn.close()

except mysql.connector.Error as err:
    print(f"Error: {err}")
