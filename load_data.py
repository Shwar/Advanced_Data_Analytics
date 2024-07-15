import mysql.connector
import pandas as pd

# Read data from CSV file
csv_file_path = 'stocktweets.csv'  
data = pd.read_csv(csv_file_path)

# Convert the date column to the correct format
data['date'] = pd.to_datetime(data['date'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')

try:
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='don05@Simon',
        database='stock_analysis'
    )

    cursor = conn.cursor()

    # Insert data into the database
    print("Inserting data from CSV:")
    for index, row in data.iterrows():
        try:
            cursor.execute('''
                INSERT INTO stocktweet (id, date, ticker, tweet) VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE date=VALUES(date), ticker=VALUES(ticker), tweet=VALUES(tweet)
            ''', (row['id'], row['date'], row['ticker'], row['tweet']))
            print(f"Inserted/Updated row {index + 1}: {row['id'], row['date'], row['ticker'], row['tweet']}")
        except Exception as e:
            print(f"Error inserting row {index} in stocktweet: {e}")

    conn.commit()

except mysql.connector.Error as err:
    print(f"Error: {err}")

finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()
