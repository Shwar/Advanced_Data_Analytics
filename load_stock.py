import mysql.connector
import pandas as pd
import os
import glob

ticker_value ='Null'

#  create table and insert CSV data into the database
def create_table_and_insert_data(csv_file_path, conn):
    try:
        data = pd.read_csv(csv_file_path)
        
        # check date column exists and is in the correct format
        if 'Date' in data.columns:
            data['Date'] = pd.to_datetime(data['Date'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')
        else:
            raise KeyError("'date' column not found in the CSV file.")
        
        table_name = os.path.splitext(os.path.basename(csv_file_path))[0].replace('-', '_').replace(' ', '_')
        
        cursor = conn.cursor()

        # Create table SQL command
        create_table_query = f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            date DATE,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            adj_close FLOAT,
            volume BIGINT,
            ticker VARCHAR(10),
            PRIMARY KEY (date, ticker)
        )
        '''
        
        cursor.execute(create_table_query)
        conn.commit()
        
        # Insert data into the  table
        for index, row in data.iterrows():
            try:
                insert_query = f'''
                    INSERT INTO {table_name} (date, open, high, low, close, adj_close, volume, ticker) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE 
                        date=VALUES(date),
                        open=VALUES(open), high=VALUES(high), low=VALUES(low), close=VALUES(close), 
                        adj_close=VALUES(adj_close), volume=VALUES(volume), ticker=VALUES(ticker)
                '''
                cursor.execute(insert_query, (row['Date'], row['Open'], row['High'], row['Low'], row['Close'], row['Adj Close'], row['Volume'], ticker_value))
                print(f"Inserted/Updated row {index + 1} in table {table_name}: {row['date'], row['open'], row['high'], row['low'], row['close'], row['adj_close'], row['volume'], ticker_value}")
            except Exception as e:
                print(f"Error inserting row {index} in table {table_name}: {e}")
        
        conn.commit()
        cursor.close()
    
    except KeyError as ke:
        print(f"KeyError: {ke}. Ensure the CSV file '{csv_file_path}' contains a 'date' column.")

    except Exception as e:
        print(f"Error processing file {csv_file_path}: {e}")

# Establish the database connection
try:
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='don05@Simon',  
        database='stock_analysis'
    )

    folder_path = 'stockprice/'  
    csv_files = glob.glob(os.path.join(folder_path, '*.csv'))
    
    for csv_file in csv_files:
        print(f"Processing file {csv_file}")
        create_table_and_insert_data(csv_file, conn)

except mysql.connector.Error as err:
    print(f"MySQL Error: {err}")

except Exception as e:
    print(f"Error: {e}")

finally:
    try:
        if conn:
            conn.close()
    except NameError:
        print("Connection was not established.")
