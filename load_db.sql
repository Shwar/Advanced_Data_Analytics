CREATE DATABASE stock_analysis;

USE stock_analysis;

CREATE TABLE stocktweet (
    id INT PRIMARY KEY,
    date DATE,
    ticker VARCHAR(10),
    tweet TEXT
);

CREATE TABLE stockprice (
    date DATE,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    adj_close FLOAT,
    volume INT,
    ticker VARCHAR(10),
    PRIMARY KEY (date, ticker)
);

import mysql.connector

conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='don05@Simon',
    database='stock_analysis'
)

cursor = conn.cursor()

# Load stocktweet data
for index, row in stocktweets.iterrows():
    cursor.execute('''
        INSERT INTO stocktweet (id, date, ticker, tweet) VALUES (%s, %s, %s, %s)
    ''', tuple(row))

# Load stockprice data
for data, df in stockprice_dfs.items():
    for index, row in df.iterrows():
        cursor.execute('''
            INSERT INTO stockprice (date, open, high, low, close, adj_close, volume, ticker) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ''', tuple(row) + (data,))

conn.commit()
cursor.close()
conn.close()

select * from stockprice;
select * from stocktweet;

select * from sbux;

select * from pypl;


