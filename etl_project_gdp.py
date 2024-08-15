# Importing the required libraries

import requests
import sqlite3
import numpy as np
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd


# Code for ETL operations on Country-GDP data

def extract(url, table_attribs):
    html = requests.get(url).text
    data = BeautifulSoup(html, 'html.parser')
    df = pd.DataFrame(columns = table_attribs)
    tables = data.find_all('tbody')
    rows = tables[2].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            if col[0].find('a') is not None and '—' not in col[2]:
                data_dict = {"Country": col[0].a.contents[0],
                            "GDP_US_millions": col[1].contents[0]}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)
    return df

def transform(df):
    GDP_list = df['GDP_USD_millions'].tolist()
    GDP_list = [float("".join(str(x).split(','))) for x in GDP_list]
    GDP_list = [np.round(x / 1000, 2) for x in GDP_list]
    df['GDP_USD_millions'] = GDP_list
    df = df.rename(columns = {'GDP_USD_millions':'GDP_USD_billions'})
    print(df)
    return df

def load_to_csv(df, csv_path):
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists = 'replace', index = False)

def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' 
    now = datetime.now() # Get current time from datetime
    timestamp = now.strftime(timestamp_format)
    with open("./etl_project_log.txt", "a") as f:
        f.write(timestamp + ':' + message + '\n')


url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = ['Country', 'GDP_USD_millions']
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP'
csv_path = 'Countries_by_GDP.csv'
query_statement = f"SELECT * FROM {table_name} WHERE GDP_USD_billions >= 100"



log_progress('Initiating ETL process.')

df = extract(url, table_attribs)

log_progress('Extraction complete now initiating transformation process.')

df = transform(df)

log_progress('Data transformation complete now initiating loading process.')

load_to_csv(df, csv_path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect('World_Economies.db')

log_progress('Connected to SQLite3!')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table now running query.')

run_query(query_statement, sql_connection)

log_progress('ETL Process complete.')

sql_connection.close()







