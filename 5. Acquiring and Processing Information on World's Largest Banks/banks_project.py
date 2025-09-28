import requests
import sqlite3
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime as dt

parent_dir = Path(__file__).parent

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'

exchange_rate_csv = f'{parent_dir}/data/exchange_rate.csv'

table_attribs_extraction = ['Name', 'MC_USD_Billion']
table_attribs_final = ['Name', 'MC_USD_Billion', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']

output_csv_path = f'{parent_dir}/data/largest_banks_data.csv'
db_name = f'{parent_dir}/banks.db'
table_name = 'largest_banks'

log_file = f'{parent_dir}/code_log.txt'

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = dt.now()
    timestamp = now.strftime(timestamp_format)
    
    with open(log_file, 'a') as f:
        f.write(f'{timestamp}: {message}\n')
        
def extract(url, table_attribs):
    page = requests.get(url).text
    page_data = BeautifulSoup(page, 'html.parser')
    
    df = pd.DataFrame(columns=table_attribs)
    
    tables = page_data.find_all('tbody')
    rows = tables[0].find_all('tr')
    
    for row in rows:
        col = row.find_all('td')
        
        if len(col) == 0: continue
        
        bank_name = col[1].find('span').find_next_sibling().get_text(strip=True)
        mc_usd_billion = col[2].get_text(strip=True)
        
        data_dict = {'Name': bank_name, 'MC_USD_Billion': mc_usd_billion}
        df1 = pd.DataFrame(data_dict, index=[0])
        df = pd.concat([df, df1], ignore_index=True)
    
    return df

def transform(df, table_attribs, exchange_rate_csv):
    exchange_rate_df = pd.read_csv(exchange_rate_csv, index_col=False)
    df_final = pd.DataFrame(columns=table_attribs)
    
    gbp_rate = exchange_rate_df.loc[exchange_rate_df['Currency'] == 'GBP', 'Rate'].iloc[0]
    eur_rate = exchange_rate_df.loc[exchange_rate_df['Currency'] == 'EUR', 'Rate'].iloc[0]
    inr_rate = exchange_rate_df.loc[exchange_rate_df['Currency'] == 'INR', 'Rate'].iloc[0]
    
    mc_usd_rates = df['MC_USD_Billion'].to_list()
    mc_usd_rates = [float(rate) for rate in mc_usd_rates]
    
    mc_gbp_rates = [np.round(x*gbp_rate, 2) for x in mc_usd_rates]
    mc_eur_rates = [np.round(x*eur_rate, 2) for x in mc_usd_rates]
    mc_inr_rates = [np.round(x*inr_rate, 2) for x in mc_usd_rates]
    
    df_final['Name'] = df['Name']
    df_final['MC_USD_Billion'] = mc_usd_rates
    df_final['MC_GBP_Billion'] = mc_gbp_rates
    df_final['MC_EUR_Billion'] = mc_eur_rates
    df_final['MC_INR_Billion'] = mc_inr_rates
    
    return df_final

def load_to_csv(df, csv_path):
    df.to_csv(csv_path)
    
def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query, sql_connection):
    query_output = pd.read_sql(query, sql_connection)
    print(query_output)

log_progress("Preliminaries complete. Initiating ETL process.")

log_progress("Initiating data extraction.")
df = extract(url, table_attribs_extraction)
log_progress("Data extraction Complete. Initiating transformation process.")

df = transform(df, table_attribs_final, exchange_rate_csv)
log_progress("Data transformation complete. Initiating loading process.")

load_to_csv(df, output_csv_path)
log_progress("Data saved to csv file.")

conn = sqlite3.connect(db_name)
log_progress("SQL Connection initiated.")

load_to_db(df, conn, table_name)
log_progress("Data loaded to Database as table. Running the query.")

query = f'SELECT * FROM {table_name}'
run_query(query, conn)

query = f'SELECT AVG(MC_GBP_Billion) FROM {table_name}'
run_query(query, conn)

query = f'SELECT Name FROM {table_name} LIMIT 5'
run_query(query, conn)

log_progress("Process Complete.")

conn.close()
log_progress("Database connection closed.")