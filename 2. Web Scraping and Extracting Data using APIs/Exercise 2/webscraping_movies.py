import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path

url = 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'

parent_dir = Path(__file__).parent

db_name = f'{parent_dir}/films.db'
table_name = 'films'
csv_path = f'{parent_dir}/data/films.csv'
df = pd.DataFrame(columns=['Film', 'Year', "Rotten Tomatoes' Top 100"])
count = 0
count_limit = 25

html_page = requests.get(url).text
data = BeautifulSoup(html_page, 'html.parser')

tables = data.find_all('tbody')
rows = tables[0].find_all('tr')

for row in rows:
    if count < count_limit:
        col = row.find_all('td')
        
        if len(col) != 0:
            film = col[1].contents[0]
            year = col[2].contents[0]
            rt_rank = col[3].contents[0]
            
            if (not year.isnumeric()) or (not rt_rank.isnumeric()): continue
            
            year = int(year)
            rt_rank = int(rt_rank)
            
            if year >= 2000:
                data_dict = {'Film': film,
                            'Year': year,
                            "Rotten Tomatoes' Top 100": rt_rank}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)
                count += 1
        df = df.sort_values(by="Rotten Tomatoes' Top 100", ascending=True)
    else:
        break

df.to_csv(csv_path)

conn = sqlite3.connect(db_name)
df.to_sql(table_name, conn, if_exists='replace', index=False)
conn.close()