from datetime import datetime
import requests
import sqlite3


today = datetime.now().strftime('%Y%m%d')
chinese_today = f"{(datetime.now().year - 1911)}/{datetime.now().strftime('%m/%d')}"
 
conn = sqlite3.connect('Stocks.db')
cursor = conn.cursor()
cursor.execute('SELECT StockNo FROM StockNumbers')
 
combined = []


for stock_no in cursor.fetchall():
    response = requests.get(f'https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={today}&stockNo={stock_no[0]}')
    response_data = response.json()['data']
 
    result = [data for index, data in enumerate(response_data) if chinese_today in response_data[index]]
 
    if result:
        result[0].insert(0, stock_no[0])
        combined.append(result[0])
 
print(combined)