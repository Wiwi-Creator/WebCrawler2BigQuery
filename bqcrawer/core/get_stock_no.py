import requests
import json

response = requests.get('https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date=20231012&stockNo=2330')
response_data = json.loads(response.text)['data']

combined = []

#if response_data:
#    response_data[0].insert(0, stock_no[0])
#    combined.append(response_data[0])

# 打印或處理多筆資料
for data1 in response_data:
    data2 = {
            "stock_no": "2330",
            "traded_date": data1[0],
            "traded_shares": data1[1],
            "traded_price": data1[2],
            "opening_price": data1[3],
            "high_price": data1[4],
            "low_price": data1[5],
            "closing_price": data1[6],
            "gross_spread": data1[7],
            "trading_volume": data1[8]
        }
    print(data2)