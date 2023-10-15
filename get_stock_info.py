from datetime import datetime, timedelta
from bqcrawler.task.utils.exporter import BigqueryExporterBase
import requests
from google.cloud import bigquery
import json

project_id = 'bishare-1606'
dataset_id = 'wiwi_test'
table_id = 'taiwan_stock_list'
bq_client = bigquery.Client(project=project_id)
today = datetime.now().strftime('%Y%m%d')

current_date = datetime.now()

yesterday = current_date - timedelta(days=1)
chinese_yesterday = f"{(yesterday.year - 1911)}/{yesterday.strftime('%m/%d')}"

query_job = bq_client.query(f"""SELECT DISTINCT stock_no FROM `{project_id}.{dataset_id}.{table_id}`""")
your_stock_numbers = [row['stock_no'] for row in query_job]
stock_info = []

for stock_no in your_stock_numbers:
    response = requests.get(f'https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={today}&stockNo={stock_no}')
    response_data = json.loads(response.text)['data']
    result = [data for index, data in enumerate(response_data) if chinese_yesterday in response_data[index]]
    if result:
        result[0].insert(0, stock_no)
        stock_info.append(result[0])

data_dict = {}

for item in stock_info:
    stock_no = item[0]
    data_dict[stock_no] = {
        "traded_date": item[1],
        "traded_shares": item[2],
        "traded_price": item[3],
        "opening_price": item[4],
        "high_price": item[5],
        "low_price": item[6],
        "closing_price": item[7],
        "gross_spread": item[8],
        "trading_volume": item[9]
    }

load_table_id = 'taiwan_stock'
exporter = BigqueryExporterBase(projectID=project_id)
exporter.streaming_insert(
    data=stock_info,
    datasetID=dataset_id,
    tableID=load_table_id
    )