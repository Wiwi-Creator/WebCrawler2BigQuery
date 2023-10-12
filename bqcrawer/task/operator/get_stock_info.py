from datetime import datetime, timedelta
import requests
from google.cloud import bigquery
from utils.exporter import BigqueryExporterBase
import json
from pathlib import Path

project_id = 'bishare-1606'
dataset_id = 'wiwi_test'
table_id = 'taiwan_stock'
bq_client = bigquery.Client(project=project_id)
schema_path = Path(__file__).parent / "configs/schemas/stock.json"
today = datetime.now().strftime('%Y%m%d')
# 取得當前日期
current_date = datetime.now()
# 減去一天
yesterday = current_date - timedelta(days=1)
chinese_yesterday = f"{(yesterday.year - 1911)}/{yesterday.strftime('%m/%d')}"

query_job = bq_client.query(f"""
            SELECT stock_no FROM `{project_id}.{dataset_id}.{table_id}`
        """)
your_stock_numbers = [row['stock_no'] for row in query_job]
combined = []
for stock_no in your_stock_numbers:  # 用您的股票號碼替換
    response = requests.get(f'https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={today}&stockNo={stock_no}')
    response_data = json.loads(response.text)['data']
    result = [data for index, data in enumerate(response_data) if chinese_yesterday in response_data[index]]
    if result:
        result_dict = {
            'stock_no': stock_no,
            'traded_date': result[0][0],
            'traded_shares': result[0][1],
            'traded_price': result[0][2],
            'opening_price': result[0][3],
            'high_price': result[0][4],
            'low_price': result[0][5],
            'closing_price': result[0][6],
            'gross_spread': result[0][7],
            'trading_volume': result[0][8]
        }
        combined.append(result_dict)
        
exporter = BigqueryExporterBase(projectID=project_id)
exporter.update_table_using_delete_insert(
    schema_path=schema_path,
    data=transaction_detail,
    datasetID=dataset_id,
    tableID=table_id,
    mapping_fields=["traded_date"]
    )