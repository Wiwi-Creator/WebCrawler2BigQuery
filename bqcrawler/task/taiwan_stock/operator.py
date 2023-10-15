from datetime import datetime, timedelta
from google.cloud import bigquery

class TaiwanStockList(OrderList, OrderDetail):
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
