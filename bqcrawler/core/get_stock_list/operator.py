from datetime import datetime, timedelta
from google.cloud import bigquery

class TaiwanStockList():
    def __init__(self, projectID: str,dataset_id: str,table_id: str):
        self.projectID = projectID
        self.dataset_id = dataset_id
        self.table_id = table_id
    def get_stock_list(self):
        bq_client = bigquery.Client(project=self.projectID)
        query_job = bq_client.query(f"""SELECT DISTINCT stock_no FROM `{self.projectID}.{self.dataset_id}.{self.table_id}`""")
        stock_no = [row['stock_no'] for row in query_job]
        return stock_no