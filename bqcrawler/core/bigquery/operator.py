from google.cloud import bigquery


class BigQueryOperator(object):
    def __init__(self, projectID: str, datasetID: str, tableID: str):
        self.projectID = projectID
        self.datasetID = datasetID
        self.tableID = tableID
        
    def get_stock_list(self):
        bq_client = bigquery.Client(project=self.projectID)
        query_job = bq_client.query(f"""SELECT DISTINCT stock_no FROM `{self.projectID}.{self.datasetID}.{self.tableID}`""")
        stock_no = [row['stock_no'] for row in query_job]
        return stock_no