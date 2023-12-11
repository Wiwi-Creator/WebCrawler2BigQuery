from bqcrawler.core.bigquery.operator import BigQueryOperator
from bqcrawler.core.TWSE.operator import TWSEOperator
from bqcrawler.core.utils.exporter import BigqueryExporterBase
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


class WebCrawlerOperator():
    def __init__() -> None:
        pass

    def trans_stock_info_to_bigquery():
        projectID = 'bishare-1606'
        data_datetime = datetime.now()
        try:
            logging.info("Get stock_no from bigquery")
            list_task = BigQueryOperator(projectID=projectID,
                                         datasetID='wiwi_test', 
                                         tableID='taiwan_stock_list')
            stock_no = list_task.get_stock_list()
            logging.info("Get stock_info from TW-Stock")
            info_task = TWSEOperator(stock_no, data_datetime)
            if stock_no:
                stock_info = info_task.get_stock_info()
            else:
                raise Exception("There's No Stock_No From TW-Stock!")
            
        except Exception as e:
            raise Exception(e)
        
        finally:
            exporter = BigqueryExporterBase(projectID=projectID)
            if stock_info is not None:
                logging.info("Start Streaming Insert Into Bigquery!")
                exporter.streaming_insert(
                    data=stock_info,
                    datasetID='wiwi_test',
                    tableID='taiwan_stock'
                    )
            else:
                logging.warning("StockInfo not found in TW-Stock")