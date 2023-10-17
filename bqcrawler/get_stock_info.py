from core.get_stock_list.operator import TaiwanStockList
from task.taiwan_stock.operator import TaiwanStockInfo
from task.utils.exporter import BigqueryExporterBase
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
        projectID='bishare-1606'
        data_datetime = datetime.now()
        try:
            logging.info("Get stock_no from bigquery")
            list_task = TaiwanStockList(projectID=projectID,
                                        datasetID='wiwi_test', 
                                        tableID='taiwan_stock_list')
            stock_no = list_task.get_stock_list()
            logging.info("Get stock_info from TW-Stock")
            info_task = TaiwanStockInfo(stock_no, data_datetime)
            stock_info = info_task.get_stock_info()
        except Exception as e:
            raise Exception(e)
        finally:
            exporter = BigqueryExporterBase(projectID=projectID)
            if stock_info != None:
                logging.info("Start Streaming Insert Into Bigquery!")
                exporter.streaming_insert(
                    data=stock_info,
                    datasetID='wiwi_test',
                    tableID='taiwan_stock'
                    )
            else:
                logging.warning("StockInfo not found in TW-Stock")

trans_stock_info_to_bigquery()