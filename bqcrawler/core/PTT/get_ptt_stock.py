from WebCrawler2BigQuery.bqcrawler.task.utils.exporter import BigqueryExporterBase
from WebCrawler2BigQuery.bqcrawler.core.PTT.PTTStockTitle import PTTStockTitle
import logging


logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


class PTTCrawlerOperator():
    title_list = PTTStockTitle.get_title()
    exporter = BigqueryExporterBase()
    exporter.streaming_insert(
        data=title_list,
        datasetID='wiwi_test',
        tableID='PTT_stock_title'
        )

