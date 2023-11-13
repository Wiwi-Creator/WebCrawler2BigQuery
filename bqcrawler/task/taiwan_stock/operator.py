import requests
import json


class TaiwanStockInfo(object):
    def __init__(self, stock_no: str, data_date):
        self.stock_no = stock_no
        self.data_date = data_date

    def get_stock_info(self):
        url_date = self.data_date.strftime('%Y%m%d')
        ch_data_date = f"{(self.data_date.year - 1911)}/{self.data_date.strftime('%m/%d')}"
        stock_info = []
        for web_stock_no in self.stock_no:
            response = requests.get(f'https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={url_date}&stockNo={web_stock_no}')
            response_data = json.loads(response.text)['data']
            result = [data for index, data in enumerate(response_data) if ch_data_date in response_data[index]]
            if result:
                result_dict = {
                    "stock_no": web_stock_no,
                    "traded_date": result[0][0],
                    "traded_shares": result[0][1],
                    "traded_price": result[0][2],
                    "opening_price": result[0][3],
                    "high_price": result[0][4],
                    "low_price": result[0][5],
                    "closing_price": result[0][6],
                    "gross_spread": result[0][7],
                    "trading_volume": result[0][8]
                    }
                stock_info.append(result_dict)
        return stock_info