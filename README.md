# bqcrawler

## 程式執行事前準備

- 股票代號對照表(stock_list)相關資料
    * 在 `$dataset`.`stock_list` 建立股票對照表，紀錄各項股票代號及相關資訊。
- 個股每日 收盤價/成交數/...等相關資料
    * 在`$dataset`.`tw_stock` 建立個股table，將透過API request取得後的資料寫入該表，紀錄各項股票資訊細節。

## 套件使用範例

- import 套件
- 匯入資料日期(參數還在想)
```python
from bqcrawler import WebCrawlerOperator

api_task = WebCrawlerOperator()
api_task.trans_stock_info_to_bigquery()
```

## bqcrawler 開發注意事項

- 資料導入流程在task/內
- DB操作在core/內，**各DB在core內獨立，不可互相干涉**
- 各api操作在task/folder內
