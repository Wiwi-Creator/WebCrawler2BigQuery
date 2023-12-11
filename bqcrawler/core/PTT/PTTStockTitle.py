import requests
from bs4 import BeautifulSoup


class PTTStockTitle(object):
    def get_title():
        URL = 'https://www.ptt.cc/bbs/Stock/index1.html' 
        # 使用 for 迴圈將逐筆將標籤(tags)裡的 List 印出, 這裡取3頁
        result = []
        #for round in range(3):
        # Send get request to PTT Stock
        RES = requests.get(URL) 
        # 將 HTML 網頁程式碼丟入 bs4 分析模組
        soup = BeautifulSoup(RES.text, 'html.parser') 
        # 查找標題文章的 html 元素。過濾出標籤名稱為'div'且 class 屬性為 title, 子標籤名稱為'a'
        articles = soup.select('div.title a') 
        # 呈上。取出'下一頁'元素
        paging = soup.select('div.btn-group-paging a') 
        # 將'下一頁'元素存到 next_URL 中
        next_URL = 'https://www.ptt.cc' + paging[2]['href'] 
        URL = next_URL 
        for x in articles: 
            title = x.text
            url = 'https://www.ptt.cc' + x['href']
            article_info = {"title": title, "url": url}
            result.append(article_info)
        return result