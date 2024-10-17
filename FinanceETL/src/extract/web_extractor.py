import requests
from bs4 import BeautifulSoup

class WebExtractor:
    def extract(self, url):
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        tables = soup.find_all('table')
        # Trả về danh sách các bảng dữ liệu
        return tables
