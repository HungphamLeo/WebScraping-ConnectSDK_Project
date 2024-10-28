from generate_url import generate_summary_url
import pandas as pd
import requests
from bs4 import BeautifulSoup

class CompanyInfoBase:
    def __init__(self):
        pass
    def get_base_info(self,url):
        response = requests.get(url)
        response.encoding = 'utf-8' 
        soup = BeautifulSoup(response.text, "html.parser")
        for td in soup.find_all("td", colspan="100%"):
            td["colspan"] = "1"
        tbale = pd.read_html(str(soup))
        return tbale
    
    def get_company_info(self,company_name):
        url = generate_summary_url(company_name)
        table = self.get_base_info(url)
        company_info = table[0]
        company_table_info = pd.DataFrame(company_info)
        return company_table_info
    
    def get_all_company_info(self, list_company_name):
        company_info_table = None
        for company_name in list_company_name:
            table = self.get_company_info(company_name)
            if company_info_table is None:
                company_info_table = table
            else:
                company_info_table = pd.concat([company_info_table, table.iloc[:,1]])
        company_info_table = company_info_table.reset_index(drop=True)
        return company_info_table

            

    
    

