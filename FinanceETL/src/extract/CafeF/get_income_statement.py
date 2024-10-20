from get_web_data_base import BaseCrawler
import pandas as pd
from generate_url import generate_income_statement_url, URL_BASE, CONST_TABLE

class IncomeStatementCrawler(BaseCrawler):
    def __init__(self, url_base = URL_BASE, company_name = 'vib', year = '2024', type_report="4 QUARTER", quarter="1"):
        super().__init__(url_base, company_name, year, type_report, quarter)
        self.url_income_statement = generate_income_statement_url(company_name, year, type_report, quarter)
    
    def get_income_statement(self):
        return self.extract_table_content(url = self.url_income_statement, index_table=4)

    def get_multi_table_quarter(self, company_name=None, year=None, number_quarter=4, quarter=1):
        if company_name is None:
            company_name = self.company_name
        if year is None:
            year = self.year
    
        column_names = self.generate_column_quarter_names(number_quarter=number_quarter, year=year, quarter=quarter, name_of_tables=f"{company_name}_Income_Statement_Quarter") 
        round_merge = number_quarter // 4
        final_table = None
    
        for i in range(round_merge):
            current_year = year - i
            __table = self.extract_table_content(generate_income_statement_url(company_name, str(current_year), "4 QUARTER", str(quarter)), 4)
            __table = __table.iloc[:, :-1]
            if final_table is None:
                final_table = __table
            else:
                final_table = pd.concat([__table,final_table.iloc[:,1:]], axis=1)
        final_table.columns = column_names  
        return final_table

    def get_multi_table_year(self, company_name=None, year=None, number_year=CONST_TABLE):
        if company_name is None:
            company_name = self.company_name
        if year is None:
            year = self.year
        round_merge = int(number_year // CONST_TABLE)
        final_table = None
    
        for _ in range(round_merge):
            current_year = year - CONST_TABLE
            __table = self.extract_table_content(generate_income_statement_url(company_name, str(current_year), "1 YEAR", str(0)), 4)
            __table = __table.iloc[:, :-1]
            if final_table is None:
                final_table = __table
            else:
                final_table = pd.concat([__table,final_table.iloc[:,1:]], axis=1)
        column_names = self.generate_column_year_names(number_year=number_year, year=year, name_of_tables=f"{company_name}_Income_Statement_Annual") 
        final_table.columns = column_names  
        return final_table

    