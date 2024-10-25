from get_balance_sheet import BalanceSheetCrawler
from get_income_statement import IncomeStatementCrawler
from get_cash_flow import CashFlowCrawler
# from get_web_data_base import TxtFileSaver
# from .....logger import setup_logger_global
import pandas as pd
import json
import os 
import time


company_name_list = ["acb","bab","bid","ctg","eib","hdb",
                     "mbb","msb","nvb","ocb","shb","ssb",
                     "stb","tcb","tpb","vcb","vib","vpb"]
year_annually = 2023
year_quarter = 2024
quarter = 2
number_quarter = 16
number_year = 4
balance_sheet_object = BalanceSheetCrawler(quarter="2")
income_statement_object = IncomeStatementCrawler(quarter="2")
cash_flow_object = CashFlowCrawler(quarter="2")
path_quarter = "C:/Users/Admin/Downloads/Project/Github/master/data_craw/quarter/"
path_annual = "C:/Users/Admin/Downloads/Project/Github/master/data_craw/annual/"


for folder in ["balance_sheet_quarter", "income_statement_quarter", "cash_flow_quarter"]:
    os.makedirs(os.path.join(path_quarter, folder), exist_ok=True)
for folder in ["balance_sheet_annual", "income_statement_annual", "cash_flow_annual"]:
    os.makedirs(os.path.join(path_annual, folder), exist_ok=True)

for company_name in company_name_list:
    balance_sheet = balance_sheet_object.get_multi_table_quarter(company_name, year_quarter, number_quarter, quarter)
    income_statement = income_statement_object.get_multi_table_quarter(company_name, year_quarter, number_quarter, quarter)
    cash_flow = cash_flow_object.get_multi_table_quarter(company_name, year_quarter, number_quarter, quarter)

    balance_sheet_object.save_data(balance_sheet, "xlsx", os.path.join(path_quarter, "balance_sheet_quarter", f'{company_name}.xlsx'))
    income_statement_object.save_data(income_statement, "xlsx", os.path.join(path_quarter, "income_statement_quarter", f'{company_name}.xlsx'))
    cash_flow_object.save_data(cash_flow, "xlsx", os.path.join(path_quarter, "cash_flow_quarter", f'{company_name}.xlsx'))
    time.sleep(3)

# for company_name in company_name_list:
    # balance_sheet = balance_sheet_object.get_multi_table_year(company_name, year_annually, number_year)
    # income_statement = income_statement_object.get_multi_table_year(company_name, year_annually, number_year)
    # cash_flow = cash_flow_object.get_multi_table_year(company_name, year_annually, number_year)

    # balance_sheet_object.save_data(balance_sheet, "xlsx", os.path.join(path_annual, "balance_sheet_annual", f'{company_name}.xlsx'))
    # income_statement_object.save_data(income_statement, "xlsx", os.path.join(path_annual, "income_statement_annual", f'{company_name}.xlsx'))
    # cash_flow_object.save_data(cash_flow, "xlsx", os.path.join(path_annual, "cash_flow_annual", f'{company_name}.xlsx'))
    # time.sleep(3)





        
       