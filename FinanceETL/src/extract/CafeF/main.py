from get_balance_sheet import BalanceSheetCrawler
# from get_income_statement import IncomeStatementCrawler
# from get_cash_flow import CashFlowCrawler
# from get_web_data_base import TxtFileSaver
# from .....logger import setup_logger_global
# import pandas as pd
# import json
# import os 


balance_sheet_object = BalanceSheetCrawler(quarter="2")
balance_sheet = balance_sheet_object.get_multi_table_quarter("vib", 2024, 12, 2)
print(balance_sheet)
balance_sheet_object.save_data(balance_sheet, "xlsx", "C:/Users/Admin/Downloads/Project/Github/master/data_craw/balance_sheet.xlsx")
balance_sheet.to_csv("balance_sheet.csv", index=False)

# income_statement_object = IncomeStatementCrawler(quarter="2")
# income_statement = income_statement_object.get_income_statement()
# income_statement.to_csv("income_statement.csv", index=False)

# cash_flow_object = CashFlowCrawler(quarter="2")
# cash_flow = cash_flow_object.get_cash_flow()
# cash_flow.to_csv("cash_flow.csv", index=False)



        
       