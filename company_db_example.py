import os
from database.research_database import (create_database_and_collection, 
                                        create_income_stock_db, update_insert_income_stock_data,
                                        create_balance_sheet_db, update_insert_balance_sheet_data,
                                        create_cash_flow_db, update_insert_cash_flow_data)
from research_data_provider import *
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
api_key = os.getenv('API_KEY_ALPHAVANTAGE')

# Set up database and API parameters
db_name = 'stock_data'
symbol = 'IBM'
number_year_data = 10
quote_exchange = utils_research.quote_exchange.US

# Initialize FundamentalStockData client
fundamental_client = FundementalStockData(api_key=api_key, base_asset=symbol, quote_asset=quote_exchange, datatype='json')

# Retrieve fundamental data
overview_stock = fundamental_client.get_fundamental_stock_data() 
income_statement = fundamental_client.get_fundamental_income_statement()
balance_sheet = fundamental_client.get_fundamental_balance_sheet()
cash_flow = fundamental_client.get_fundamental_cash_flow()

# Create database collection
overview_company_mongo = create_database_and_collection(db_name=db_name, collection_name='Overview_company')

# Prepare data for insertion
income_statement_data = income_statement['annualReports']
income_statement_data['symbol'] = symbol
balance_sheet_data = balance_sheet['annualReports']
balance_sheet_data['symbol'] = symbol
cash_flow_data = cash_flow['annualReports']
cash_flow_data['symbol'] = symbol

# Create database tables
income_statement_db = create_income_stock_db()
balance_sheet_db = create_balance_sheet_db()
cash_flow_db = create_cash_flow_db()

# Update and insert data for multiple years
for i in range(number_year_data):
    insert_income_statement_db = update_insert_income_stock_data()
    insert_balance_sheet_db = update_insert_balance_sheet_data()
    insert_cash_flow_db = update_insert_cash_flow_data()