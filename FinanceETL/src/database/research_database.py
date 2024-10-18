import os
from pymongo import MongoClient
import mysql.connector
from utils import (MYSQL_HOST, 
                   MYSQL_USER, 
                   MYSQL_PASSWORD, 
                   MYSQL_DATABASE,
                   MONGO_DB_URL

)
from logger import setup_logger_global
connection_logger_name = os.path.abspath(__file__)
logger_database = setup_logger_global(connection_logger_name, connection_logger_name + '.log')


def get_connection():
    """
    Establishes a connection to a MySQL database and returns a tuple containing the connection object and a cursor object.

    Returns:
        tuple: A tuple containing the connection object and a cursor object.

    Raises:
        mysql.connector.Error: If there is an error connecting to the MySQL database.
    """
    connection = mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE
    )
    cursor = connection.cursor()
    return connection, cursor

def close_connection(connection, cursor):
    """
    Closes a MySQL connection and cursor.

    Args:
        connection (mysql.connector.connection.MySQLConnection): The MySQL connection object.
        cursor (mysql.connector.cursor.MySQLCursor): The MySQL cursor object.

    Returns:
        None
    """
    cursor.close()
    connection.close()

def create_income_stock_db():
    """
    Creates income stock db 
    """
    try:
        connection, cursor = get_connection()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS income_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            symbol VARCHAR(50),
            fiscalDateEnding DATE,
            reportedCurrency VARCHAR(10),
            grossProfit BIGINT,
            totalRevenue BIGINT,
            costOfRevenue BIGINT,
            costofGoodsAndServicesSold BIGINT,
            operatingIncome BIGINT,
            sellingGeneralAndAdministrative BIGINT,
            researchAndDevelopment BIGINT,
            operatingExpenses BIGINT,
            investmentIncomeNet BIGINT,
            netInterestIncome BIGINT,
            interestIncome BIGINT,
            interestExpense BIGINT,
            nonInterestIncome BIGINT,
            otherNonOperatingIncome BIGINT,
            depreciation BIGINT,
            depreciationAndAmortization BIGINT,
            incomeBeforeTax BIGINT,
            incomeTaxExpense BIGINT,
            interestAndDebtExpense BIGINT,
            netIncomeFromContinuingOperations BIGINT,
            comprehensiveIncomeNetOfTax BIGINT,
            ebit BIGINT,
            ebitda BIGINT,
            netIncome BIGINT
        );
        """   
        
        cursor.execute(create_table_query)
        connection.commit()
        cursor.close()
        connection.close()
    except Exception as e:
        print(f"Error: {e}")
        return False
    return True

def update_insert_income_stock_data(data, symbol):
    try:
        connection, cursor = get_connection()
        check_query = """
        SELECT COUNT(*) FROM income_data WHERE fiscalDateEnding = %s AND reportedCurrency = %s;
        """
        cursor.execute(check_query, (data['fiscalDateEnding'], data['reportedCurrency']))
        record_exists = cursor.fetchone()[0] > 0
        
        if record_exists:
            # Cập nhật bản ghi nếu đã tồn tại
            update_query = """
            UPDATE income_data SET
                symbol = %s,
                grossProfit = %s,
                totalRevenue = %s,
                costOfRevenue = %s,
                costofGoodsAndServicesSold = %s,
                operatingIncome = %s,
                sellingGeneralAndAdministrative = %s,
                researchAndDevelopment = %s,
                operatingExpenses = %s,
                investmentIncomeNet = %s,
                netInterestIncome = %s,
                interestIncome = %s,
                interestExpense = %s,
                nonInterestIncome = %s,
                otherNonOperatingIncome = %s,
                depreciation = %s,
                depreciationAndAmortization = %s,
                incomeBeforeTax = %s,
                incomeTaxExpense = %s,
                interestAndDebtExpense = %s,
                netIncomeFromContinuingOperations = %s,
                comprehensiveIncomeNetOfTax = %s,
                ebit = %s,
                ebitda = %s,
                netIncome = %s
            WHERE fiscalDateEnding = %s AND reportedCurrency = %s;
            """
            cursor.execute(update_query, (
                data['symbol'],
                data['grossProfit'], data['totalRevenue'], data['costOfRevenue'], data['costofGoodsAndServicesSold'],
                data['operatingIncome'], data['sellingGeneralAndAdministrative'], data['researchAndDevelopment'],
                data['operatingExpenses'], data['investmentIncomeNet'], data['netInterestIncome'], data['interestIncome'],
                data['interestExpense'], data['nonInterestIncome'], data['otherNonOperatingIncome'], data['depreciation'],
                data['depreciationAndAmortization'], data['incomeBeforeTax'], data['incomeTaxExpense'], data['interestAndDebtExpense'],
                data['netIncomeFromContinuingOperations'], data['comprehensiveIncomeNetOfTax'], data['ebit'], data['ebitda'],
                data['netIncome'], data['fiscalDateEnding'], data['reportedCurrency']
            ))
        else:
            # Chèn bản ghi mới nếu chưa tồn tại
            insert_query = """
            INSERT INTO income_data (
                fiscalDateEnding, reportedCurrency, symbol, grossProfit, totalRevenue, costOfRevenue,
                costofGoodsAndServicesSold, operatingIncome, sellingGeneralAndAdministrative,
                researchAndDevelopment, operatingExpenses, investmentIncomeNet, netInterestIncome,
                interestIncome, interestExpense, nonInterestIncome, otherNonOperatingIncome,
                depreciation, depreciationAndAmortization, incomeBeforeTax, incomeTaxExpense,
                interestAndDebtExpense, netIncomeFromContinuingOperations, comprehensiveIncomeNetOfTax,
                ebit, ebitda, netIncome
            ) VALUES (
                %(fiscalDateEnding)s, %(reportedCurrency)s,%(symbol)s, %(grossProfit)s, %(totalRevenue)s, %(costOfRevenue)s,
                %(costofGoodsAndServicesSold)s, %(operatingIncome)s, %(sellingGeneralAndAdministrative)s,
                %(researchAndDevelopment)s, %(operatingExpenses)s, %(investmentIncomeNet)s, %(netInterestIncome)s,
                %(interestIncome)s, %(interestExpense)s, %(nonInterestIncome)s, %(otherNonOperatingIncome)s,
                %(depreciation)s, %(depreciationAndAmortization)s, %(incomeBeforeTax)s, %(incomeTaxExpense)s,
                %(interestAndDebtExpense)s, %(netIncomeFromContinuingOperations)s, %(comprehensiveIncomeNetOfTax)s,
                %(ebit)s, %(ebitda)s, %(netIncome)s
            );
            """
            cursor.executemany(insert_query, data)
            connection.commit()
    except Exception as e:
        print(f"Error occurred: {e}")
        connection.rollback()

def create_balance_sheet_db():
    """
    Creates balance sheet db 
    """
    try:
        connection, cursor = get_connection()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS balance_sheet_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            symbol VARCHAR(50),
            company_id VARCHAR(50),
            fiscalDateEnding DATE,
            reportedCurrency VARCHAR(10),
            totalAssets BIGINT,
            totalCurrentAssets BIGINT,
            cashAndCashEquivalentsAtCarryingValue BIGINT,
            cashAndShortTermInvestments BIGINT,
            inventory BIGINT,
            currentNetReceivables BIGINT,
            totalNonCurrentAssets BIGINT,
            propertyPlantEquipment BIGINT,
            accumulatedDepreciationAmortizationPPE BIGINT,
            intangibleAssets BIGINT,
            intangibleAssetsExcludingGoodwill BIGINT,
            goodwill BIGINT,
            investments BIGINT,
            longTermInvestments BIGINT,
            shortTermInvestments BIGINT,
            otherCurrentAssets BIGINT,
            otherNonCurrentAssets BIGINT,
            totalLiabilities BIGINT,
            totalCurrentLiabilities BIGINT,
            currentAccountsPayable BIGINT,
            deferredRevenue BIGINT,
            currentDebt BIGINT,
            shortTermDebt BIGINT,
            totalNonCurrentLiabilities BIGINT,
            capitalLeaseObligations BIGINT,
            longTermDebt BIGINT,
            currentLongTermDebt BIGINT,
            longTermDebtNoncurrent BIGINT,
            shortLongTermDebtTotal BIGINT,
            otherCurrentLiabilities BIGINT,
            otherNonCurrentLiabilities BIGINT,
            totalShareholderEquity BIGINT,
            treasuryStock BIGINT,
            retainedEarnings BIGINT,
            commonStock BIGINT,
            commonStockSharesOutstanding BIGINT
        );
        """
        
        cursor.execute(create_table_query)
        connection.commit()
        cursor.close()
        connection.close()
    except Exception as e:
        print(f"Error: {e}")
        return False
    return True

def update_insert_balance_sheet_data(data):
    try:
        connection, cursor = get_connection()
        check_query = """
        SELECT COUNT(*) FROM balance_sheet_data WHERE company_id = %s AND fiscalDateEnding = %s;
        """
        cursor.execute(check_query, (data['company_id'], data['fiscalDateEnding']))
        record_exists = cursor.fetchone()[0] > 0
        
        if record_exists:
            # Cập nhật bản ghi nếu đã tồn tại
            update_query = """
            UPDATE balance_sheet_data SET
                symbol = %s,
                totalAssets = %s,
                totalCurrentAssets = %s,
                cashAndCashEquivalentsAtCarryingValue = %s,
                cashAndShortTermInvestments = %s,
                inventory = %s,
                currentNetReceivables = %s,
                totalNonCurrentAssets = %s,
                propertyPlantEquipment = %s,
                accumulatedDepreciationAmortizationPPE = %s,
                intangibleAssets = %s,
                intangibleAssetsExcludingGoodwill = %s,
                goodwill = %s,
                investments = %s,
                longTermInvestments = %s,
                shortTermInvestments = %s,
                otherCurrentAssets = %s,
                otherNonCurrentAssets = %s,
                totalLiabilities = %s,
                totalCurrentLiabilities = %s,
                currentAccountsPayable = %s,
                deferredRevenue = %s,
                currentDebt = %s,
                shortTermDebt = %s,
                totalNonCurrentLiabilities = %s,
                capitalLeaseObligations = %s,
                longTermDebt = %s,
                currentLongTermDebt = %s,
                longTermDebtNoncurrent = %s,
                shortLongTermDebtTotal = %s,
                otherCurrentLiabilities = %s,
                otherNonCurrentLiabilities = %s,
                totalShareholderEquity = %s,
                treasuryStock = %s,
                retainedEarnings = %s,
                commonStock = %s,
                commonStockSharesOutstanding = %s
            WHERE company_id = %s AND fiscalDateEnding = %s;
            """
            cursor.execute(update_query, (
                data['symbol'],
                data['totalAssets'], data['totalCurrentAssets'], data['cashAndCashEquivalentsAtCarryingValue'],
                data['cashAndShortTermInvestments'], data['inventory'], data['currentNetReceivables'], data['totalNonCurrentAssets'],
                data['propertyPlantEquipment'], data['accumulatedDepreciationAmortizationPPE'], data['intangibleAssets'],
                data['intangibleAssetsExcludingGoodwill'], data['goodwill'], data['investments'], data['longTermInvestments'],
                data['shortTermInvestments'], data['otherCurrentAssets'], data['otherNonCurrentAssets'], data['totalLiabilities'],
                data['totalCurrentLiabilities'], data['currentAccountsPayable'], data['deferredRevenue'], data['currentDebt'],
                data['shortTermDebt'], data['totalNonCurrentLiabilities'], data['capitalLeaseObligations'], data['longTermDebt'],
                data['currentLongTermDebt'], data['longTermDebtNoncurrent'], data['shortLongTermDebtTotal'], data['otherCurrentLiabilities'],
                data['otherNonCurrentLiabilities'], data['totalShareholderEquity'], data['treasuryStock'], data['retainedEarnings'],
                data['commonStock'], data['commonStockSharesOutstanding'], data['company_id'], data['fiscalDateEnding']
            ))
        else:
            # Chèn bản ghi mới nếu chưa tồn tại
            insert_query = """
            INSERT INTO balance_sheet_data (
                company_id, fiscalDateEnding, reportedCurrency,symbol, totalAssets, totalCurrentAssets,
                cashAndCashEquivalentsAtCarryingValue, cashAndShortTermInvestments, inventory,
                currentNetReceivables, totalNonCurrentAssets, propertyPlantEquipment,
                accumulatedDepreciationAmortizationPPE, intangibleAssets, intangibleAssetsExcludingGoodwill,
                goodwill, investments, longTermInvestments, shortTermInvestments, otherCurrentAssets,
                otherNonCurrentAssets, totalLiabilities, totalCurrentLiabilities, currentAccountsPayable,
                deferredRevenue, currentDebt, shortTermDebt, totalNonCurrentLiabilities, capitalLeaseObligations,
                longTermDebt, currentLongTermDebt, longTermDebtNoncurrent, shortLongTermDebtTotal,
                otherCurrentLiabilities, otherNonCurrentLiabilities, totalShareholderEquity, treasuryStock,
                retainedEarnings, commonStock, commonStockSharesOutstanding
            ) VALUES (
                %(company_id)s, %(fiscalDateEnding)s, %(reportedCurrency)s, %(symbol)s,%(totalAssets)s, %(totalCurrentAssets)s,
                %(cashAndCashEquivalentsAtCarryingValue)s, %(cashAndShortTermInvestments)s, %(inventory)s,
                %(currentNetReceivables)s, %(totalNonCurrentAssets)s, %(propertyPlantEquipment)s,
                %(accumulatedDepreciationAmortizationPPE)s, %(intangibleAssets)s, %(intangibleAssetsExcludingGoodwill)s,
                %(goodwill)s, %(investments)s, %(longTermInvestments)s, %(shortTermInvestments)s, %(otherCurrentAssets)s,
                %(otherNonCurrentAssets)s, %(totalLiabilities)s, %(totalCurrentLiabilities)s, %(currentAccountsPayable)s,
                %(deferredRevenue)s, %(currentDebt)s, %(shortTermDebt)s, %(totalNonCurrentLiabilities)s, %(capitalLeaseObligations)s,
                %(longTermDebt)s, %(currentLongTermDebt)s, %(longTermDebtNoncurrent)s, %(shortLongTermDebtTotal)s,
                %(otherCurrentLiabilities)s, %(otherNonCurrentLiabilities)s, %(totalShareholderEquity)s, %(treasuryStock)s,
                %(retainedEarnings)s, %(commonStock)s, %(commonStockSharesOutstanding)s
            );
            """
            cursor.execute(insert_query, data)
        
        connection.commit()
        cursor.close()
        connection.close()
    except Exception as e:
        print(f"Error occurred: {e}")
        connection.rollback()

def create_cash_flow_db():
    """
    Creates cash flow table in the database
    """
    try:
        connection, cursor = get_connection()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS cash_flow_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            symbol VARCHAR(50),
            company_id INT,
            fiscalDateEnding DATE,
            reportedCurrency VARCHAR(10),
            operatingCashflow BIGINT,
            paymentsForOperatingActivities BIGINT,
            proceedsFromOperatingActivities BIGINT,
            changeInOperatingLiabilities BIGINT,
            changeInOperatingAssets BIGINT,
            depreciationDepletionAndAmortization BIGINT,
            capitalExpenditures BIGINT,
            changeInReceivables BIGINT,
            changeInInventory BIGINT,
            profitLoss BIGINT,
            cashflowFromInvestment BIGINT,
            cashflowFromFinancing BIGINT,
            proceedsFromRepaymentsOfShortTermDebt BIGINT,
            paymentsForRepurchaseOfCommonStock BIGINT,
            paymentsForRepurchaseOfEquity BIGINT,
            paymentsForRepurchaseOfPreferredStock BIGINT,
            dividendPayout BIGINT,
            dividendPayoutCommonStock BIGINT,
            dividendPayoutPreferredStock BIGINT,
            proceedsFromIssuanceOfCommonStock BIGINT,
            proceedsFromIssuanceOfLongTermDebtAndCapitalSecuritiesNet BIGINT,
            proceedsFromIssuanceOfPreferredStock BIGINT,
            proceedsFromRepurchaseOfEquity BIGINT,
            proceedsFromSaleOfTreasuryStock BIGINT,
            changeInCashAndCashEquivalents BIGINT,
            changeInExchangeRate BIGINT,
            netIncome BIGINT,
            FOREIGN KEY (company_id) REFERENCES company_data(id)
        );
        """
        
        cursor.execute(create_table_query)
        connection.commit()
        cursor.close()
        connection.close()
    except Exception as e:
        print(f"Error: {e}")
        return False
    return True

def update_insert_cash_flow_data(data):
    try:
        connection, cursor = get_connection()
        check_query = """
        SELECT COUNT(*) FROM cash_flow_data WHERE fiscalDateEnding = %s AND reportedCurrency = %s AND company_id = %s;
        """
        cursor.execute(check_query, (data['fiscalDateEnding'], data['reportedCurrency'], data['company_id']))
        record_exists = cursor.fetchone()[0] > 0
        
        if record_exists:
            # Cập nhật bản ghi nếu đã tồn tại
            update_query = """
            UPDATE cash_flow_data SET
                symbol = %s,
                operatingCashflow = %s,
                paymentsForOperatingActivities = %s,
                proceedsFromOperatingActivities = %s,
                changeInOperatingLiabilities = %s,
                changeInOperatingAssets = %s,
                depreciationDepletionAndAmortization = %s,
                capitalExpenditures = %s,
                changeInReceivables = %s,
                changeInInventory = %s,
                profitLoss = %s,
                cashflowFromInvestment = %s,
                cashflowFromFinancing = %s,
                proceedsFromRepaymentsOfShortTermDebt = %s,
                paymentsForRepurchaseOfCommonStock = %s,
                paymentsForRepurchaseOfEquity = %s,
                paymentsForRepurchaseOfPreferredStock = %s,
                dividendPayout = %s,
                dividendPayoutCommonStock = %s,
                dividendPayoutPreferredStock = %s,
                proceedsFromIssuanceOfCommonStock = %s,
                proceedsFromIssuanceOfLongTermDebtAndCapitalSecuritiesNet = %s,
                proceedsFromIssuanceOfPreferredStock = %s,
                proceedsFromRepurchaseOfEquity = %s,
                proceedsFromSaleOfTreasuryStock = %s,
                changeInCashAndCashEquivalents = %s,
                changeInExchangeRate = %s,
                netIncome = %s
            WHERE fiscalDateEnding = %s AND reportedCurrency = %s AND company_id = %s;
            """
            cursor.execute(update_query, (
                data['symbol'],
                data['operatingCashflow'], data['paymentsForOperatingActivities'], data['proceedsFromOperatingActivities'],
                data['changeInOperatingLiabilities'], data['changeInOperatingAssets'], data['depreciationDepletionAndAmortization'],
                data['capitalExpenditures'], data['changeInReceivables'], data['changeInInventory'], data['profitLoss'],
                data['cashflowFromInvestment'], data['cashflowFromFinancing'], data['proceedsFromRepaymentsOfShortTermDebt'],
                data['paymentsForRepurchaseOfCommonStock'], data['paymentsForRepurchaseOfEquity'], data['paymentsForRepurchaseOfPreferredStock'],
                data['dividendPayout'], data['dividendPayoutCommonStock'], data['dividendPayoutPreferredStock'],
                data['proceedsFromIssuanceOfCommonStock'], data['proceedsFromIssuanceOfLongTermDebtAndCapitalSecuritiesNet'],
                data['proceedsFromIssuanceOfPreferredStock'], data['proceedsFromRepurchaseOfEquity'], data['proceedsFromSaleOfTreasuryStock'],
                data['changeInCashAndCashEquivalents'], data['changeInExchangeRate'], data['netIncome'],
                data['fiscalDateEnding'], data['reportedCurrency'], data['company_id']
            ))
        else:
            # Chèn bản ghi mới nếu chưa tồn tại
            insert_query = """
            INSERT INTO cash_flow_data (
                company_id, fiscalDateEnding, reportedCurrency,symbol,operatingCashflow, paymentsForOperatingActivities,
                proceedsFromOperatingActivities, changeInOperatingLiabilities, changeInOperatingAssets,
                depreciationDepletionAndAmortization, capitalExpenditures, changeInReceivables, changeInInventory,
                profitLoss, cashflowFromInvestment, cashflowFromFinancing, proceedsFromRepaymentsOfShortTermDebt,
                paymentsForRepurchaseOfCommonStock, paymentsForRepurchaseOfEquity, paymentsForRepurchaseOfPreferredStock,
                dividendPayout, dividendPayoutCommonStock, dividendPayoutPreferredStock, proceedsFromIssuanceOfCommonStock,
                proceedsFromIssuanceOfLongTermDebtAndCapitalSecuritiesNet, proceedsFromIssuanceOfPreferredStock,
                proceedsFromRepurchaseOfEquity, proceedsFromSaleOfTreasuryStock, changeInCashAndCashEquivalents,
                changeInExchangeRate, netIncome
            ) VALUES (
                %(company_id)s, %(fiscalDateEnding)s, %(reportedCurrency)s, %(symbol)s,%(operatingCashflow)s, %(paymentsForOperatingActivities)s,
                %(proceedsFromOperatingActivities)s, %(changeInOperatingLiabilities)s, %(changeInOperatingAssets)s,
                %(depreciationDepletionAndAmortization)s, %(capitalExpenditures)s, %(changeInReceivables)s, %(changeInInventory)s,
                %(profitLoss)s, %(cashflowFromInvestment)s, %(cashflowFromFinancing)s, %(proceedsFromRepaymentsOfShortTermDebt)s,
                %(paymentsForRepurchaseOfCommonStock)s, %(paymentsForRepurchaseOfEquity)s, %(paymentsForRepurchaseOfPreferredStock)s,
                %(dividendPayout)s, %(dividendPayoutCommonStock)s, %(dividendPayoutPreferredStock)s,
                %(proceedsFromIssuanceOfCommonStock)s, %(proceedsFromIssuanceOfLongTermDebtAndCapitalSecuritiesNet)s,
                %(proceedsFromIssuanceOfPreferredStock)s, %(proceedsFromRepurchaseOfEquity)s, %(proceedsFromSaleOfTreasuryStock)s,
                %(changeInCashAndCashEquivalents)s, %(changeInExchangeRate)s, %(netIncome)s
            );
            """
            cursor.execute(insert_query, data)
            connection.commit()
    except Exception as e:
        print(f"Error occurred: {e}")
        connection.rollback()


def get_connection_mongo():
    client = MongoClient(MONGO_DB_URL)
    return client

def create_database_and_collection(db_name, collection_name):
    client = get_connection_mongo()
    db = client[db_name]
    collection = db[collection_name]
    return collection

def insert_data(collection, data):
    try:
        result = collection.insert_one(data)
        return result
    except Exception as e:
        logger_database.error(e)

def delete_data(collection, query):
    result = collection.delete_one(query)
    if result.deleted_count > 0:
        return True
    else:
        return False

def update_data(collection, query, new_values):
    result = collection.update_one(query, {"$set": new_values})
    if result.modified_count > 0:
        return True
    else:
        return False


