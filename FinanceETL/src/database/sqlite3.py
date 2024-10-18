import sqlite3
import time
import os
from logger import setup_logger_global
from utils import DATABASE_NAME
# Logger setup
connection_logger_name = os.path.abspath(__file__)
logger_database = setup_logger_global(connection_logger_name, connection_logger_name + '.log')

def create_table():
    """
    Creates the necessary tables in the SQLite database for storing order and account details.
    """
    try:
        con = sqlite3.connect(DATABASE_NAME)
        cur = con.cursor()
        
        # Create orders table
        query_database_orders = """
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            create_at INTEGER,
            update_at INTEGER,
            delete_at INTEGER,
            user_id TEXT,
            strategy_name TEXT,
            symbol TEXT,
            order_id TEXT,
            client_order_id TEXT,
            order_side TEXT,
            order_type TEXT,
            order_level INTEGER,
            entry_number TEXT,
            note TEXT
        );
        """
        cur.execute(query_database_orders)
        
        # Create account details daily table
        query_balance_account_daily = """
        CREATE TABLE IF NOT EXISTS account_details_daily (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            create_at INTEGER,
            update_at INTEGER,
            delete_at INTEGER,
            user_id TEXT,
            strategy_name TEXT,
            symbol TEXT,
            base_quantity TEXT,
            quote_quantity TEXT,
            base_price TEXT,
            quote_price TEXT,
            note TEXT
        );
        """
        cur.execute(query_balance_account_daily)
        con.commit()
        cur.close()
        
    except Exception as e:
        logger_database.error(f"{e} -- {e.__traceback__.tb_lineno}")
        return False
    return True

def run_query(query):
    """
    Executes a SQL query using the provided query string.
    """
    try:
        con = sqlite3.connect(DATABASE_NAME)
        cur = con.cursor()
        cur.execute(query)
        con.commit()
        cur.close()
    except Exception as e:
        logger_database.error(f"{e} -- {e.__traceback__.tb_lineno}")

def create_thread_querys(query_lists):
    """
    Creates a thread that continuously executes SQL queries from a global list.
    """
    try:
        while True:
            if query_lists:
                query = query_lists.pop(0)
                run_query(query)
            else:
                time.sleep(1)
    except Exception as e:
        logger_database.error(f"{e} -- {e.__traceback__.tb_lineno}")

def insert_order(order_info):
    """
    Inserts an order into the 'orders' table in the SQLite database.
    """
    try:
        con = sqlite3.connect(DATABASE_NAME)
        cur = con.cursor()
        order = (
            int(time.time()), 
            int(time.time()), 
            0, 
            order_info.get("user_id", ""), 
            order_info.get("strategy_name", ""),
            order_info.get("symbol", ""),
            order_info.get("order_id", ""),
            order_info.get("client_order_id", ""),
            order_info.get("order_side", "buy"),
            order_info.get("order_type", "limit"),
            order_info.get("order_level", 1),
            order_info.get("entry_number", ""),
            order_info.get("note", "")
        )
        cur.execute("""INSERT INTO orders VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", order)
        con.commit()
        cur.close()
    except Exception as e:
        logger_database.error(f"{e} -- {e.__traceback__.tb_lineno}")

def get_all_orders():
    """
    Retrieves all orders from the 'orders' table in the SQLite database.
    """
    try:
        query = "SELECT * FROM orders"
        con = sqlite3.connect(DATABASE_NAME)
        cur = con.cursor()
        res = cur.execute(query)
        con.commit()
        result = res.fetchall()
        cur.close()
        return result
    except Exception as e:
        logger_database.error(f"{e} -- {e.__traceback__.tb_lineno}")
    return None

def delete_from_order_temp(order_id):
    """
    Deletes a row from the 'orders' table in the SQLite database based on the provided 'order_id'.
    """
    try:
        query = f"DELETE FROM orders WHERE order_id = '{order_id}'"
        con = sqlite3.connect(DATABASE_NAME)
        cur = con.cursor()
        cur.execute(query)
        con.commit()
        cur.close()
    except Exception as e:
        logger_database.error(f"{e} -- {e.__traceback__.tb_lineno}")

def get_orders(strategy_name, symbol=""):
    """
    Retrieves orders from the 'orders' table in the SQLite database based on the provided strategy name and symbol.
    """
    try:
        query = f"SELECT * FROM orders WHERE strategy_name = '{strategy_name}'"
        if symbol:
            query += f" AND symbol = '{symbol}'"
        
        con = sqlite3.connect(DATABASE_NAME)
        cur = con.cursor()
        res = cur.execute(query)
        con.commit()
        result = res.fetchall()
        cur.close()
        return result
    except Exception as e:
        logger_database.error(f"{e} -- {e.__traceback__.tb_lineno}")
    return None

def insert_account_detail_status_daily(user_id="", strategy_name="", symbol="", base_quantity="0", quote_quantity="0", base_price="0", quote_price="0", note=""):
    """
    Inserts an account detail status daily into the database.
    """
    try:
        con = sqlite3.connect(DATABASE_NAME)
        cur = con.cursor()
        account_detail = (
            int(time.time()), int(time.time()), 0, user_id, strategy_name, symbol, 
            base_quantity, quote_quantity, base_price, quote_price, note
        )
        cur.execute("""INSERT INTO account_details_daily VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", account_detail)
        con.commit()
        cur.close()
    except Exception as e:
        logger_database.error(f"{e} -- {e.__traceback__.tb_lineno}")

def get_account_detail_status_daily(user_id="", symbol=""):
    """
    Retrieves the account detail status daily from the database based on the provided user ID and symbol.
    """
    try:
        query = "SELECT * FROM account_details_daily WHERE user_id = ?"
        params = (user_id,)
        if symbol:
            query += " AND symbol = ?"
            params = (user_id, symbol)

        con = sqlite3.connect(DATABASE_NAME)
        cur = con.cursor()
        res = cur.execute(query, params)
        con.commit()
        result = res.fetchall()
        cur.close()
        return result
    except Exception as e:
        logger_database.error(f"{e} -- {e.__traceback__.tb_lineno}")
    return None
