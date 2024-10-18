import os
import time
import datetime
import redis
import mysql.connector
from utils import (MYSQL_HOST, 
                   MYSQL_USER, 
                   MYSQL_PASSWORD, 
                   MYSQL_DATABASE,

)
from logger import setup_logger_global
connection_logger_name = os.path.abspath(__file__)
logger_database = setup_logger_global(connection_logger_name, connection_logger_name + '.log')
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

def xtb_create_table_make_orders():
    """
    Creates the make_orders table in the MySQL database.
    """
    try:
        con = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
        if con.is_connected():
            cur = con.cursor()

            # Create make_orders table
            query_make_orders = """
            CREATE TABLE IF NOT EXISTS make_orders (
                id INT AUTO_INCREMENT PRIMARY KEY,
                order_id VARCHAR(255) NOT NULL,
                exchange VARCHAR(50),
                strategy_name VARCHAR(255),
                api_key VARCHAR(255),
                account_id VARCHAR(255),
                param_id VARCHAR(255),
                symbol VARCHAR(50),
                note TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            );
            """
            cur.execute(query_make_orders)
            con.commit()
            cur.close()
            con.close()
            
    except Exception as e:
        print(f"Error: {e}")
        return False
    return True

def xtb_create_event_calendar():
    """
    Creates the event_calendar table in the MySQL database. 
    """
    try:
        connection, cursor = get_connection()

        # Create event_calendar table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS news_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            ts VARCHAR(255),
            wait_news_time VARCHAR(255),
            official_news BOOLEAN,
            country VARCHAR(255),
            title VARCHAR(255),
            body TEXT
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



def xtb_create_table_inventory_values():
    """
    Creates the inventory_values table in the MySQL database.
    """
    try:
        con = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
        if con.is_connected():
            cur = con.cursor()

            # Create inventory_values table
            query_inventory_values = """
            CREATE TABLE IF NOT EXISTS inventory_values (
                id INT AUTO_INCREMENT PRIMARY KEY,
                exchange_name VARCHAR(255) NOT NULL,
                base_symbol VARCHAR(50) NOT NULL,
                quote_symbol VARCHAR(50) NOT NULL,
                quote DECIMAL(20, 10) NOT NULL,
                base DECIMAL(20, 10) NOT NULL,
                inventory DECIMAL(20, 10) NOT NULL,
                price DECIMAL(20, 10) NOT NULL,
                quote_price DECIMAL(20, 10) NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                time_stamp BIGINT NOT NULL
            );
            """
            cur.execute(query_inventory_values)
            
            con.commit()
            cur.close()
            con.close()
            
    except Exception as e:
        print(f"Error: {e}")
        return False
    return True

def xtb_delete_news_db():
    try:
        connection, cursor = get_connection()
        
        # Define the query to delete records where official_news is not False
        delete_query = """
        DELETE FROM news_data
        WHERE official_news != False
        """
        
        cursor.execute(delete_query)
        connection.commit()
        
        print("Records deleted successfully.")
        
    except Exception as e:
        print(f"Error occurred: {e}")
        connection.rollback()
        
    finally:
        cursor.close()
        connection.close()

def xtb_insert_news_db(data_list):
    try:
        connection, cursor = get_connection()
        check_query = """
        SELECT COUNT(*) AS count
        FROM news_data
        WHERE ts = %s AND title = %s AND country = %s
        """
        insert_query = """
        INSERT INTO news_data (
            ts, wait_news_time, official_news, country, title, body
        ) VALUES (
            %(ts)s, %(wait_news_time)s, %(official_news)s, %(country)s, %(title)s, %(body)s
        );
        """
        
        # Check for existing news and insert only if it doesn't exist
        for data in data_list:
            cursor.execute(check_query, (data['ts'], data['title'], data['country']))
            result = cursor.fetchone()
            if result['count'] == 0:
                cursor.execute(insert_query, data)
            else:
                print(f"News item with ts={data['ts']}, title={data['title']} already exists.")
        
        connection.commit()
        
    except Exception as e:
        print(f"Error occurred: {e}")
        connection.rollback()

def xtb_insert_symbol_db(data_list):
    try:
        connection, cursor = get_connection()
        insert_query = """
        INSERT INTO symbol_data (
            ts, symbol, category, askPr, bidPr, bestBid, bestAsk,
            contractSize, baseAsset, highest_24h_price, lowest_24h_price,
            spread, leverage, minQty, maxQty, tickSize, tickPrice,
            priceScale, qtyScale
        ) VALUES (
            %(ts)s, %(symbol)s, %(category)s, %(askPr)s, %(bidPr)s, %(bestBid)s, %(bestAsk)s,
            %(contractSize)s, %(baseAsset)s, %(highest_24h_price)s, %(lowest_24h_price)s,
            %(spread)s, %(leverage)s, %(minQty)s, %(maxQty)s, %(tickSize)s, %(tickPrice)s,
            %(priceScale)s, %(qtyScale)s
        );
        """
        cursor.executemany(insert_query, data_list)
        connection.commit()
    except Exception as e:
        print(f"Error occurred: {e}")
        connection.rollback()



def xtb_create_table_symbol_data():
    """
    Creates the symbol_data table in the MySQL database.
    """
    try:
        connection, cursor = get_connection()
        create_table_query = """
            CREATE TABLE IF NOT EXISTS symbol_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                ts VARCHAR(255),
                symbol VARCHAR(255),
                category VARCHAR(255),
                askPr FLOAT,
                bidPr FLOAT,
                bestBid FLOAT,
                bestAsk FLOAT,
                contractSize INT,
                baseAsset VARCHAR(255),
                highest_24h_price FLOAT,
                lowest_24h_price FLOAT,
                spread FLOAT,
                leverage FLOAT,
                minQty FLOAT,
                maxQty FLOAT,
                tickSize FLOAT,
                tickPrice FLOAT,
                priceScale INT,
                qtyScale INT
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


def xtb_get_connection():
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

def xtb_close_connection(connection, cursor):
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

    

def xtb_insert_make_order(order):
    """
    Inserts an order into the 'make_orders' table in the MySQL database.

    Args:
        order (dict): A dictionary containing the details of the order to be inserted. It should have the following keys:
            - 'order_id' (str): The ID of the order.
            - 'exchange' (str): The exchange where the order is placed.
            - 'strategy_name' (str): The name of the strategy used to place the order.
            - 'api_key' (str): The API key used to authenticate the order.
            - 'account_id' (str): The ID of the account associated with the order.
            - 'param_id' (str): The ID of the parameters used to place the order.
            - 'symbol' (str): The symbol of the asset being traded.
            - 'note' (str): Any additional notes or comments about the order.

    Returns:
        None or Exception: If an error occurs while inserting the order, an Exception is returned. Otherwise, None is returned.

    Raises:
        Exception: If an error occurs while connecting to the database or executing the SQL query.

    Note:
        The 'created_at' and 'updated_at' columns are set to the current timestamp.
    """
    try:
        connection, cursor = xtb_get_connection()
        current_timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        insert_query = """
            INSERT INTO make_orders (order_id, exchange, strategy_name, api_key, account_id, param_id, symbol, note, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
        
        cursor.execute(insert_query, (order['order_id'], order['exchange'], order['strategy_name'], order['api_key'], order['account_id'], 
                                      order['param_id'], order['symbol'], order['note'], current_timestamp, current_timestamp))
        
        connection.commit()
        return True
    except Exception as err:
        logger_database.error(f"Error inserting row:{err}")
        return err
        
    finally:
        xtb_close_connection(connection, cursor)

def xtb_fetch_all_make_order():
    """
    Fetches all the make orders from the database.

    Returns:
        A list of lists, where each inner list contains the following elements:
        - order_id (int): The ID of the order.
        - exchange (str): The exchange where the order was placed.
        - strategy_name (str): The name of the strategy used for the order.
        - api_key (str): The API key used for the order.
        - account_id (int): The ID of the account associated with the order.
        - param_id (int): The ID of the parameter used for the order.
        - symbol (str): The symbol of the order.
        - note (str): Additional notes about the order.

    Raises:
        mysql.connector.Error: If there is an error executing the SQL query.

    Note:
        Only orders that have not been marked as deleted are returned.

    """
    try:
        connection, cursor = xtb_get_connection()
        # current_timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # Fetch all rows that are not already marked as deleted
        select_query = """SELECT order_id, exchange, strategy_name, api_key, account_id, param_id, symbol, note 
                            FROM make_orders 
                            WHERE deleted_at IS NULL
                        """
        cursor.execute(select_query)
        result =[]
        list_make_orders = cursor.fetchall()
        for make_order in list_make_orders:
            result.append([
                        make_order[0], # order_id
                        make_order[1], # exchange
                        make_order[2], # strategy_name
                        make_order[3], # api_key
                        make_order[4], # account_id
                        make_order[5], # param_id
                        make_order[6], # symbol
                        make_order[7], # note
                    ])
        return result
    except mysql.connector.Error as err:
        logger_database.error(f"Error soft deleting rows: {err}")
        return err
        
    finally:
        xtb_close_connection(connection, cursor)

def xtb_delete_make_order(order_id):
    """
    Deletes a make order from the database.
    Parameters:
        order_id (int): The ID of the order to be soft deleted.

    Returns:
        None

    Raises:
        Exception: If an error occurs during the soft deletion process.

    Notes:
        - This function retrieves a database connection and cursor using the `get_connection()` function.
        - The current timestamp is obtained using `datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')`.
        - The `UPDATE` query is executed using the `cursor.execute()` method.
        - The `connection.commit()` method is called to commit the changes to the database.
        - If an error occurs during the soft deletion process, the error is logged using the `logger_database.error()` method.
        - The `close_connection()` function is called to close the database connection and cursor.

    """
    try:
        connection, cursor = xtb_get_connection()
        current_timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
       
        update_query = "UPDATE make_orders SET deleted_at = %s WHERE order_id = %s"
        cursor.execute(update_query, (current_timestamp, order_id))
        logger_database.info(f"Soft deleted row with id {order_id} successfully.")
        connection.commit()
        return True
    except Exception as err:
        logger_database.error(f"Error soft deleting rows: {err}")
        return err
        
    finally:
        xtb_close_connection(connection, cursor)


def insert_inventory_value(inventory):
    """
    Inserts an inventory value into the 'inventory_values' table in the MySQL database.

    Args:
        inventory (dict): A dictionary containing the inventory details. It should have the following keys:
            - 'exchange_name' (str): The name of the exchange.
            - 'base_symbol' (str): The base symbol.
            - 'quote_symbol' (str): The quote symbol.
            - 'quote' (float): The quote value.
            - 'base' (float): The base value.
            - 'inventory' (float): The inventory value.
            - 'price' (float): The price value.
            - 'quote_price' (float): The quote price value.

    Returns:
        None if the insertion is successful, otherwise an error message.

    Raises:
        Exception: If there is an error inserting the row into the database.

    """
    try:
        connection, cursor = xtb_get_connection()
        
        current_timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        current_unix_timestamp = int(time.time())
        insert_query = """
            INSERT INTO inventory_values (exchange_name, base_symbol, quote_symbol, quote, base, inventory, price, quote_price, 
                created_at, updated_at, time_stamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
        
        cursor.execute(insert_query, (inventory['exchange_name'], inventory['base_symbol'], inventory['quote_symbol'], inventory['quote'], 
                                      inventory['base'], inventory['inventory'], inventory['price'], inventory['quote_price'], 
                                      current_timestamp, current_timestamp, current_unix_timestamp))
        
        connection.commit()
        return True
    except Exception as err:
        logger_database.error(f"Error inserting row: {err}")
        return err
        
    finally:
        xtb_close_connection(connection, cursor)

def insert_volume_snapshots(snapshot):
    """
    Inserts a snapshot into the volume_snapshots table based on the provided snapshot data.

    Args:
        snapshot (dict): The snapshot data containing 'time_stamp', 'strategy_name', 'exchange', 'base_symbol', 'quote_symbol', 
                         'price', 'quote_price', 'base_volume', 'quote_volume', 'usd_volume', 'created_at', 'updated_at'.

    Returns:
        None if successful, otherwise returns an error message.
    """
    try:
        connection, cursor = xtb_get_connection()
        
        # current_timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Prepare the SELECT statement to get the last row based on the 'id' column
        query = f"""SELECT * 
                    FROM volume_snapshots 
                    WHERE strategy_name = '{snapshot['strategy_name']}' AND exchange = '{snapshot['exchange']}' 
                        AND base_symbol = '{snapshot['base_symbol']}' AND quote_symbol = '{snapshot['quote_symbol']}' 
                    ORDER BY id DESC LIMIT 1"""
        
        # Execute the SELECT statement
        cursor.execute(query)
        
        # Fetch the last row
        last_row = cursor.fetchone()
        if last_row is None or int(last_row[4]) < snapshot['time_stamp']:
            insert_query = """
                INSERT INTO volume_snapshots (time_stamp, strategy_name, exchange, base_symbol, quote_symbol, price, quote_price, 
                    base_volume, quote_volume, usd_volume, created_at, updated_at)
                VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
            cursor.execute(insert_query, (snapshot['time_stamp'], snapshot['strategy_name'], snapshot['exchange'], snapshot['base_symbol'], 
                                          snapshot['quote_symbol'], snapshot['price'], snapshot['quote_price'], snapshot['base_volume'], 
                                          snapshot['quote_volume'], snapshot['usd_volume'], snapshot['created_at'], snapshot['updated_at'] ))
        else: 
            update_query = """
                UPDATE volume_snapshots 
                SET price = %s, quote_price = %s, base_volume = base_volume + %s, 
                    quote_volume = quote_volume + %s, usd_volume = usd_volume + %s, updated_at = %s
                WHERE time_stamp = %s AND strategy_name = %s AND exchange = %s AND base_symbol = %s AND quote_symbol = %s
                """
            cursor.execute(update_query, (snapshot['price'], snapshot['quote_price'], snapshot['base_volume'], snapshot['quote_volume'], 
                                          snapshot['usd_volume'], snapshot['updated_at'], snapshot['time_stamp'], snapshot['strategy_name'], 
                                          snapshot['exchange'], snapshot['base_symbol'], snapshot['quote_symbol']))
        
        connection.commit()
        return None
        
    except Exception as err:
        logger_database.error(f"Error inserting row: {err}")
        return err
        
    finally:
        xtb_close_connection(connection, cursor)


    
