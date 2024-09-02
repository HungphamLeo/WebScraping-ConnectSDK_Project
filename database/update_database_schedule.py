import os
import json
import schedule
import time
from trading_data_provider.xtb_connection import xapi_data_provider
from database.trading_database import insert_symbol_db, insert_news_db
json_key = './trading_data_provider/private_infomations/my_credentials.json'
cron_job_json_file = './database/cron_job.json'

def update_all_symbol(symbol_list):
    for symbol in symbol_list:
        insert_symbol_db(symbol)

def update_news(news_list):
    for news in news_list:
        insert_news_db(news)

def job_function(json_key,cron_job_json_file):
    day_mapping = {
        'monday': schedule.every().monday,
        'tuesday': schedule.every().tuesday,
        'wednesday': schedule.every().wednesday,
        'thursday': schedule.every().thursday,
        'friday': schedule.every().friday,
        'saturday': schedule.every().saturday,
        'sunday': schedule.every().sunday
    }
    data_input = {}
    exchange = 'xtb'
    if os.path.exists(json_key):
        with open(json_key, 'r', encoding='utf-8') as f:
            data_input = json.load(f)
            f.close()

    if os.path.exists(cron_job_json_file):
        with open(cron_job_json_file, 'r', encoding='utf-8') as f:
            crob_job = json.load(f)
    api_key = data_input[exchange]['api_key']
    secret_key = data_input[exchange]['secret_key']
    client = xapi_data_provider(api_key= api_key,secret_key= secret_key)
    
    event_list = client.get_calendars()
    for job in crob_job['jobs']:
        function_name = job['function']
        time_str = job['time']
        day_of_week = job['day_of_week']
        if function_name == 'get_all_symbol':
            symbol_list = client.get_all_symbol()
            day_mapping[day_of_week].at(time_str).do(update_all_symbol(symbol_list))
        elif function_name == 'get_calendars':
            event_list = client.get_calendars()
            day_mapping[day_of_week].at(time_str).do(update_news(event_list))
        else:
            pass

job_function()
while True:
    schedule.run_pending()
    time.sleep(3600)