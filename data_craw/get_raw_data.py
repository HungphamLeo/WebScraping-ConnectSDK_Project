import requests
from bs4 import BeautifulSoup
import json
import csv
import pandas as pd

def get_data_from_url(url, save_to_file=False, file_path=None):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    if save_to_file and file_path:
        save_to_file_txt(str(soup), file_path)
    return soup

def extract_table_content(url, index_table, save_to_file=False, type_file=None, file_path=None):
    
    source = pd.read_html(url)
    if save_to_file and type_file and file_path:
        if type_file == 'txt':
            save_to_file_txt(source[index_table], file_path)
        elif type_file == 'json':
            save_as_json(source[index_table], file_path)
        elif type_file == 'csv':
            save_as_csv(pd.DataFrame(source[index_table]), file_path)
        elif type_file == 'xlsx':
            save_to_file_excel(pd.DataFrame(source[index_table]), file_path)
    return source

def save_to_file_txt(data, file_path):
    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(str(data))
    print(f"Data saved to TXT file: {file_path}")

def save_to_file_excel(data, file_path):
    data.to_excel(file_path, index=False)
    print(f"Data saved to Excel file: {file_path}")

def save_as_json(data, json_file_path):
    with open(json_file_path, 'w', encoding='utf-8') as json_file:
        json.dump(data, json_file, ensure_ascii=False, indent=4)
    print(f"Data saved to JSON file: {json_file_path}")

def save_as_csv(data, csv_file_path):
    data.to_csv(csv_file_path, index=False)
    print(f"Data saved to CSV file: {csv_file_path}")

url = "https://s.cafef.vn/bao-cao-tai-chinh/tta/incsta/2022/1/0/0/luu-chuyen-tien-te-gian-tiep-.chn"
table_content = extract_table_content(url, 4, True, 'xlsx', "income_sta_3.xlsx")

# income_sta_1
#cash_flow_1