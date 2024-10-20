import requests
import time
from bs4 import BeautifulSoup
import pandas as pd
import json
from abc import ABC, abstractmethod

class FileSaver(ABC):
    @abstractmethod
    def save(self, data, file_path):
        pass

class TxtFileSaver(FileSaver):
    def save(self, data, file_path):
        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(str(data))
        print(f"Data saved to TXT file: {file_path}")

class CsvFileSaver(FileSaver):
    def save(self, data, file_path):
        data.to_csv(file_path, index=False)
        print(f"Data saved to CSV file: {file_path}")

class JsonFileSaver(FileSaver):
    def save(self, data, file_path):
        with open(file_path, 'w', encoding='utf-8') as json_file:
            json.dump(data, json_file, ensure_ascii=False, indent=4)
        print(f"Data saved to JSON file: {file_path}")


class ExcelFileSaver(FileSaver):
    def save(self, data, file_path):
        data.to_excel(file_path, index=False)
        print(f"Data saved to Excel file: {file_path}")

class FileSaverFactory:
    @staticmethod
    def get_file_saver(file_type):
        print(file_type)
        if file_type == 'txt':
            return TxtFileSaver()
        elif file_type == 'csv':
            return CsvFileSaver()
        elif file_type == 'json':
            return JsonFileSaver()
        elif file_type == 'xlsx':
            return ExcelFileSaver()
        else:
            raise ValueError(f"Unsupported file type: {file_type}")

class BaseCrawler:
    def __init__(self, url_base, company_name, year, type_report="4 QUARTER", quarter="1"):
        self.url = url_base
        self.company_name = company_name
        self.year = year
        self.type_report = type_report
        self.quarter = quarter


    def get_data_from_url(self):
        response = requests.get(self.url)
        soup = BeautifulSoup(response.content, 'html.parser')
        return soup

    def extract_table_content(self, url = None, index_table=0
                              ):
        if url is None:
            url = self.url
        tables = pd.read_html(url)
        if index_table < len(tables):
            df = pd.DataFrame(tables[index_table])
            num_columns = df.shape[1]
            column_names = [f'column_{i}' for i in range(num_columns)] 
            df.columns = column_names
            return df
        else:
            raise IndexError(f"Table index {index_table} out of range")

    def save_data(self, data, file_type, file_path):
        """
        file_type: txt, csv, json, xlsx
        """
        file_saver = FileSaverFactory.get_file_saver(file_type)
        file_saver.save(data, file_path)
    
    def generate_column_quarter_names(self,quarter, year, number_quarter, name_of_tables = "Report"):
        quarter = int(quarter)
        year = int(year)
        column_names = []

        for _ in range(int(number_quarter)):
            column_names.append(f"{year}_q{quarter}")
            quarter -= 1
            if quarter == 0:
                year -= 1
                quarter = 4
        column_names.append(name_of_tables)
        column_names.reverse()
        
        return column_names
    
    def generate_column_year_names(self, number_year, year, name_of_tables = "Report"):
        column_names = []
        for i in range(number_year):
            year = int(year) - i
            column_names.append(f"{year}")
        column_names.append(name_of_tables)
        column_names.reverse()
        return column_names
    