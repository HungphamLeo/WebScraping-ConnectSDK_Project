from .transform_company_report_base import TransformStatementBase
from ...config.config import Config
import pandas as pd

class TransformCashFlow(TransformStatementBase):
    def __init__(self, file_path: str, file_type: str):
        super().__init__(file_path, file_type)
        self.file_path = file_path
        self.file_type = file_type
        self.data = None

    def transform_cash_flow_data(self, input_table, source: str):
        original_column_name = input_table.columns[0]
        data_dict = input_table.set_index(original_column_name).to_dict()
        transformed_dict = {}
        for year, data in data_dict.items():
            if source == Config.ALPHA_VANTAGE:
                transformed_dict[year] = self.mapping_alpha_vantage_cash_flow(data)
            elif source == Config.CAFE_F:
                transformed_dict[year] = self.mapping_cafeF_cash_flow(data)
            else:
                raise ValueError("Unsupported source: {}".format(source))
        df = pd.DataFrame(transformed_dict)
        df.insert(0, original_column_name, input_table[original_column_name])
        return df
    
    