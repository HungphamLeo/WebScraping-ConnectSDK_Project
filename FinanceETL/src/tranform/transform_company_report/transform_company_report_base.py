import pandas as pd
import json
import pyarrow as pa
import pyarrow.hdfs as hdfs


class TransformStatementBase:
    def __init__(self, file_path: str, file_type: str):
        self.file_path = file_path
        self.file_type = file_type
        self.data = None
    
    def read_data(self):
        hdfs_client = hdfs.connect('localhost', port=50070)
        if self.file_type == 'json':
            with hdfs_client.open(self.file_path, 'rb') as f:
                self.data = json.load(f)
        elif self.file_type == 'xlsx':
            with hdfs_client.open(self.file_path, 'rb') as f:
                self.data = pd.read_excel(f)
        elif self.file_type == 'csv':
            with hdfs_client.open(self.file_path, 'rb') as f:
                self.data = pd.read_csv(f)
        else:
            raise ValueError("Unsupported file type: {}".format(self.file_type))
    
    