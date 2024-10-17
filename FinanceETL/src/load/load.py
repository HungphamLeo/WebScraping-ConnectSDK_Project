class Loader:
    def __init__(self, storage):
        self.storage = storage

    def load(self, data):
        # Lưu dữ liệu vào kho lưu trữ
        self.storage.save("path_or_query", data)
