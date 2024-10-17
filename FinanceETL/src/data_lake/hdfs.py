from hdfs import InsecureClient

class HDFSStorage:
    def __init__(self, hdfs_url):
        self.client = InsecureClient(hdfs_url)

    def save(self, file_path, data):
        with self.client.write(file_path, encoding='utf-8') as writer:
            writer.write(data)
