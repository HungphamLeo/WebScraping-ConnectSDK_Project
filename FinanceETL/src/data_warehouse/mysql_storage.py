import mysql.connector

class MySQLStorage:
    def __init__(self, config):
        self.connection = mysql.connector.connect(**config)

    def insert(self, query, data):
        cursor = self.connection.cursor()
        cursor.execute(query, data)
        self.connection.commit()
