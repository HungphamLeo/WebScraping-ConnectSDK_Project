class SingletonDB:
    _instance = None

    def __new__(cls, config):
        if cls._instance is None:
            cls._instance = super(SingletonDB, cls).__new__(cls)
            cls._instance.connection = mysql.connector.connect(**config)
        return cls._instance
