import yaml
import mysql.connector
import time

class SQLLogger:

    def __init__(self):

        self.cfg = None
        with open("config.yml", 'r') as ymlfile:
            self.cfg = yaml.safe_load(ymlfile)


        self.db = mysql.connector.connect(
          host= self.cfg["sql_database"]["host"],
          port = self.cfg["sql_database"]["port"],
          user=self.cfg["sql_database"]["user"],
          passwd=self.cfg["sql_database"]["password"],
        )

        self.database_name = self.cfg["log_info"]["database_name"]
        self.keepLocalCopy = self.cfg["log_info"]["keep_local_copy"]

        self.cursor = self.db.cursor(prepared=True)