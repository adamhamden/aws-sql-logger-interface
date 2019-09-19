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

        print(self.db)

        self.database_name = self.cfg["log_info"]["database_name"]
        self.keepLocalCopy = self.cfg["log_info"]["keep_local_copy"]

        self.cursor = self.db.cursor(prepared=True)

        self.cursor.execute("CREATE DATABASE IF NOT EXISTS  "+ self.database_name)

        self.cursor.execute("SHOW DATABASES")

        for x in self.cursor:
            print(x)

        self.db = mysql.connector.connect(
            host=self.cfg["sql_database"]["host"],
            port=self.cfg["sql_database"]["port"],
            user=self.cfg["sql_database"]["user"],
            passwd=self.cfg["sql_database"]["password"],
            database= self.cfg["log_info"]["database_name"],

        )
        self.cursor = self.db.cursor(prepared=True)

        self.cursor.execute("CREATE TABLE IF NOT EXISTS " +
                            "log(" +
                            "timestamp TEXT NOT NULL, " +
                            "topic_id TEXT NOT NULL, " +
                            "data BLOB NOT NULL, " +
                            "source TEXT NOT NULL, " +
                            "mismatched BOOLEAN"
                            ")"
                            )

        self.cursor.execute("CREATE TABLE IF NOT EXISTS " 
                            "local_log(" 
                            "timestamp TEXT NOT NULL, " 
                            "topic_id TEXT NOT NULL, " 
                            "data BLOB NOT NULL, "
                            "source TEXT NOT NULL, "
                            "mismatched BOOLEAN"
                            ")"
                            )

        self.cursor.execute("CREATE TABLE IF NOT EXISTS "
                            "topics(" 
                            "topic_id INT AUTO_INCREMENT, " 
                            "topic_name TEXT NOT NULL, " 
                            "data_type TEXT NOT NULL, "
                            "PRIMARY KEY (topic_id)"
                            ")"
                            )

        self.cursor.execute("SHOW TABLES")

        for x in self.cursor:
            print(x)


        self.cursor.execute("DROP TABLE log")
        self.cursor.execute("DROP TABLE local_log")
        self.cursor.execute("DROP TABLE topics")

        time.sleep(200)


if __name__ == "__main__":

    logger = SQLLogger()
