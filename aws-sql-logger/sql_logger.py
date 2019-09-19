# !/usr/bin/env python3

import yaml
import mysql.connector


class SQLLogger:

    def __init__(self):

        print("1")
        self.cfg = None
        with open("config.yml", 'r') as ymlfile:
            print("2")
            self.cfg = yaml.safe_load(ymlfile)

        print("can open")

        self.db = mysql.connector.connect(
          host= self.cfg["sql_database"]["host"],
          port = self.cfg["sql_database"]["port"],
          user=self.cfg["sql_database"]["user"],
          passwd=self.cfg["sql_database"]["password"],
        )

        print(self.db)

        self.database_name = self.cfg["log_info"]["database_name"]
        self.keepLocalCopy = self.cfg["log_info"]["keep_local_copy"]

        self.cursor = db.cursor()

        self.cursor.execute("CREATE DATABASE IF NOT EXISTS ?", (self.database_name,))
        self.cursor.execute("USE ?", (self.database_name,))



        self.cursor.execute("CREATE TABLE IF NOT EXISTS " +
                            "log(" +
                            "timestamp TEXT NOT NULL, " +
                            "topic_id TEXT NOT NULL, " +
                            "data BLOB NOT NULL, " +
                            "source TEXT NOT NULL, " +
                            "mismatched BOOLEAN"
                            ")"
                            )

        self.cursor.execute("CREATE TABLE IF NOT EXISTS " +
                            "local_log(" +
                            "timestamp TEXT NOT NULL, " +
                            "topic_id TEXT NOT NULL, " +
                            "data BLOB NOT NULL, " +
                            "source TEXT NOT NULL, " +
                            "mismatched BOOLEAN"
                            ")"
                            )

        self.cursor.execute("CREATE TABLE IF NOT EXISTS " +
                            "topics(" +
                            "topic_id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                            "topic_name TEXT NOT NULL, " +
                            "data_type TEXT NOT NULL" +
                            ")"
                            )

        self.cursor.execute("SHOW TABLES")

        for x in self.cursor:
            print(x)


if __name__ == "__main__":

    logger = SQLLogger()
