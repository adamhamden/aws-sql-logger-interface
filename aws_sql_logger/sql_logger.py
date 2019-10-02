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

        """self.cursor.execute("DROP DATABASE test_database_1")
        self.db.commit()"""

        self.cursor.execute("CREATE DATABASE IF NOT EXISTS  "+ self.database_name)

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

        self.db.commit()

    def backup(self):

        if self.keepLocalCopy:
            self.sql = "SELECT * FROM local_log"
            self.cursor = self.db.cursor()
            self.cursor.execute(self.sql)
            self.local_log = self.cursor.fetchall()

            self.sql = "SELECT * FROM topics"
            self.cursor = self.db.cursor()
            self.cursor.execute(self.sql)
            self.topics = self.cursor.fetchall()


            filename = "backup_local_log_" + str(time.time()) + ".txt"

            with open(filename, 'w+') as file:
                file.write(str(self.topics))
                file.write("\n")
                file.write(str(self.local_log))

        self.cursor.execute("DROP TABLE local_log")

        self.db.commit()

    def add_topic(self, topic_name, data_type):

        self.sql = "SELECT * FROM topics WHERE topic_name = %s"
        self.adr = (topic_name,)
        self.cursor.execute(self.sql, self.adr)
        topic_matches = self.cursor.fetchall()
        if len(topic_matches) != 0:
            raise ValueError("Attempting add an existing topic")

        self.sql = "INSERT INTO topics VALUES (NULL, %s, %s)"
        self.adr = (topic_name, data_type,)
        self.cursor.execute(self.sql, self.adr)

        self.db.commit()

    def write(self, topic_name, data, source, is_keep_local_copy=False):

        self.sql = "SELECT * from topics WHERE topic_name = %s"
        self.adr = (topic_name,)
        self.cursor.execute(self.sql, self.adr)

        topic_matches = self.cursor.fetchall()
        if len(topic_matches) != 1:
            raise ValueError("Attempting to write to an invalid topic name")

        topic_id = topic_matches[0][0]
        topic_data_type = topic_matches[0][2].decode()
        insert_data_type = str(type(data).__name__)

        is_mismatched = 0
        if topic_data_type != insert_data_type:
            is_mismatched = 1


        self.sql = "INSERT INTO log(timestamp, topic_id, data, source, mismatched) VALUES (NOW(),%s,%s,%s,%s)"
        self.adr = (topic_id, data, source, is_mismatched,)
        self.cursor.execute(self.sql, self.adr)

        if is_keep_local_copy:
            self.sql = "INSERT INTO local_log(timestamp, topic_id, data, source, mismatched) VALUES (NOW(),%s,%s,%s,%s)"
            self.cursor.execute(self.sql, self.adr)

if __name__ == "__main__":

    logger = SQLLogger()
    #logger.add_topic("some_topic", "int")
    #logger.add_topic("some_new_topic", "int")

    logger.write("some_topic", 234, "my_file.py")
    logger.write("some_topic", 124, "my_file.py")
    logger.write("some_topic", 23457, "my_file.py")
    logger.write("some_topic", 344657, "my_file.py")

    logger.add_topic("my_new_topic", "int")
    logger.add_topic("my_new_topic", "string")


    logger.backup()
