import yaml
import mysql.connector
from mysql.connector import pooling
import time
import threading


class RobotLogger:

    def __init__(self):
        self.cfg = None
        with open("config.yml", 'r') as ymlfile:
            self.cfg = yaml.safe_load(ymlfile)

        self.db = mysql.connector.connect(
          host=self.cfg["sql_database"]["host"],
          port=self.cfg["sql_database"]["port"],
          user=self.cfg["sql_database"]["user"],
          passwd=self.cfg["sql_database"]["password"],
        )

        self.database_name = self.cfg["log_info"]["database_name"]
        self.keepLocalCopy = self.cfg["log_info"]["keep_local_copy"]
        self.cursor = self.db.cursor(prepared=True)
        self.cursor.execute("CREATE DATABASE IF NOT EXISTS  " + self.database_name)
        self.db.commit()
        self.db.close()

        self.db = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="pool",
            pool_size=1,
            host=self.cfg["sql_database"]["host"],
            port=self.cfg["sql_database"]["port"],
            user=self.cfg["sql_database"]["user"],
            passwd=self.cfg["sql_database"]["password"],
            database=self.cfg["log_info"]["database_name"],
        )

        connection = self._get_connection()
        self.cursor = connection.cursor(prepared=True)

        self._create_tables()

        self.robot_id = self.cfg["log_info"]["robot_id"]

        if self._is_robot_id_assigned():
            self._create_and_store_robot_id()

        connection.commit()
        connection.close()

    def _get_connection(self):
        connection = None
        while True:
            try:
                connection = self.db.get_connection()
            except Exception as e:
                print(e)  # TODO check that you're not hiding a bug here: how it hangs
                continue
            else:
                break
        return connection

    def _is_robot_id_assigned(self):
        return self.robot_id is None

    def _create_and_store_robot_id(self):
        self.cursor.execute("INSERT INTO robots VALUES (NULL)")
        self.cursor.execute("SELECT robot_id FROM robots ORDER BY robot_id DESC LIMIT 0, 1")
        self.cfg["log_info"]["robot_id"] = self.cursor.fetchone()[0]
        self.robot_id = self.cfg["log_info"]["robot_id"]
        with open("config.yml", "w") as f:
            yaml.dump(self.cfg, f)

    def _create_tables(self):
        self.cursor.execute(
            "CREATE TABLE IF NOT EXISTS " +
            "log(" +
            "timestamp TEXT NOT NULL, " +
            "topic_id TEXT NOT NULL, " +
            "data BLOB NOT NULL, " +
            "source TEXT NOT NULL, " +
            "mismatched BOOLEAN,"
            "robot_id INT"
            ")"
        )
        self.cursor.execute(
            "CREATE TABLE IF NOT EXISTS "
            "local_log("
            "timestamp TEXT NOT NULL, "
            "topic_id TEXT NOT NULL, "
            "data BLOB NOT NULL, "
            "source TEXT NOT NULL, "
            "mismatched BOOLEAN,"
            "robot_id INT"
            ")"
        )
        self.cursor.execute(
            "CREATE TABLE IF NOT EXISTS "
            "topics("
            "topic_id INT AUTO_INCREMENT, "
            "topic_name TEXT NOT NULL, "
            "data_type TEXT NOT NULL, "
            "PRIMARY KEY (topic_id)"
            ")"
        )
        self.cursor.execute(
            "CREATE TABLE IF NOT EXISTS "
            "robots("
            "robot_id INT AUTO_INCREMENT,"
            "PRIMARY KEY (robot_id)"
            ")"
        )

    def add_topic(self, topic_name, data_type):

        connection = None
        while True:
            try:
                connection = self.db.get_connection()
            except:
                continue
            else:
                break

        self.cursor = connection.cursor(prepared=True)
        self.sql = "SELECT * FROM topics WHERE topic_name = %s"
        self.adr = (topic_name,)
        self.cursor.execute(self.sql, self.adr)

        topic_matches = self.cursor.fetchall()
        if len(topic_matches) != 0:
            raise ValueError("Attempting add an existing topic")

        self.sql = "INSERT INTO topics VALUES (NULL, %s, %s)"
        self.adr = (topic_name, data_type,)
        self.cursor.execute(self.sql, self.adr)

        connection.commit()
        connection.close()

    def backup(self):

        connection = self._get_connection()

        self.cursor = connection.cursor(prepared=True)

        if self.keepLocalCopy:

            self.cursor.execute("SELECT * FROM local_log")
            self.log = self.cursor.fetchall()
            self.cursor.execute("SELECT * FROM topics")
            self.topics = self.cursor.fetchall()

            filename = "backup_local_log_" + str(time.time()) + ".txt"
            file = open(filename, 'w+')
            file.write(str(self.topics))
            file.write("\n")
            file.write(str(self.log))

        self.cursor.execute("DROP TABLE local_log;")
        self.cursor.execute("CREATE TABLE IF NOT EXISTS " 
                            "local_log(" 
                            "timestamp TEXT NOT NULL, " 
                            "topic_id TEXT NOT NULL, " 
                            "data BLOB NOT NULL, "
                            "source TEXT NOT NULL, "
                            "mismatched BOOLEAN,"
                            "robot_id INT"
                            ")"
                            )
        connection.commit()
        connection.close()

    def write(self, topic_name, data, source, is_keep_local_copy=False):

        connection = None
        while True:
            try:
                connection = self.db.get_connection()
            except:
                continue
            else:
                break

        self.cursor = connection.cursor()

        self.sql = "SELECT * from topics WHERE topic_name = %s"
        self.adr = (topic_name,)
        self.cursor.execute(self.sql, self.adr)

        topic_matches = self.cursor.fetchall()

        if len(topic_matches) != 1:
            raise ValueError("Attempting to write to an invalid topic name")

        connection.commit()
        connection.close()

        self.topic_id = topic_matches[0][0]

        self.topic_data_type = topic_matches[0][2]
        self.insert_data_type = str(type(data).__name__)

        self.is_mismatched = 0
        if self.topic_data_type != self.insert_data_type:
            self.is_mismatched = 1

        self.adr = (self.topic_id, data, source, self.is_mismatched, is_keep_local_copy)
        self.add_thread(self.write_callback, (self.adr,))

    def add_thread(self, callback, arguments):
        self.x = threading.Thread(target=callback, args=arguments)
        self.x.start()

    def write_callback(self, insert_info):

        connection = None
        while True:
            try:
                connection = self.db.get_connection()
            except:
                continue
            else:
                break

        self.cursor = connection.cursor()

        self.sql = "INSERT INTO log VALUES (NOW(),%s,%s,%s,%s,%s)"
        self.adr = (insert_info[0], insert_info[1], insert_info[2], insert_info[3], self.robot_id,)
        self.cursor.execute(self.sql, self.adr)

        if insert_info[4]:
            self.sql = "INSERT INTO local_log VALUES (NOW(),%s,%s,%s,%s,%s)"
            self.adr = (insert_info[0], insert_info[1], insert_info[2], insert_info[3], self.robot_id,)
            self.cursor.execute(self.sql, self.adr)

        connection.commit()
        connection.close()

    def _clear_db(self):

        connection = None
        while True:
            try:
                connection = self.db.get_connection()
            except:
                continue
            else:
                break

        self.cursor = connection.cursor()
        self.cursor.execute("DROP TABLE log;")
        self.cursor.execute("DROP TABLE local_log;")
        self.cursor.execute("DROP TABLE topics;")
        self.cursor.execute("DROP TABLE robots;")

        self._create_tables()
        connection.commit()
        connection.close()

