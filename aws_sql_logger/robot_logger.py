import aws_sql_logger.sql_logger as logger
import yaml
import mysql.connector
import time
import uuid


class RobotLogger(logger.SQLLogger):

    def __init__(self):
        super().__init__()
        self.robot_id = str(uuid.getnode())
        self.cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME='log';", (self.database_name,))

        self.header = self.cursor.fetchall()
        self.new_header = []
        for col in self.header:
            self.new_header.append(col[0].decode())

        if "robot_id" not in self.new_header:
            self.cursor.execute("ALTER TABLE log ADD COLUMN robot_id TEXT;")
            self.cursor.execute("ALTER TABLE local_log ADD COLUMN robot_id TEXT;")

    def write(self, topic_name, data, source, is_keep_local_copy=False):
        super().write(topic_name, data, source, is_keep_local_copy)
        #time.sleep(.1)
        self.cursor.execute("UPDATE log SET robot_id = ? ORDER BY timestamp DESC LIMIT 1", (self.robot_id,))

        if is_keep_local_copy:
            self.cursor.execute("UPDATE local_log SET robot_id = ? ORDER BY timestamp DESC LIMIT 1", (self.robot_id,))

        self.db.commit()

    def add_topic(self, topic_name, data_type):
        super().add_topic(topic_name, data_type)

    def backup(self):
        super().backup()


if __name__ == "__main__":
    logger = RobotLogger()
    logger.add_topic("topic_1", "int")
    logger.write("topic_1", 99999, "my_file.py")

    logger.backup()