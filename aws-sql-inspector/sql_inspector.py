import yaml
import mysql.connector
from prettytable import PrettyTable
import time


class Query:

    def __init__(self, list_of_matches, header=[]):
        self.header = header
        self.data = list_of_matches


    def __repr__(self):

        x = PrettyTable()
        x.field_names = self.header

        for match in self.data:
            x.add_row(list(match))

        return str(x)

    def get(self):
        return pd.DataFrame(self.data)


class SQLInspector:

    def __init__(self):

        self.cfg = None
        with open("config.yml", 'r') as ymlfile:
            self.cfg = yaml.safe_load(ymlfile)


        self.db = mysql.connector.connect(
            host=self.cfg["sql_database"]["host"],
            port=self.cfg["sql_database"]["port"],
            user=self.cfg["sql_database"]["user"],
            passwd=self.cfg["sql_database"]["password"],
            database= self.cfg["log_info"]["database_name"],

        )

        self.database_name = self.cfg["log_info"]["database_name"]
        self.keepLocalCopy = self.cfg["log_info"]["keep_local_copy"]

        self.cursor = self.db.cursor(prepared=True)

    def get_query(self, condition):

        execute_statement = "SELECT * FROM log WHERE " + condition
        self.cursor.execute(execute_statement)
        list_of_matches = self.cursor.fetchall()
        query = Query(list_of_matches)

        return query

if __name__ == "__main__":

    inspector = SQLInspector()

    query = inspector.get_query("topic_id = 1")
    time.sleep(10)
    print(query)