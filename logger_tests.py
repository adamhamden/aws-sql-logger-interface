import unittest
import aws_sql_inspector.sql_inspector as si
import aws_sql_logger.robot_logger as rl
import threading
import time


inspector = si.SQLInspector()
query = None

def update_query(table_name):
    global query
    query = str(inspector.get_query(table_name,"1=1"))

class TestLogger(unittest.TestCase):

    def testTopics(self):
        robot_logger = rl.RobotLogger()
        robot_logger.clear_db()
        for i in range(0, 5):
            robot_logger.add_topic("test_thread" + str(i), "int")

        time.sleep(5)
        update_query("topics")
        print(query)
        self.assertEqual(9, int(len(query.split('\n'))))

    def testThread(self):
        robot_logger = rl.RobotLogger()
        robot_logger.clear_db()
        robot_logger.add_topic("test_thread_1", "int")
        for i in range(0, 5):
            robot_logger.write("test_thread_1", i, str(__file__), True)

        time.sleep(5)
        update_query("log")
        print(query)
        self.assertEqual(9, int(len(query.split('\n'))))

    def testAddInvalidTopic(self):
        robot_logger = rl.RobotLogger()
        robot_logger.clear_db()
        robot_logger.add_topic("test_thread_1", "int")

        try:
            robot_logger.add_topic("test_thread_1", "int")
        except:
            self.assertTrue(True)
        else:
            self.assertTrue(False)


    def testWriteToInvalidTopic(self):
        robot_logger = rl.RobotLogger()
        robot_logger.clear_db()
        robot_logger.add_topic("test_thread_1", "int")

        try:
            robot_logger.write("test_thread_2", 1, str(__file__), True)
        except:
            self.assertTrue(True)
        else:
            self.assertTrue(False)

    def testMultipleWrites(self):

        robot_logger1 = rl.RobotLogger()
        robot_logger1.clear_db()
        robot_logger1.add_topic("test_thread_1", "int")

        robot_logger2 = rl.RobotLogger()
        robot_logger2.add_topic("test_thread_2", "int")

        x = threading.Thread(target=robot_logger1.write, args=("test_thread_1", 1, str(__file__), True))
        y = threading.Thread(target=robot_logger2.write, args=("test_thread_2", 2, str(__file__), True))
        z = threading.Thread(target=robot_logger1.write, args=("test_thread_1", 1, str(__file__), True))
        k = threading.Thread(target=robot_logger2.write, args=("test_thread_2", 2, str(__file__), True))

        x.start()
        z.start()
        y.start()
        k.start()

        time.sleep(5)
        update_query("log")
        print(query)

        self.assertEqual(8, int(len(query.split('\n'))))


