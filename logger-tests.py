import robot_logger as rl
import sql_insepctor as inspector
import unittest

class TestStateLogger(unittest.TestCase):


    def testAddTopic(self):
        rlogger = rl.RobotLogger()
        rlogger.add_topic('Age', 'int')


    def testWriteToValidTopic(self):
        rlogger.write('Age', 1234, "file.txt", True)
        rlogger.write('Age', "somee1", "file.txt", False)
        rlogger.write('Age', "somee", "file.txt", True)
        rlogger.write('Age',"Hello", "file.txt", True)