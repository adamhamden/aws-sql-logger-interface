import aws_sql_logger.robot_logger as rl
import aws_sql_inspector.sql_inspector as inspector
import time


log = rl.RobotLogger()
inspector = inspector.SQLInspector()

#log.add_topic("integer_topic1", "int")

start_time = time.time()
for i in range(0,10):
    print(i)
    for i in range(0, 3):
        while True:
            try:
                log.write("integer_topic1", i, str(__file__))
            except:
                time.sleep(10)
                continue
            break

print("Average write time for integer topic: ", end='')
print((time.time()-start_time)/10)

query = inspector.get_query("1=1")
print(query)