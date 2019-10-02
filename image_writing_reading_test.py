import aws_sql_logger.robot_logger as rl
import aws_sql_inspector.sql_inspector as inspector
import time

def read_file(filename):
    with open(filename, 'rb') as f:
        photo = f.read()
    return photo


def write_file(data, filename):
    with open(filename, 'wb') as f:
        f.write(data)

img_blob = read_file("sample.jpg")

#print(type(img_blob).__name__)

log = rl.RobotLogger()
inspector = inspector.SQLInspector()

#log.add_topic("image_topic", "bytes")

start_time = time.time()
log.write("image_topic", img_blob, str(__file__))

print("Average write time for integer topic: ", end='')
print((time.time()-start_time)/1)



query = inspector.get_query("topic_id = 4")
#print(query)
print(query.get().head(1))