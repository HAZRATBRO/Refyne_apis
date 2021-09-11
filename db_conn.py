import psycopg2 as ps

def singleton(class_):
    instances = {}
    def getinstance(*args, **kwargs):
        if class_ not in instances:
            instances[class_] = class_(*args, **kwargs)
        return instances[class_]
    return getinstance

@singleton
class DBConnect:
    def __init__(self) -> None:
        self.conn = ps.connect(
            host="localhost",
            database="refyne_task",
            user="postgres",
            password="tigerdb",
        )

