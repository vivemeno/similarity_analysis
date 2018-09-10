
import _sqlite3 as sqlite;

class DatabaseOperations:
    def __init__(self):
        self.__filePath = "/home/vivemeno/MWD Project Data/database/test5.db";
        print("This is the constructor method.")


    def executeWriteQueries(self, queries):
        try:
            conn = sqlite.connect(self.__filePath);
            c = conn.cursor();
            for query in queries:
                c.execute(query)
            conn.commit();
        except sqlite.Error as e:
            print(e)
        finally:
            conn.close()

    def executeSelectQuery(self, query):
        try:
            conn = sqlite.connect(self.__filePath);
            c = conn.cursor();
            c.execute(query)
            return c.fetchall()
        except sqlite.Error as e:
            print(e)
        finally:
            conn.close()
