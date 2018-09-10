
import os.path
from pathlib import Path
from databaseOperations import DatabaseOperations;
from initialSetup import InitialSetup;
from queryProcessor import QueryProcessor;

filePath = "/home/vivemeno/MWD Project Data/database/test5.db";
my_file = Path(filePath);
database_operations = DatabaseOperations()
if not my_file.is_file():


    initialSetup = InitialSetup(database_operations);
    initialSetup.setup_database_from_devset_data();
    # databaseOps = DatabaseOperations()
    # databaseOps.createConn(filePath)

query_processor = QueryProcessor(database_operations);
print("Enter user id, Model and k")
input_str = input();
splitInput = input_str.split();
user = splitInput[0]
model = splitInput[1]
k = splitInput[2]

query_processor.find_similar_users(user, model, int(k));



print("hello")
print(1+90)