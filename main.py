import json

with open("config.json",'r+') as config_json:
    config = json.load(config_json)
check = config["libraries"]["Num_Of_Starts"]

if check[0]>0:
    import Postgress
    Settings=Postgress.PostgresBench(config["PostgresDataBase"],check[0],config["libraries"]["Folder_with_CSV"])
    Postgress.start(Settings)

if check[1]>0:
    import SQLite
    Settings=SQLite.SQLiteBench(check[1],config["libraries"]["Folder_with_CSV"])
    SQLite.start(Settings)

if check[2]>0:
    import Pandas
    Settings=Pandas.PandasBench(check[2], config["libraries"]["Folder_with_CSV"])
    Pandas.start(Settings)

if check[3] > 0:
    import DuckDB
    Settings=DuckDB.PandasBench(check[3],config["libraries"]["Folder_with_CSV"])
    DuckDB.start(Settings)

if check[4] > 0:
    import SQLAlchemy
    Settings=SQLAlchemy.SQLAlchemyBench(config["PostgresDataBase"],check[4],config["libraries"]["Folder_with_CSV"])
    SQLAlchemy.start(Settings)