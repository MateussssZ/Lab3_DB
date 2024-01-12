import time
from sqlalchemy import create_engine,MetaData, Table,select,func,extract
import pandas as pd

class SQLAlchemyBench():
    DBInfo={}
    Num_Of_Tests=0
    CSV_Folder=''

    def __init__(self,DBInfo,Num_Of_Tests,CSV_Folder):
        self.Num_Of_Tests=Num_Of_Tests
        self.DBInfo=DBInfo
        self.CSV_Folder=CSV_Folder

def create_table(conn,LibraryInfo):
    tiny_data = pd.read_csv(
        LibraryInfo+"/nyc_yellow_tiny.csv")  # Превращаем csv в dataframe и из него строим нужную нам таблицу на базе данных
    tiny_data.to_sql('table_1', conn, if_exists='replace', index=False)

    big_data = pd.read_csv(LibraryInfo+"/nyc_yellow_big.csv", chunksize=9000000)
    for chunk in big_data:  #Аналогично с большим датасетом, но его разбиваем на чанки(чтобы не было SIGKILL и др. подобных ошибок
        chunk.to_sql('table_2', conn, if_exists='append', index=False, chunksize=10000, method='multi')

def first_query(table,total_time,conn):
    query = (select(table.c.VendorID, func.count()) #Выбрать VendorID и count(*)
                 .select_from(table) #Из tiny_table
                 .group_by(table.c.VendorID)) #Сгруппировать по VendorID

    start = time.time() #Засекаем время
    conn.execute(query)
    total_time[0]+=time.time()-start

def second_query(table,total_time,conn):
    query=(select(table.c.passenger_count,func.avg(table.c.total_amount)) #Выбрать passenger_count,avg(total_amount)
           .select_from(table) #Из tiny_table
           .group_by(table.c.passenger_count))  #Сгруппировать по passenger_count

    start=time.time() #Засекаем время
    conn.execute(query)
    total_time[1]+=time.time()-start

def third_query(table,total_time,conn):
    query = (select(table.c.passenger_count, extract("year", table.c.tpep_pickup_datetime),func.count()) #Выбрать passenger_count, year из pickup_datetime, count(*)
             .select_from(table) #Из tiny_table
             .group_by(table.c.passenger_count, extract("year", table.c.tpep_pickup_datetime))) #Сгруппировать по 1 и 2

    start=time.time() #Засекаем время
    conn.execute(query)
    total_time[2]+=time.time()-start

def fourth_query(table,total_time,conn):
    query=(select(table.c.passenger_count, extract("year", table.c.tpep_pickup_datetime),func.round(table.c.trip_distance),func.count()) #Выбрать passenger_count, year из pickup_datetime, округлённое trip_distance, count(*)
             .select_from(table) #Из tiny_table
             .group_by(table.c.passenger_count, extract("year", table.c.tpep_pickup_datetime),func.round(table.c.trip_distance)) #Сгруппировать по 1 и 2
             .order_by(extract("year", table.c.tpep_pickup_datetime),func.count())) #Упорядочить по году и count(*)

    start=time.time() #Засекаем время
    conn.execute(query)
    total_time[3]+=time.time()-start

def check_time(mode,n,table,conn):
    total_time = [0] * 4 #Время выполнения каждого из 4 запросов
    for i in range(n):
        first_query(table,total_time,conn)
        second_query(table,total_time,conn)
        third_query(table,total_time,conn)
        fourth_query(table,total_time,conn)
    print(f"[SQLAlchemy]  Average working time of first query on {mode} dataset is {(total_time[0] / n):.03f}s\n"
          f"[SQLAlchemy]  Average working time of second query on {mode} dataset  is {(total_time[1] / n):.03f}s\n"
          f"[SQLAlchemy]  Average working time of third query on {mode} dataset  is {(total_time[2] / n):.03f}s\n"
          f"[SQLAlchemy]  Average working time of fourth query on {mode} dataset is {(total_time[3] / n):.03f}s\n")

def start(config):
    try:
        EngineStr = ("postgresql://" + config.DBInfo["user"] + ":" + config.DBInfo['password'] + "@"
                     + config.DBInfo['host'] + ':' + config.DBInfo["port"] + '/postgres')
        engine = create_engine(url=EngineStr) #Создаём движок на базе постгресовской БД

        with engine.connect() as conn:
            check_tiny= engine.dialect.has_table(conn,"table_1") #Проверяем таблицы на наличие их в БД
            check_big= engine.dialect.has_table(conn,"table_2")
            if not(check_big and check_tiny):
                create_table(conn,config.CSV_Folder)

            metadata = MetaData()  # Сохраняем метадату для создания объектов таблиц
            tiny_table=Table('table_1',metadata,autoload_with=engine)
            big_table=Table("table_2",metadata,autoload_with=engine)
            Num_Of_Tries=config.Num_Of_Tests
            check_time("tiny",Num_Of_Tries,tiny_table,conn)
            check_time("big",Num_Of_Tries,big_table,conn)


    except Exception as error:  # При обнаружении ошибки выдаём ошибку
        print("Ошибка при работе с SQLAlchemy:", error)
    finally:
        print("Работа с SQLAlchemy завершена\n")
