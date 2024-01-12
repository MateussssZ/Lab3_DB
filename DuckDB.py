import duckdb
import time

class PandasBench():
    Num_Of_Tests=0
    CSV_Folder=''

    def __init__(self,Num_Of_Tests,CSV_Folder):
        self.Num_Of_Tests=Num_Of_Tests
        self.CSV_Folder=CSV_Folder

def create_table(LibraryInfo,cursor):
    Big_Create_String="CREATE TABLE IF NOT EXISTS big_taxi AS SELECT * FROM read_csv_auto('"+LibraryInfo+"/nyc_yellow_big.csv')"
    Tiny_Create_String="CREATE TABLE IF NOT EXISTS big_taxi AS SELECT * FROM read_csv_auto('"+LibraryInfo+"/nyc_yellow_tiny.csv')"
    cursor.sql(Big_Create_String)
    cursor.sql(Tiny_Create_String)

def first_query(mode,total_time,cursor):
    if (mode == 'tiny'): #В зависимости от mode меняется таблица, для которой делаем запрос
        start = time.time()
        cursor.execute('SELECT "VendorID",count(*) '
                       'FROM tiny_taxi GROUP BY 1')
        end = time.time()
    else:
        start = time.time()
        cursor.execute('SELECT "VendorID",count(*) '
                   'FROM big_taxi GROUP BY 1')
        end = time.time()
    total_time[0] += end - start

def second_query(mode,total_time,cursor):
    if (mode == 'tiny'): #В зависимости от mode меняется таблица, для которой делаем запрос
        start = time.time()
        cursor.execute("SELECT passenger_count,avg(total_amount) "
            "FROM tiny_taxi GROUP BY 1")
        end = time.time()
    else:
        start = time.time()
        cursor.execute("SELECT passenger_count,avg(total_amount) "
                   "FROM big_taxi GROUP BY 1")
        end = time.time()
    total_time[1] += end - start

def third_query(mode,total_time,cursor):
    if (mode == 'tiny'): #В зависимости от mode меняется таблица, для которой делаем запрос
        start = time.time()
        cursor.execute("SELECT passenger_count,extract(year from tpep_pickup_datetime),count(*) "
                       "FROM tiny_taxi "
                       "GROUP BY 1,2")
        end = time.time()
    else:
        start = time.time()
        cursor.execute("SELECT passenger_count,extract(year from tpep_pickup_datetime),count(*) "
                   "FROM big_taxi "
                   "GROUP BY 1,2")
        end = time.time()
    total_time[2] += end - start

def fourth_query(mode,total_time,cursor):
    if (mode=='tiny'): #В зависимости от mode меняется таблица, для которой делаем запрос
        start = time.time()
        cursor.execute("select passenger_count,extract(year from tpep_pickup_datetime),round(trip_distance),count(*) "
                       "from tiny_taxi "
                       "group by 1,2,3 "
                       "order by 2,4 desc")
        end = time.time()
    else:
        start = time.time()
        cursor.execute("select passenger_count,extract(year from tpep_pickup_datetime),round(trip_distance),count(*) "
                   "from big_taxi "
                   "group by 1,2,3 "
                   "order by 2,4 desc")
        end = time.time()
    total_time[3] += end - start

def check_time(mode,n,cursor): #mode - tiny или big, n - количество тестов
    total_time=[0]*4 #Время для каждого из 4 запросов
    for i in range(n):
        first_query(mode, total_time,cursor)
        second_query(mode, total_time,cursor)
        third_query(mode, total_time,cursor)
        fourth_query(mode, total_time,cursor)
    print(f"[DuckDB]  Average working time of first query on {mode} dataset is {(total_time[0]/n):.05f}s\n"
               f"[DuckDB]  Average working time of second query on {mode} dataset  is {(total_time[1] / n):.05f}s\n"
               f"[DuckDB]  Average working time of third query on {mode} dataset  is {(total_time[2] / n):.05f}s\n"
               f"[DuckDB]  Average working time of fourth query on {mode} dataset is {(total_time[3] / n):.05f}s\n")

def start(config):
    try:
        cursor=duckdb.connect("duckdb_taxi.db") #Создаём(или подключаемся) к .db файлу
        create_table(config.CSV_Folder,cursor) #Если нужных нам таблиц нет, то создаём их, импортируя данные из .csv файлов

        Num_Of_Tests = config.Num_Of_Tests
        check_time('tiny',Num_Of_Tests,cursor)
        check_time('big',Num_Of_Tests,cursor)
    except Exception as error:
        print("Ошибка при работе с DuckDB:", error)
    finally:
        print("Работа с DuckDB завершена\n")
