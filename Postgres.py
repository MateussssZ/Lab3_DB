import time
import psycopg2
from psycopg2 import Error
from sqlalchemy import create_engine
import pandas as pd

class PostgresBench(): #Класс для удобного запуска и работы с кфг
    DBInfo={} #Инфа для подключения к БД постгреса
    Num_Of_Tests=0 #Количество тестов
    CSV_Folder='' #Путь к папке с CSV файлами

    def __init__(self,DBInfo,Num_Of_Tests,CSV_Folder): #Инициализация
        self.Num_Of_Tests=Num_Of_Tests
        self.DBInfo=DBInfo
        self.CSV_Folder=CSV_Folder

def create_table(PostgresInfo,CSVFolder,cursor,connection):
    EngineStr=("postgresql://"+PostgresInfo["user"]+":"+PostgresInfo['password']+"@"
               +PostgresInfo['host']+':'+PostgresInfo["port"]+'/postgres') #В итоге выйдет что-то вроде 'postgresql://postgres:password@localhost:5432/postgres'
    engine = create_engine(EngineStr) #Создаём движок и читаем csv файл при помощи пандаса

    csv_table = pd.read_csv(CSVFolder+r"/nyc_yellow_tiny.csv")
    csv_table.to_sql('table_1',engine,if_exists='replace',index=False) #Делаем из него таблицу
    #Так как pandas некоторые столбцы с датами конвертирует в text, конвертируем их обратно в timestamp для
    #того, чтобы правильно писать запросы
    cursor.execute("ALTER TABLE table_1 ALTER COLUMN tpep_dropoff_datetime "
                "TYPE timestamp without time zone USING tpep_dropoff_datetime::timestamp without time zone")
    cursor.execute("ALTER TABLE table_1 ALTER COLUMN tpep_pickup_datetime "
                "TYPE timestamp without time zone USING tpep_pickup_datetime::timestamp without time zone")

    csv_table=pd.read_csv(CSVFolder+r"/nyc_yellow_big-001.csv",chunksize=6500000)
    for chunk in csv_table:
        chunk.to_sql('table_2',engine,if_exists='append',index=False,chunksize=10000)
    cursor.execute("ALTER TABLE table_2 ALTER COLUMN tpep_dropoff_datetime "
                   "TYPE timestamp without time zone USING tpep_dropoff_datetime::timestamp without time zone")
    cursor.execute("ALTER TABLE table_2 ALTER COLUMN tpep_pickup_datetime "
                   "TYPE timestamp without time zone USING tpep_pickup_datetime::timestamp without time zone")

    connection.commit() #Сохраняем изменения методом commit

def first_query(mode,total_time,cursor):
    if (mode=="tiny"):
        start = time.time()
        cursor.execute('SELECT "VendorID",count(*) '
                       'FROM table_1 GROUP BY 1')
        end=time.time()
        total_time[0] += end - start
    else:
        start = time.time()
        cursor.execute('SELECT "VendorID",count(*) '
                       'FROM table_2 GROUP BY 1')
        end = time.time()
        total_time[0] += end - start

def second_query(mode,total_time,cursor):
    if (mode=="tiny"):
        start=time.time()
        cursor.execute("SELECT passenger_count,avg(total_amount) "
                       "FROM table_1 GROUP BY 1")
        end=time.time()
        total_time[1] += end - start
    else:
        start = time.time()
        cursor.execute("SELECT passenger_count,avg(total_amount) "
                       "FROM table_2 GROUP BY 1")
        end = time.time()
        total_time[1] += end - start

def third_query(mode,total_time,cursor):
    if (mode=="tiny"):
        start=time.time()
        cursor.execute("SELECT passenger_count,extract(year from tpep_pickup_datetime),count(*) "
                       "FROM table_1 "
                       "GROUP BY 1,2")
        end=time.time()
        total_time[2] += end - start
    else:
        start = time.time()
        cursor.execute("SELECT passenger_count,extract(year from tpep_pickup_datetime),count(*) "
                       "FROM table_2 "
                       "GROUP BY 1,2")
        end = time.time()
        total_time[2] += end - start

def fourth_query(mode,total_time,cursor):
    if (mode=="tiny"):
        start=time.time()
        cursor.execute("select passenger_count,extract(year from tpep_pickup_datetime),round(trip_distance),count(*) "
                       "from table_1 "
                       "group by 1,2,3 "
                       "order by 2,4 desc")
        end=time.time()
        total_time[3]+=end-start
    else:
        start = time.time()
        cursor.execute("select passenger_count,extract(year from tpep_pickup_datetime),round(trip_distance),count(*) "
                       "from table_2 "
                       "group by 1,2,3 "
                       "order by 2,4 desc")
        end = time.time()
        total_time[3] += end - start

def check_time(mode,number_of_tries,cursor):
    total_time=[0]*4
    for i in range(number_of_tries):
        first_query(mode,total_time,cursor)
        second_query(mode,total_time,cursor)
        third_query(mode,total_time,cursor)
        fourth_query(mode,total_time,cursor)
    print(f"[psycopg2]  Average working time of first query on {mode} data is {(total_time[0] / number_of_tries):.03f}s\n"
          f"[psycopg2]  Average working time of second query on {mode} data is {(total_time[1] / number_of_tries):.03f}s\n"
          f"[psycopg2]  Average working time of third query on {mode} data is {(total_time[2] / number_of_tries):.03f}s\n"
          f"[psycopg2]  Average working time of fourth query on {mode} data is {(total_time[3] / number_of_tries):.03f}s\n")


def start(config):
    try:
        # with open("config.json", 'r+') as config_json:
        #     config = json.load(config_json)
        #
        connection = psycopg2.connect(user=config.DBInfo["user"],password=config.DBInfo["password"],host=config.DBInfo["host"],port=config.DBInfo["port"]) #Устанавливаем соединение с сервером ПСГ
        cursor=connection.cursor()

        cursor.execute("SELECT EXISTS(SELECT * FROM information_schema.tables where table_name='table_1')") #Проверяем, существует ли нужная нам таблица
        small_check = cursor.fetchone()[0]
        cursor.execute("SELECT EXISTS(SELECT * FROM information_schema.tables where table_name='table_2')")
        big_check = cursor.fetchone()[0]
        if not(small_check and big_check): #Если не существует, то создаём эти таблицы
            create_table(config.DBInfo,config.CSV_Folder,cursor,connection)

        Num_Of_Tests=config.Num_Of_Tests
        check_time("tiny",Num_Of_Tests,cursor)
        check_time('big',Num_Of_Tests,cursor)
    except (Exception,Error) as error: #При обнаружении ошибки выдаём ошибку
        print("Ошибка при работе с PostgreSQL",error)
    finally:
        if connection:
            cursor.close()  #Если соединение было успешным, то закрываем курсос и соединение и пишем, что всё закрыто
            connection.close()
            print("Соединение с PostgreSQL закрыто\n")
