import time
import psycopg2
from psycopg2 import Error
from sqlalchemy import create_engine
import pandas as pd

def create_table():
    engine = create_engine('postgresql://postgres:password@localhost:5432/postgres') #Создаём движок и читаем csv файл при помощи пандаса
    csv_table = pd.read_csv(r"./DBases/nyc_yellow_tiny.csv")
    csv_table.to_sql('table_1',engine,if_exists='replace',index=False) #Делаем из него таблицу
    #Так как pandas некоторые столбцы с датами конвертирует в text, конвертируем их обратно в timestamp для
    #того, чтобы правильно писать запросы
    cursor.execute("ALTER TABLE table_1 ALTER COLUMN tpep_dropoff_datetime "
                "TYPE timestamp without time zone USING tpep_dropoff_datetime::timestamp without time zone")
    cursor.execute("ALTER TABLE table_1 ALTER COLUMN tpep_pickup_datetime "
                "TYPE timestamp without time zone USING tpep_pickup_datetime::timestamp without time zone")
    #Сохраняем наши изменения методом commit
    connection.commit()

def first_query():
    start = time.time()
    cursor.execute('SELECT "VendorID",count(*) '
                   'FROM table_1 GROUP BY 1')
    end=time.time()
    total_time[0] += end - start

def second_query():
    start=time.time()
    cursor.execute("SELECT passenger_count,avg(total_amount) "
                   "FROM table_1 GROUP BY 1")
    end=time.time()
    total_time[1] += end - start

def third_query():
    start=time.time()
    cursor.execute("SELECT passenger_count,extract(year from tpep_pickup_datetime),count(*) "
                   "FROM table_1 "
                   "GROUP BY 1,2")
    end=time.time()
    total_time[2] += end - start

def fourth_query():
    start=time.time()
    cursor.execute("select passenger_count,extract(year from tpep_pickup_datetime),round(trip_distance),count(*) "
                   "from table_1 "
                   "group by 1,2,3 "
                   "order by 2,4 desc")
    end=time.time()
    total_time[3]+=end-start

try:
    connection = psycopg2.connect(user="postgres",password="password",host="127.0.0.1",port="5432") #Устанавливаем соединение с сервером ПСГ
    cursor=connection.cursor()
    cursor.execute("SELECT EXISTS(SELECT * FROM information_schema.tables where table_name='table_1')") #Проверяем, существует ли нужная нам таблица
    if not(cursor.fetchone()[0]): #Если не существует, то создаём эту таблицу
        create_table()
    #print(connection.get_dsn_parameters(),"\n")
    total_time = [0]*4
    for i in range(0,15):
        first_query()
        second_query()
        third_query()
        fourth_query()
    print(f"Average working time of first query is {(total_time[0]/15):.03f}s\n"
          f"Average working time of second query is {(total_time[1]/15):.03f}s\n"
          f"Average working time of third query is {(total_time[2]/15):.03f}s\n"
          f"Average working time of fourth query is {(total_time[3]/15):.03f}s\n")
except (Exception,Error) as error: #При обнаружении ошибки выдаём ошибку
    print("Ошибка при работе с PostgreSQL",error)
finally:
    if connection:
        cursor.close()  #Если соединение было успешным, то закрываем курсос и соединение и пишем, что всё закрыто
        connection.close()
        print("Соединение с PostgreSQL закрыто")
