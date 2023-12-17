import sqlite3 #Сама библиотека
import pandas as pd #Для превращения csv в dataframe
import time

def create_table():
    tiny_data = pd.read_csv(r"./DBases/nyc_yellow_tiny.csv") #Превращаем csv в dataframe и из него строим нужную нам таблицу на базе данных
    tiny_data.to_sql('tiny_taxi', connection, if_exists='replace', index=False)
    big_data = pd.read_csv(r"./DBases/nyc_yellow_big-001.csv")
    big_data.to_sql('big_taxi', connection, if_exists='replace', index=False,chunksize=1000,method='multi')

def first_query(mode,time_table):
    if (mode=='tiny'): #В зависимости от mode меняется таблица, для которой делаем запрос
        start = time.time()
        cursor.execute('SELECT VendorID,count(*) '
                   'FROM tiny_taxi GROUP BY 1')
        end = time.time()
    else:
        start = time.time()
        cursor.execute('SELECT VendorID,count(*) '
                       'FROM big_taxi GROUP BY 1')
        end = time.time()
    time_table[0] += end-start

def second_query(mode,time_table):
    if (mode=='tiny'): #В зависимости от mode меняется таблица, для которой делаем запрос
        start = time.time()
        cursor.execute("SELECT passenger_count,avg(total_amount) "
                       "FROM tiny_taxi GROUP BY 1")
        end = time.time()
    else:
        start = time.time()
        cursor.execute("SELECT passenger_count,avg(total_amount) "
                       "FROM big_taxi GROUP BY 1")
        end = time.time()
    time_table[1] += end-start

def third_query(mode,time_table):
    if (mode=='tiny'): #В зависимости от mode меняется таблица, для которой делаем запрос
        start = time.time()
        cursor.execute('SELECT passenger_count,strftime("%Y", tpep_pickup_datetime),count(*) '
                       'FROM tiny_taxi '
                       'GROUP BY 1,2')
        end = time.time()
    else:
        start = time.time()
        cursor.execute('SELECT passenger_count,strftime("%Y", tpep_pickup_datetime),count(*) ' #Т.к. в SQLite нет timestamp - выделяем год 
                       'FROM big_taxi '                                                        #отдельными методами
                       'GROUP BY 1,2')
        end = time.time()
    time_table[2] += end-start

def fourth_query(mode,time_table):
    if (mode=='tiny'): #В зависимости от mode меняется таблица, для которой делаем запрос
        start = time.time()
        cursor.execute('select passenger_count,strftime("%Y", tpep_pickup_datetime),round(trip_distance),count(*) ' #Т.к. в SQLite нет timestamp - выделяем
                       'from tiny_taxi '                                                                            #год отдельными методами
                       'group by 1,2,3 '
                       'order by 2,4 desc')
        end = time.time()
    else:
        start = time.time()
        cursor.execute('select passenger_count,strftime("%Y", tpep_pickup_datetime),round(trip_distance),count(*) '
                       'from big_taxi '
                       'group by 1,2,3 '
                       'order by 2,4 desc')
        end = time.time()
    time_table[3]+=end-start

def check_time(mode,n): # mode - big или tiny, n - коилчество тестов
    total_time = [0] * 4 #Время выполнения каждого из 4 запросов
    for i in range(n):
        first_query(mode,total_time)
        second_query(mode,total_time)
        third_query(mode,total_time)
        fourth_query(mode,total_time)
    print(f"Average working time of first query on {mode} dataset is {(total_time[0] / n):.03f}s\n"
          f"Average working time of second query on {mode} dataset  is {(total_time[1] / n):.03f}s\n"
          f"Average working time of third query on {mode} dataset  is {(total_time[2] / n):.03f}s\n"
          f"Average working time of fourth query on {mode} dataset is {(total_time[3] / n):.03f}s\n")

try:
    connection = sqlite3.connect('taxi_sqlite.db') #Коннектимся к .db или создаём её, если её нет
    cursor = connection.cursor() #Создаём курсор для запросов
    cursor.execute(
        "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='tiny_taxi'")
    if_tiny_exists=cursor.fetchone()[0]
    cursor.execute(
        "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='big_taxi'")
    if_big_exists=cursor.fetchone()[0] #Проверяем, существуют ли нужные таблицы и, если хоть одна не существует - пересоздаём их
    if not (if_big_exists and if_tiny_exists):
        create_table()
    check_time('tiny',15)
    check_time('big',10)
    cursor.close()

except Exception as error:  # При обнаружении ошибки выдаём ошибку
    print("Ошибка при работе с SQLite:", error)

finally:
    connection.close()
    print("Соединение с SQLite закрыто")

