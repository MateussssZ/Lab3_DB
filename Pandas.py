import pandas as pd
import time

def first_Query(current_df, total_time):
    start=time.time()
    selected_df = current_df[["VendorID"]] #Выбираем нужные нам столбцы
    grouped_df = selected_df.groupby("VendorID") #Группируем их
    final_df = grouped_df.size().reset_index(name='counts') #Находим count(*)
    total_time[0]+=time.time()-start

def second_Query(current_df, total_time):
    start = time.time()
    selected_df = current_df[["passenger_count","total_amount"]] #Выбираем нужные нам столбцы
    grouped_df = selected_df.groupby("passenger_count") #Группируем их
    final_df=grouped_df.mean().reset_index() #Берём avg
    total_time[1] += time.time() - start

def third_Query(current_df, total_time):
    start = time.time()
    selected_df=current_df[["passenger_count","tpep_pickup_datetime"]]#.copy(), чтобы убрать предупреждения из консоли
    selected_df['year']=pd.to_datetime(selected_df.pop('tpep_pickup_datetime'),format='%Y-%m-%d %H:%M:%S').dt.year #Выделяем год
    grouped_df=selected_df.groupby(["passenger_count",'year']) #Группируем по нужным столбцам
    final_df=grouped_df.size().reset_index(name='counts') #count(*)
    total_time[2] += time.time() - start

def fourth_Query(current_df, total_time):
    start = time.time()
    selected_df=current_df[["passenger_count","tpep_pickup_datetime","trip_distance"]]#.copy(), чтобы убрать предупреждения из консоли
    selected_df["trip_distance"]=selected_df["trip_distance"].round().astype(int)
    selected_df["year"]=pd.to_datetime(selected_df.pop('tpep_pickup_datetime'),format='%Y-%m-%d %H:%M:%S').dt.year #Выделяем год
    grouped_df=selected_df.groupby(["passenger_count","year","trip_distance"]) #Группируем по нужным столбцам
    final_df=grouped_df.size().reset_index(name="counts") #count(*)
    final_df=final_df.sort_values(["year","counts"],ascending=[True,False]) #Order by
    total_time[3] += time.time() - start

def check_Time(mode,table_df,n): #mode - tiny или big, table_df - нужный dataframe для работы, n - количество тестов
    total_time=[0]*4 #Время на каждом из 4 запросов
    for i in range(n):
        first_Query(table_df,total_time)
        second_Query(table_df,total_time)
        third_Query(table_df,total_time)
        fourth_Query(table_df,total_time)
    if (mode=='big'):
            big_total_time[0]=total_time[0]
            big_total_time[1]=total_time[1]
            big_total_time[2] = total_time[2]
            big_total_time[3] = total_time[3]
    else:
        print(f"Average working time of first query on {mode} dataset is {(total_time[0] / n):.03f}s\n"
          f"Average working time of second query on {mode} dataset  is {(total_time[1] / n):.03f}s\n"
          f"Average working time of third query on {mode} dataset  is {(total_time[2] / n):.03f}s\n"
          f"Average working time of fourth query on {mode} dataset is {(total_time[3] / n):.03f}s\n")

try:
    pd.options.mode.chained_assignment = None  # default='warn' Убираем лишние warning, чтобы не мазолили глаза
    tiny_table_df = pd.read_csv("./DBases/nyc_yellow_tiny.csv")
    check_Time('tiny',tiny_table_df,15) #Считываем малый датасет и сразу отправляем на тесты
    big_table_df = pd.read_csv("./DBases/nyc_yellow_big-001.csv",chunksize=10000000) #Большой датасет сжирает к **** мои 16 Гб оперативы
    big_total_time=[0]*4
    num_of_tests=10;
    for chunk in big_table_df:  #И крашит процесс, поэтому делим его на 2 примерно равных чанка и замеряем для каждого чанка время по очереди
        check_Time('big', chunk, num_of_tests)
    print(f"Average working time of first query on big dataset is {(big_total_time[0] / num_of_tests):.03f}s\n"
          f"Average working time of second query on big dataset  is {(big_total_time[1] / num_of_tests):.03f}s\n"
          f"Average working time of third query on big dataset  is {(big_total_time[2] / num_of_tests):.03f}s\n"
          f"Average working time of fourth query on big dataset is {(big_total_time[3] / num_of_tests):.03f}s\n")
except Exception as error:  # При обнаружении ошибки выдаём ошибку
    print("Ошибка при работе с Pandas:", error)
finally: #При успешной работе завершаем её
    print("Работа с Pandas завершена")
