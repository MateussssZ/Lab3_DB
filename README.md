# Привет!
Это бенчмарк из 5 библиотек:
- **PostgreSQL**
- **SQLite**
- **ВuckDB**
- **Pandas**
- **SQLAlchemy**
___
### Для запуска понадобится установить следующие пакеты  (pip install `пакет`)
                        psycopg2
                        sqlite3
                        duckdb
                        pandas
                        sqlalchemy
### Кроме того
- Python 3
- Датасеты, находящиеся по [ссылке](https://drive.google.com/drive/folders/1usY-4CxLIz_8izBB9uAbg-JQEKSkPMg6)
- Все файлы с расширением **.py** из этого репозитория, а также конфиг **.json**
___
# Основные моменты, необходимые для запуска:
1. CSV-файлы должны находиться по пути `./DBases/file.csv` от рабочей директории.
2. SQLite будет жаловаться на датасеты, говоря, что в них есть повторяющиеся столбцы. Для этого придётся убрать(или редактировать название) последний столбец Airport_fee
3. Для Postgres и SQLAlchemy понадобится сервер localhost с настройками user="postgres",password="password",host="127.0.0.1",port="5432"
4. При желании можно изменить данные, описанные в пунктах 1 и 3, но тогда нужно **обязательно** внести изменения в ***config.json***
---
<h1 style="text-align: center; font-size: 35px">Отчёт об измерениях</h1>

<h3 style="text-align: center; font-size: 25px">psycopg2</h3>

**Плюсы:**

:white_check_mark: Хороша своей простотой и лаконичностью
:white_check_mark: Библиотека для новичков, позволяющая быстро начать работать с PostgreSQL
