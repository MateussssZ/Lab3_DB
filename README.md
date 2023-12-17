# Привет!
Это бенчмарк 4 библиотек:
- **PostgreSQL**
- **SQLite**
- **ВuckDB**
- **Pandas**
------
# Для запуска понадобятся cледующие пакеты(pip install)
            psycopg2
            sqlite3
            duckdb
            pandas
            sqlalchemy
----------------------------------
# Кроме того
- Python
- Датасеты, находящиеся по ссылке https://drive.google.com/drive/folders/1usY-4CxLIz_8izBB9uAbg-JQEKSkPMg6
- Все файлы с расширением .py из этого репозитория(SQLite.py Postgres.py Pandas.py DuckDB.py)
----------------------------------
# Основные моменты, необходимые для запуска:
- CSV-файлы должны находиться по пути ./DBases/<file>.csv от рабочей директории.
- SQLite будет жаловаться на датасеты, говоря, что в них есть повторяющиеся столбцы. Для этого придётся убрать(или редактировать название) последний столбец Airport_fee
- Для Postgres.py понадобится localhost с настройками user="postgres",password="password",host="127.0.0.1",port="5432"
