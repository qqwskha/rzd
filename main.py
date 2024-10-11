import pandas as pd
import pyodbc
import os
from tqdm import tqdm

# Определим путь к папке программы
current_directory = os.path.dirname(os.path.abspath(__file__))

# Путь к файлу Access (.accdb)
database_path = os.path.join(current_directory, 'MTR.accdb')

# Строка подключения к базе данных Access
conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        r'DBQ=' + database_path + ';'
)

# Подключение к базе данных через pyodbc
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# SQL-запрос для получения данных из исходной таблицы MTR
query = "SELECT * FROM MTR"

# Чтение данных
df = pd.read_sql(query, conn)

# Получаем список колонок из исходной таблицы
columns = df.columns.tolist()

# Создаем строку для создания таблиц с теми же колонками
columns_str = ', '.join([f"[{col}] TEXT" for col in columns])

# Создаем таблицу для заполненных данных (filled_table)
cursor.execute(f'''
    CREATE TABLE filled_table (
        [ID] AUTOINCREMENT PRIMARY KEY,
        {columns_str}
    )
''')

# Создаем таблицу для пустых данных (empty_table)
cursor.execute(f'''
    CREATE TABLE empty_table (
        [ID] AUTOINCREMENT PRIMARY KEY,
        {columns_str}
    )
''')

# Сохраняем изменения
conn.commit()

# Разделение на две таблицы
total_rows = len(df)

# Обрабатываем строки с отслеживанием прогресса
for i in tqdm(range(total_rows), desc="Обработка строк"):
    row = df.iloc[i]

    # Проверяем, заполнены ли колонки "Регламенты (ГОСТ/ТУ)" и "Параметры"
    if pd.notna(row['Регламенты (ГОСТ/ТУ)']) and pd.notna(row['Параметры']):
        # Вставляем строку в таблицу filled_table
        placeholders = ', '.join(['?' for _ in range(len(columns))])
        cursor.execute(f'''
            INSERT INTO filled_table ({', '.join([f"[{col}]" for col in columns])})
            VALUES ({placeholders})
        ''', tuple(row))
    else:
        # Вставляем строку в таблицу empty_table
        placeholders = ', '.join(['?' for _ in range(len(columns))])
        cursor.execute(f'''
            INSERT INTO empty_table ({', '.join([f"[{col}]" for col in columns])})
            VALUES ({placeholders})
        ''', tuple(row))

    # Сохраняем изменения после каждой вставки
    conn.commit()

# Закрываем соединение с базой данных
cursor.close()
conn.close()

print(f"Таблицы успешно созданы и заполнены в базе данных: {database_path}")