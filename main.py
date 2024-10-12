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

# SQL-запрос для получения данных из таблиц MTR, GOST и ED_IZM
query_mtr = "SELECT * FROM MTR"
query_gost = "SELECT [GOST#Gost_code], [GOST#Gost_title], [GOST#Gost_annotation] FROM GOST"
query_ed_izm = "SELECT [Код ЕИ], [Наименование], [Краткое] FROM ED_IZM"

# Чтение данных из таблиц MTR, GOST и ED_IZM
df_mtr = pd.read_sql(query_mtr, conn)
df_gost = pd.read_sql(query_gost, conn)
df_ed_izm = pd.read_sql(query_ed_izm, conn)

# Добавляем пустые столбцы для данных из таблиц GOST и ED_IZM
df_mtr['GOST_Title'] = None
df_mtr['GOST_Annotation'] = None
df_mtr['ED_IZM_Name'] = None
df_mtr['ED_IZM_Short'] = None

# Получаем список колонок из исходной таблицы MTR + новые колонки для GOST и ED_IZM
columns = df_mtr.columns.tolist()

# Функция для создания таблицы только если она не существует
def create_table_if_not_exists(table_name):
    try:
        # Пробуем выполнить запрос к таблице
        cursor.execute(f"SELECT 1 FROM {table_name} WHERE 1=0")
    except pyodbc.ProgrammingError:
        # Если возникла ошибка, создаём таблицу
        cursor.execute(f'''
            CREATE TABLE {table_name} (
                [ID] AUTOINCREMENT PRIMARY KEY,
                {', '.join([f"[{col}] TEXT" for col in columns])}
            )
        ''')
        conn.commit()
        print(f"Таблица {table_name} успешно создана.")
    else:
        print(f"Таблица {table_name} уже существует, данные будут добавляться.")

# Проверяем и создаем таблицы только если они не существуют
create_table_if_not_exists('filled_table')
create_table_if_not_exists('empty_table')

# Функция для поиска информации в таблице GOST
def find_gost_info(gost_code, df_gost):
    # Ищем соответствие по столбцу GOST#Gost_code
    matched_row = df_gost[df_gost['GOST#Gost_code'] == gost_code]
    if not matched_row.empty:
        return matched_row.iloc[0]['GOST#Gost_title'], matched_row.iloc[0]['GOST#Gost_annotation']
    else:
        return None, None

# Функция для поиска информации в таблице ED_IZM
def find_ed_izm_info(ed_izm_code, df_ed_izm):
    # Ищем соответствие по столбцу Код ЕИ
    matched_row = df_ed_izm[df_ed_izm['Код ЕИ'] == ed_izm_code]
    if not matched_row.empty:
        return matched_row.iloc[0]['Наименование'], matched_row.iloc[0]['Краткое']
    else:
        return None, None

# Разделение на две таблицы
total_rows = len(df_mtr)

# Используем tqdm для отображения прогресса
progress_bar = tqdm(total=total_rows, desc="Обработка строк", ncols=100)

# Обрабатываем строки с отслеживанием прогресса
for i in range(total_rows):
    row = df_mtr.iloc[i]

    # Проверяем, заполнены ли колонки "Регламенты (ГОСТ/ТУ)" и "Параметры"
    if pd.notna(row['Регламенты (ГОСТ/ТУ)']) and pd.notna(row['Параметры']):
        # Ищем информацию в таблице GOST
        gost_code = row['Регламенты (ГОСТ/ТУ)']
        gost_title, gost_annotation = find_gost_info(gost_code, df_gost)

        # Если найдено соответствие, добавляем значения в соответствующие столбцы
        if gost_title is not None and gost_annotation is not None:
            df_mtr.at[i, 'GOST_Title'] = gost_title
            df_mtr.at[i, 'GOST_Annotation'] = gost_annotation
            # Выводим информацию динамически в одну строку
            progress_bar.set_postfix_str(f"Найдено ГОСТ: {gost_code} -> {gost_title}, {gost_annotation}")
        else:
            # Выводим информацию, если соответствие не найдено
            progress_bar.set_postfix_str(f"Не найдено ГОСТ: {gost_code}")

        # Ищем информацию в таблице ED_IZM
        ed_izm_code = row['Базисная Единица измерения']
        ed_izm_name, ed_izm_short = find_ed_izm_info(ed_izm_code, df_ed_izm)

        # Если найдено соответствие, добавляем значения в соответствующие столбцы
        if ed_izm_name is not None and ed_izm_short is not None:
            df_mtr.at[i, 'ED_IZM_Name'] = ed_izm_name
            df_mtr.at[i, 'ED_IZM_Short'] = ed_izm_short
            # Выводим информацию динамически в одну строку
            progress_bar.set_postfix_str(f"Найдено ЕИ: {ed_izm_code} -> {ed_izm_name}, {ed_izm_short}")
        else:
            # Выводим информацию, если соответствие не найдено
            progress_bar.set_postfix_str(f"Не найдено ЕИ: {ed_izm_code}")

        # Вставляем строку в таблицу filled_table
        placeholders = ', '.join(['?' for _ in range(len(columns))])
        values = tuple(df_mtr.iloc[i][col] for col in columns)  # Собираем значения
        cursor.execute(f'''
            INSERT INTO filled_table ({', '.join([f"[{col}]" for col in columns])})
            VALUES ({placeholders})
        ''', values)
    else:
        # Ищем информацию в таблице ED_IZM даже для пустых значений ГОСТа
        ed_izm_code = row['Базисная Единица измерения']
        ed_izm_name, ed_izm_short = find_ed_izm_info(ed_izm_code, df_ed_izm)

        # Если найдены данные о ЕИ, добавляем их
        if ed_izm_name is not None and ed_izm_short is not None:
            df_mtr.at[i, 'ED_IZM_Name'] = ed_izm_name
            df_mtr.at[i, 'ED_IZM_Short'] = ed_izm_short

        # Вставляем строку в таблицу empty_table
        placeholders = ', '.join(['?' for _ in range(len(columns))])
        values = tuple(df_mtr.iloc[i][col] for col in columns)  # Собираем значения
        cursor.execute(f'''
            INSERT INTO empty_table ({', '.join([f"[{col}]" for col in columns])})
            VALUES ({placeholders})
        ''', values)

    # Обновляем прогресс
    progress_bar.update(1)

    # Сохраняем изменения после каждой вставки
    conn.commit()

# Закрываем прогресс-бар
progress_bar.close()

# Закрываем соединение с базой данных
cursor.close()
conn.close()

print(f"Таблицы успешно созданы и заполнены в базе данных: {database_path}")
