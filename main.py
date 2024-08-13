#  ---------------------------------------------------------------------------
# Установим необходимые Packages для Python
# Для этого в теринале выполним команду(ы):
# pip install pandas
# pip install sqlalchemy
# pip install XlsxWriter
#  ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
#                       импортируем необходимые библиотеки
# ----------------------------------------------------------------------------
import os                           # для раболты с файлами, операционной системой
import datetime                     # для работы с датой и временем
import time
import numpy as np

import pandas as pd

from sqlalchemy import create_engine    # Импортируем sqlalchemy из библиотеки create_engine
# ----------------------------------------------------------------------------


# ----------------------------------------------------------------------------
#                                   Мои функции
# ----------------------------------------------------------------------------


#  ----------------------- Сортировка таблицы --------------------------------
def table_sort (source_file, return_file):
    df = pd.read_excel(source_file, sheet_name='Domain',  parse_dates=["Время"], index_col=None, date_format='%D:%M:%Y')

    if df.shape[0] < 1:                                         #Если в таблице одна запись ее бработка не целесообразна
        print("В таблице недостаточно запией для обработки")
        exit()

    # добавим значение времени к дате
    #----------------------------------
    row = 0
    while row <int(df.shape[0]):
        data = df['Дата'][row]
        year = datetime.datetime.date(data).year
        month = datetime.datetime.date(data).month
        day = datetime.datetime.date(data).day

        hour = int(df['Время'][row][:2])
        minute = int(df['Время'][row][3:5])
        second = int(df['Время'][row][6:])

        df.iloc[row, 1] = datetime.datetime(year=year, month=month, day=day, hour=hour, minute=minute, second=second)

        row +=1
    #-------------------------------------

    df.sort_values(by=['USER_ID', 'Дата'], inplace=True)            # отсортируем значения по пользователю и дате


    df.reset_index(drop=True, inplace=True)                         # сбросим индексы


    df.drop(labels='Время', axis=1, inplace=True)                   # теперь поле Время нам не нужно, его можно удалить

    # сохраним в временный exel (tmp.xlsx)
    try:
        with pd.ExcelWriter(return_file, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='Domain')
    except:
        print("table_sort: Файл '", tmp_file_xlsx, "' открыт. Закройте файл и перезапустите скрипт")
        exit()

    return (df) # вернем датафрейм
# -----------------------------------------------------------------------------

# ----------------------- Обработка таблицы -------------------------------------
def processing_temp_table(df):
    #df = pd.read_excel(tmp_file_xlsx, sheet_name='Domain', index_col=0)

    last_U_ID       = ''
    last_U_status   = ''
    logon           = 'An account was successfully logged on.'
    logof           = 'An account was logged off.'

    #Дата Время Статус USER_ID

    # ---------------- логика обработки таблицы ----------------------------------------------------------------------------------------
    # если подключений много, то оставляем первое подключение, считая, что отключений между подключениями не было
    # если откючений много, то оставляем последнееотключение, считая, что подключений между отключениями не было и это была одна сессия
    # ----------------------------------------------------------------
    # просматриваем все записи таблицы.
    # если изменяется USER_ID то переходим на следующую строку таблицы
    # если USER_ID не изменяется проверяем Статус
    # если Статус повторяется оставляем первый logon и последний logf
    # ----------------------------------------------------------------------------------------------------------------------------------

    l_UID = df.USER_ID.iloc[0]                          # сохраним последенее состояние поля USER_ID
    l_status = df.Статус.iloc[0]                        # сохраним последенее состояние поля Статус
    l_date = datetime.datetime.date(df.Дата.iloc[0])    # сохраним текущую Дату

    # ----------------------------------------------------------------------#
    # В задании не указана максимальная длительность сессии,                #
    # поэтому будем считать, что в 23:59:59 она автоматически завершается.  #
    # С учетом этого, время начала новой сессии   #
    # ----------------------------------------------------------------------#

    row = 1
    while row < int(df.shape[0]):                                           # выполняем пока количество раз, сколько строк в датафрейме
        c_UID = df.USER_ID.iloc[row]                                        # записываем текущий USER_ID
        c_status = df.Статус.iloc[row]                                      # записываем текущий Статус подключения
        c_date = datetime.datetime.date(df.Дата.iloc[0])                    # записываем ттекущую Дату

        if  c_UID == l_UID and c_status == l_status and l_status == logof:  # если пользователь тоже и статус logof
            df.drop(labels=int(df.iloc[row - 1].name), inplace= True)       # удалим предыдущую запись (оставим последнее отключение)
            row -= 1                                                        # чтобы остаться на месте
        if  c_UID == l_UID and c_status == l_status and l_status == logon:  # если пользователь тоже и статус logon
            if c_date == l_date:
                df.drop(labels=int(df.iloc[row].name), inplace= True)       # удалим текущую запись (оставим первое  последнее отключение  этот день
            else:
                df.drop(labels=int(df.iloc[row-1].name),inplace=True)       # удалим nтекущую запись (оставим первое  последнее отключение
            row -= 1                                                        # чтобы остаться на месте
        l_UID = df.USER_ID.iloc[row]                                        # сохраним последенее состояние поля USER_ID
        l_status = df.Статус.iloc[row]                                      # сохраним последенее состояние поля Статус
        l_date = datetime.datetime.date(df.Дата.iloc[row])                  # сохраним последенее состояние последнюю дату из поля Дата
        row += 1                                                            # прейдем на следующую строку таблицы

    if df.Статус.iloc[0] == logof:                                          # если Статус первой записи logof то удалим ее
        df.drop(labels=int(df.iloc[0].name), inplace=True)

    if df.Статус.iloc[df.shape[0]-1] == logon:                              # если Статус  записи logon то удалим ее
        df.drop(labels=int(df.iloc[df.shape[0]-1].name), inplace=True)

    try:
        with pd.ExcelWriter(tmp_file_xlsx, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='Domain')
    except:
        print("processing_temp_table: Файл'", tmp_file_xlsx, "' открыт. Закройте файл и перезапустите скрипт")
        #exit()


    return (df)
# ----------------------------------------------------------------------------


# ----------------------------------------------------------------------------

def create_data_base (data_base_name, df):

    df.rename(columns={'Дата':'date','Статус':'status','USER_ID':'user_id'}, inplace=True)           # переименуем столбцы
    df.drop(labels = ['Unnamed: 0'], axis = 1, inplace=True)                # удалим поле, где были индексы

    # поработаем с Юзерами
    users = df.copy()                                                       # скопируем датафрейм
    users.drop(labels=['date','status'], axis = 1, inplace=True)            # удалим лишние поля
    users = users.drop_duplicates()                                         # удалим "дубликаты пользователей"
    users['begin_time'] = datetime.time(hour=8, minute=30)                  # добавим колонку начала рабочего временипо умолчанию 08:30
    users['end_time']   = datetime.time(hour=17, minute=30)                 # добавим колонку конца рабочего времени по умолчанию 17:30

    # поработаем с Логами
    logs = df.copy()                                                        # скопируем датафрейм
    logs.rename(columns={'user_id': 'id_user'}, inplace=True)               # переименуем столбец USER_ID на id_user (для удобства)

    # поработаем с Сессиями
    sessions = df.copy()                                                    # скопируем датафрейм
    sessions.reset_index(drop=True, inplace=True)                           # сбросим индексы


    log_on = 'An account was successfully logged on.'
    log_of = 'An account was logged off.'

    sessions.rename(columns={'user_id':'id_user'}, inplace=True)           # переименуем столбец для удобства, чтобы не путать

    # Добавим столбцы начала и конца сессии (соответственно start_session и stop_session)
    sessions['start_session'] = sessions[sessions['status'] == log_on]['date']
    sessions['stop_session'] = sessions[sessions['status'] == log_of]['date']

    ts = sessions['stop_session']
    ts.drop(labels=[0], inplace=True)           # удалим первую строку, чтобы поднять все следующие записи на 1 вверх
    ts.reset_index(drop=True, inplace=True)     # сбросим индексы
    sessions['stop_session'] = ts               # скопируем данные обратно в stop_session

    sessions.drop(labels=['date','status'], axis=1, inplace=True)   # удалим столбец Дата

    sessions = sessions.dropna()                                    # удалим все записи, где есть пусты значения

    # Экспорт в БД---------------------
    engine = create_engine('sqlite:///'+data_base_name,  echo = False)   # соединение с базой данных SQL
    sqlite_connection = engine.connect()
    try:
        users.to_sql('users', sqlite_connection, if_exists='replace', index = False)
        logs.to_sql('logs', sqlite_connection, if_exists='replace', index=False)
        sessions.to_sql('sessions', sqlite_connection, if_exists='replace', index=False)
    except:
        print()

    sqlite_connection.close()
    # ----------------------------------

# ------------------------------тело скрипта ---------------------------------

source_file = os.getcwd()+"\\ParseCountTime.xlsm"   # путь к исходному файлу  ParseCountTime.xlsm
tmp_file_xlsx = os.getcwd()+"\\tmp.xlsx"            # путь к временному файлу tmp.xlsx
data_base_name = os.getcwd()+"\\database.db"           # путь к БД database.db

tmp_df = table_sort(source_file, tmp_file_xlsx)

tmp_df = processing_temp_table(tmp_df)

#create_data_base (data_base_name, tmp_file_xlsx)
create_data_base (data_base_name, tmp_df)

# ----------------------------------------------------------------------------
