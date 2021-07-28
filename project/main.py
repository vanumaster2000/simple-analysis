# -*- coding: <utf-8> -*-

# TODO Добавить UI для ввода данных подключения к БД
import psycopg2
from psycopg2 import Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd
import pandas.io.sql as psql
import numpy as np
from project.producers import aircraft_producers as air_prod
import datetime
import multiprocessing as mp


# TODO Добавить генерацию красивых pdf-документов с отчетностью
def planes_data(dataframe: pd.DataFrame) -> None:
    """
    Анализ данных, связанных с бортами авиакомпании
    :param dataframe: Pandas DataFrame с информацией о бортах
    :return: Ничего не возвращается (Неявный None)
    """
    filler('=')
    print()
    print('ИСПОЛЬЗУЕМЫЙ ФЛОТ АВИАСУДОВ')
    res = [x['en'] for x in dataframe['model']]  # Получение английских наименований авиасудов
    occurrences = {x: res.count(x) for x in list(set(res))}
    types = sorted([x for x in occurrences.keys()])
    print('Модели самолетов, находящихся в использовании:')
    for single in types:
        print('\t', single)
    types = [x.split()[0].lower() for x in types]  # Приведение производителя (первое слово в строке) к нижнему регистру
    aircraft_by_producers = {x.title(): types.count(x) for x in air_prod}
    aircraft_by_producers = [(x, aircraft_by_producers[x]) for x in aircraft_by_producers.keys() if
                             aircraft_by_producers[x] > 0]
    aircraft_by_producers = {x: y for (x, y) in aircraft_by_producers}
    filler('-')
    print('Количество используемых самолетов по производителям:')
    for (producer, amount) in aircraft_by_producers.items():
        print(f'\t{producer}: {amount} ед.')

    print()
    filler('=')
    print()

# TODO Добавить генерацию красивых pdf-документов с отчетностью
def flights_data(dataframe: pd.DataFrame) -> None:
    """
    Анализ данных, связанных с рейсами бортов авиакомпании
    :param dataframe: Pandas Dataframe с информацией о полетах
    :return: Ничего не возвращается (Неявный None)
    """
    print(f'СРЕДНЕЕ ВРЕМЯ ПОЛЕТА НА ОСНОВЕ {len(dataframe)} ЗАПИСЕЙ:')
    # Получение полных перечней времени взлета и посадки бортов
    departure = [str(x)[:-6] for x in dataframe[['actual_departure']]['actual_departure']]
    arrival = [str(x)[:-6] for x in dataframe[['actual_arrival']]['actual_arrival']]

    pool = mp.Pool(4)
    flight_time = pool.starmap(mp_func, zip(arrival, departure))
    avg_flight_time = datetime.timedelta(seconds=np.average(flight_time))
    print(str(avg_flight_time).split('.')[0])

    print()
    filler('=')
    print()


def mp_func(arr: str, dep: str) -> datetime.timedelta.seconds:
    """
    Вспомогательный метод для анализа среднего времени полета
    :param arr: Время вылета борта
    :param dep: Время прилета борта
    :return: Время полета в секундах
    """
    diff = datetime.datetime.strptime(arr, '%Y-%m-%d %H:%M:%S') \
        - datetime.datetime.strptime(dep, '%Y-%m-%d %H:%M:%S')
    diff = diff.total_seconds()
    return diff


def filler(symbol: str):
    """
    Функция для графического отделения данных в консольном отображении
    :param symbol: Символ для повторения в консоли
    :return: Ничего не возвращается (Неявный None)
    """
    print(symbol * 10)


if __name__ == '__main__':
    start_time = datetime.datetime.now()
    mp.freeze_support()
    try:
        connection = psycopg2.connect(
            host='localhost',
            port='5432',
            database='demo',
            user='superUser',
            password='superUserPassword'
        )
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = connection.cursor()

        # Выгрузка данных
        dataframe = psql.read_sql('SELECT * FROM aircrafts_data', connection)
        # Анализ данных самолетов
        planes_data(dataframe)

        # Анализ данных полетов
        dataframe = psql.read_sql('SELECT * FROM flights WHERE actual_arrival IS NOT NULL', connection)
        flights_data(dataframe)

    except (Exception, Error) as error:
        print('Ошибка при работе с базой данных.\n\t', error)

    finally:
        if connection:
            cursor.close()
            connection.close()
            print('Connection closed\n', 'Процесс выполнен за ', datetime.datetime.now() - start_time)
