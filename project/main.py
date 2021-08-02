# -*- coding: <utf-8> -*-

# TODO: Добавить UI для ввода данных подключения к БД
import psycopg2
from psycopg2 import Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd
import pandas.io.sql as psql
import numpy as np
from project.producers import aircraft_producers as air_prod
import datetime
import multiprocessing as mp


# TODO: Добавить генерацию красивых pdf-документов с отчетностью
def planes_data(planes_dataframe: pd.DataFrame) -> None:
    """
    Анализ данных, связанных с бортами авиакомпании
    :param planes_dataframe: Pandas DataFrame с информацией о бортах
    :return: Ничего не возвращается (Неявный None)
    """

    print(filler('='), '\n', sep='')

    print('ИСПОЛЬЗУЕМЫЙ ФЛОТ АВИАСУДОВ')
    res = [x['en'] for x in planes_dataframe['model']]  # Получение английских наименований судов
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

    print(filler('-'))

    print('Количество используемых самолетов по производителям:')
    for (producer, amount) in aircraft_by_producers.items():
        print(f'\t{producer}: {amount} ед.')

    print('\n', filler('='), '\n', sep='')


# TODO Добавить генерацию красивых pdf-документов с отчетностью
def flights_data(flights_dataframe: pd.DataFrame) -> None:
    """
    Анализ данных, связанных с рейсами бортов авиакомпании
    :param flights_dataframe: Pandas Dataframe с информацией о полетах
    :return: Ничего не возвращается (Неявный None)
    """
    print("ПОЛЕТЫ")
    print(f'Среднее время полета на основе {len(flights_dataframe)} записей:')
    # Получение полных перечней времени взлета и посадки бортов и отсечение нулевого смещения по часовому поясу
    departure_actual = [str(x)[:-6] for x in flights_dataframe[['actual_departure']]['actual_departure']]
    arrival_actual = [str(x)[:-6] for x in flights_dataframe[['actual_arrival']]['actual_arrival']]
    departure_planned = [str(x)[:-6] for x in flights_dataframe[['scheduled_departure']]['scheduled_departure']]

    with mp.Pool(4) as pool:
        flight_time = pool.starmap(mp_avg_flight_time, zip(arrival_actual, departure_actual))
        avg_flight_time = datetime.timedelta(seconds=np.average(flight_time))
        print(str(avg_flight_time).split('.')[0])

        print(filler('-'))

        delay_time = pool.starmap(mp_delay_time, zip(departure_planned, departure_actual))
        in_time = 0  # Счетчик своевременных вылетов
        delayed = []  # Список для хранения времени задержки рейсов с задержанным вылетом
        too_soon = []  # Список для хранения времени преждевременного вылета рейсов с преждевременным вылетом
        for time in delay_time:
            if time == 0:
                in_time += 1
            elif time > 0:
                delayed.append(time)
            else:
                too_soon.append(time)

    avg_delay_time = str(datetime.timedelta(seconds=np.average(delayed))).split('.')[0]
    flights_dataframe['delay'] = delay_time

    res = f'ИЗ {len(departure_actual)} совершенных рейсов\n' \
          f'\tВовремя вылетели: {in_time}'
    if in_time > 0:
        in_time_perc = "{:.3%}".format(in_time / len(departure_actual))
        res += f' ({in_time_perc})'
    res += f'\n\tОпоздали с вылетом: {len(delayed)}'
    if len(delayed) > 0:
        delayed_perc = "{:.3%}".format(len(delayed) / len(departure_actual))
        res += f' ({delayed_perc}). ' \
               f'При этом среднее время задержки равно: {avg_delay_time}'
    res += f'\n\tВылетели с опережением графика: {len(too_soon)}'
    if len(too_soon) > 0:
        too_soon_perc = "{:.3%}".format(len(too_soon) / len(departure_actual))
        res += f' ({too_soon_perc}.'
    print(res)
    print(filler('-'))
    df_with_seconds = flights_dataframe.drop(
        ['scheduled_departure', 'scheduled_arrival', 'actual_departure', 'actual_arrival',
         'flight_id', 'aircraft_code', 'status'], axis=1)

    if in_time > 0:
        # Датафрейм со своевременными рейсами
        df_in_time = df_with_seconds.loc[df_with_seconds['delay'] == 0].drop(['delay'], axis=1)
        df = df_in_time.flight_no.value_counts().to_frame()
        maxes = df.index[df['flight_no'] == df['flight_no'].max()].tolist()
        df_in_time = df_in_time.drop_duplicates(keep='first')
        if len(maxes) > 2:
            data = df_in_time.loc[df_in_time['flight_no'].isin(maxes)]
            deps = data['departure_airport'].tolist()
            arrs = data['arrival_airport'].tolist()
            to_print = [board + ' из ' + dep + ' в ' + arr for board, dep, arr in zip(maxes, deps, arrs)]
            print('Наиболее часто вылетающие вовремя рейсы:')
            for element in to_print:
                print(f'\t {element}')
        else:
            data = df_in_time.loc[df_in_time['flight_no'] == maxes[0]]
            dep = data['departure_airport'].item()
            arr = data['arrival_airport'].item()
            print(f'Наиболее часто вылетающий вовремя рейс:\n\t{maxes[0]} из {dep} в {arr}')

    if len(delayed) > 0:
        # Датафрейм с рейсами, вылетевшими с опозданием
        df_delayed = df_with_seconds.loc[df_with_seconds['delay'] > 0].drop(['delay'], axis=1)
        df = df_delayed.flight_no.value_counts().to_frame()
        maxes = df.index[df['flight_no'] == df['flight_no'].max()].tolist()
        df_delayed = df_delayed.drop_duplicates(keep='first')
        if len(maxes) > 2:
            data = df_delayed.loc[df_delayed['flight_no'].isin(maxes)]
            boards = data['flight_no'].tolist()
            deps = data['departure_airport'].tolist()
            arrs = data['arrival_airport'].tolist()
            to_print = [board + ' из ' + dep + ' в ' + arr for board, dep, arr in zip(boards, deps, arrs)]
            print('Наиболее часто вылетающие с задержкой рейсы:')
            for element in to_print:
                print(f'\t {element}')
        else:
            data = df_delayed.loc[df_delayed['flight_no'] == maxes[0]]
            dep = data['departure_airport'].item()
            arr = data['arrival_airport'].item()
            print(f'\tНаиболее часто вылетающий с задержкой рейс:\n\t{maxes[0]} из {dep} в {arr}')

    if len(too_soon) > 0:
        # Датафрейм с рейсами, вылетевшими раньше плана
        df_soon = df_with_seconds.loc[df_with_seconds['delay'] < 0].drop(['delay'], axis=1)
        df = df_soon.flight_no.value_counts().to_frame()
        maxes = df.index[df['flight_no'] == df['flight_no'].max()].tolist()
        df_soon = df_soon.drop_duplicates(keep='first')
        if len(maxes) > 2:
            data = df_soon.loc[df_soon['flight_no'].isin(maxes)]
            boards = data['flight_no'].tolist()
            deps = data['departure_airport'].tolist()
            arrs = data['arrival_airport'].tolist()
            to_print = [board + ' из ' + dep + ' в ' + arr for board, dep, arr in zip(boards, deps, arrs)]
            print('Наиболее часто вылетающие с задержкой рейсы:')
            for element in to_print:
                print(f'\t {element}')
        else:
            data = df_soon.loc[df_soon['flight_no'] == maxes[0]]
            dep = data['departure_airport'].item()
            arr = data['arrival_airport'].item()
            print(f'\tНаиболее часто вылетающий с задержкой рейс:\n\t{maxes[0]} из {dep} в {arr}')

    print('\n', filler('='), '\n', sep='')


def mp_avg_flight_time(arr: str, dep: str) -> float:
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


def mp_delay_time(dep_plan: str, dep_act: str) -> float:
    """
    Вспомогательный метод для расчета разницы в запланированном и реальном времени вылета борта
    :param dep_plan: Запланированное время вылета борта
    :param dep_act: Реальное время вылета борта
    :return: Разность между запланированным и реальным временем вылета в секундах
    """
    dep_plan, dep_act = datetime.datetime.strptime(dep_plan, '%Y-%m-%d %H:%M:%S'), \
        datetime.datetime.strptime(dep_act, '%Y-%m-%d %H:%M:%S')
    if dep_act >= dep_plan:
        diff = (dep_act - dep_plan).total_seconds()
    else:
        diff = - (dep_plan - dep_act).total_seconds()
    return diff


def filler(symbol: str):
    """
    Функция для графического отделения данных в консольном отображении
    :param symbol: Символ для повторения в консоли
    :return: Строка - заполнитель
    """
    return symbol * 50


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
        dataframe = psql.read_sql("SELECT * FROM flights WHERE status='Arrived'", connection)
        flights_data(dataframe)

    except (Exception, Error) as error:
        print('Ошибка при работе с базой данных:\n\t', error)

    finally:
        if connection:
            cursor.close()
            connection.close()
            print('Подключение к базе данных закрыто')
        print('Процесс выполнен за', datetime.datetime.now() - start_time)
