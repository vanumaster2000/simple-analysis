# -*- coding: <utf-8> -*-

import psycopg2
from psycopg2 import Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd
import pandas.io.sql as psql
import numpy as np
from project.producers import aircraft_producers as air_prod
import datetime
import multiprocessing as mp
from project.multiprocessing_functions import mp_avg_flight_time, mp_delay_time


# TODO: Добавить генерацию красивых pdf-документов с отчетностью
def planes_data(planes_dataframe: pd.DataFrame) -> None:
    """
    Анализ данных, связанных с бортами авиакомпании
    :param planes_dataframe: Pandas DataFrame с информацией о бортах
    :raises: TypeError
    :return: Ничего не возвращается (Неявный None)
    """
    if type(planes_dataframe) != pd.DataFrame:
        raise TypeError('Метод предназначен для обработки pandas.Dataframe')
    print(filler('='))
    print('ИСПОЛЬЗУЕМЫЙ ФЛОТ АВИАСУДОВ')
    planes_dataframe = planes_dataframe.drop(['range'], axis=1).assign(Economy=0, Comfort=0, Business=0)
    seats = pd.read_sql("SELECT * FROM seats", connection).drop(['seat_no'], axis=1)  # Перечень мест для каждого борта
    seats_data = seats.groupby('aircraft_code')['fare_conditions'].value_counts()
    for code in planes_dataframe['aircraft_code']:
        data = seats_data[code].reset_index(name='count')
        fares = data['fare_conditions'].to_list()
        for fare in fares:
            planes_dataframe.loc[planes_dataframe['aircraft_code'] == code, fare] = \
                data.loc[data['fare_conditions'] == fare, 'count'].item()
    res = [(x['en'], eco, com, bus) for x, eco, com, bus in
           zip(planes_dataframe['model'],
               planes_dataframe['Economy'],
               planes_dataframe['Comfort'],
               planes_dataframe['Business'])
           ]  # Список кортежей вида (название судна, мест в экономе, мест в комфорте, мест в бизнесе)
    types = sorted(res, key=lambda x: x[0])
    print('Модели самолетов, находящихся в использовании:')
    for board in types:
        print('\t', board[0], '\n\t  Места:', sep='')
        (eco, com, bus) = board[1:]
        if eco > 0:
            print(f'\t\tЭконом-класс: {eco}')
        if com > 0:
            print(f'\t\tКомфорт-класс: {com}')
        if bus > 0:
            print(f'\t\tБизнесс-класс: {bus}')
    types = [
        x[0].split()[0].lower() for x in types
    ]  # Приведение производителя (первое слово в строке) к нижнему регистру
    aircraft_by_producers = {x.title(): types.count(x) for x in air_prod}
    aircraft_by_producers = [(x, aircraft_by_producers[x]) for x in aircraft_by_producers.keys() if
                             aircraft_by_producers[x] > 0]
    aircraft_by_producers = {x: y for (x, y) in aircraft_by_producers}

    print(filler('-'))

    print('Количество используемых самолетов по производителям:')
    for (producer, amount) in aircraft_by_producers.items():
        print(f'\t{producer}: {amount} ед.')

    print(filler('='))


# TODO Добавить генерацию красивых pdf-документов с отчетностью
def flights_data(flights_dataframe: pd.DataFrame) -> None:
    """
    Анализ данных, связанных с рейсами бортов авиакомпании
    :param flights_dataframe: Pandas Dataframe с информацией о полетах
    :raises: TypeError
    :return: Ничего не возвращается (Неявный None)
    """
    if type(flights_dataframe) != pd.DataFrame:
        raise TypeError('Метод предназначен для обработки pandas.Dataframe')
    print("ПОЛЕТЫ")
    print(f'Среднее время полета на основе {len(flights_dataframe)} записей:')
    # Получение полных перечней времени взлета и посадки бортов и отсечение нулевого смещения по часовому поясу
    departure_actual = [str(x)[:-6] for x in flights_dataframe[['actual_departure']]['actual_departure']]
    arrival_actual = [str(x)[:-6] for x in flights_dataframe[['actual_arrival']]['actual_arrival']]
    departure_planned = [str(x)[:-6] for x in flights_dataframe[['scheduled_departure']]['scheduled_departure']]

    with mp.Pool(4) as pool:
        flight_time = pool.starmap(mp_avg_flight_time, zip(arrival_actual, departure_actual))
        avg_flight_time = datetime.timedelta(seconds=np.average(flight_time))

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

    print(str(avg_flight_time).split('.')[0])
    print(filler('-'))

    avg_delay_time = str(datetime.timedelta(seconds=np.average(delayed))).split('.')[0]
    flights_dataframe['delay'] = delay_time

    res = f'ИЗ {len(departure_actual)} совершенных рейсов\n' \
          f'\tВовремя вылетели: {in_time}'
    if in_time > 0:
        in_time_percent = "{:.3%}".format(in_time / len(departure_actual))
        res += f' ({in_time_percent})'
    res += f'\n\tОпоздали с вылетом: {len(delayed)}'
    if len(delayed) > 0:
        delayed_percent = "{:.3%}".format(len(delayed) / len(departure_actual))
        res += f' ({delayed_percent}).\n' \
               f'\t  При этом среднее время задержки равно: {avg_delay_time}'
    res += f'\n\tВылетели с опережением графика: {len(too_soon)}'
    if len(too_soon) > 0:
        too_soon_percent = "{:.3%}".format(len(too_soon) / len(departure_actual))
        res += f' ({too_soon_percent}.'
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
            departures = data['departure_airport'].tolist()
            arrivals = data['arrival_airport'].tolist()
            to_print = [board + ' из ' + dep + ' в ' + arr for board, dep, arr in zip(maxes, departures, arrivals)]
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
            departures = data['departure_airport'].tolist()
            arrivals = data['arrival_airport'].tolist()
            to_print = [board + ' из ' + dep + ' в ' + arr for board, dep, arr in zip(boards, departures, arrivals)]
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
            departures = data['departure_airport'].tolist()
            arrivals = data['arrival_airport'].tolist()
            to_print = [board + ' из ' + dep + ' в ' + arr for board, dep, arr in zip(boards, departures, arrivals)]
            print('Наиболее часто вылетающие с задержкой рейсы:')
            for element in to_print:
                print(f'\t {element}')
        else:
            data = df_soon.loc[df_soon['flight_no'] == maxes[0]]
            dep = data['departure_airport'].item()
            arr = data['arrival_airport'].item()
            print(f'\tНаиболее часто вылетающий с задержкой рейс:\n\t{maxes[0]} из {dep} в {arr}')

    print(filler('='))


def tickets_data(tickets_dataframe: pd.DataFrame) -> None:
    """
    Анализ данных, связанных с билетами
    :param tickets_dataframe: Pandas Dataframe с информацией о билетах
    :raises: TypeError
    :return: Ничего не возвращается (Неявный None)
    """
    if type(tickets_dataframe) != pd.DataFrame:
        raise TypeError('Метод предназначен для обработки pandas.Dataframe')
    print('БИЛЕТЫ')
    economy_tickets = tickets_dataframe.loc[tickets_dataframe['type'] == 'Economy']
    economy_tickets_amount = len(economy_tickets)
    avg_price = economy_tickets['price'].sum() / economy_tickets_amount
    print(f'Средняя цена билета:\n'
          f'\tВ эконом-класс: {"{:.2f}".format(avg_price)}')
    comfort_tickets = tickets_dataframe.loc[tickets_dataframe['type'] == 'Comfort']
    comfort_tickets_amount = len(comfort_tickets)
    avg_price = comfort_tickets['price'].sum() / comfort_tickets_amount
    print(f'\tВ комфорт-класс: {"{:.2f}".format(avg_price)}')
    business_tickets = tickets_dataframe.loc[tickets_dataframe['type'] == 'Business']
    business_tickets_amount = len(business_tickets)
    avg_price = business_tickets['price'].sum() / business_tickets_amount
    print(f'\tВ бизнесс-класс: {"{:.2f}".format(avg_price)}')
    print(filler('-'))
    total = economy_tickets_amount + business_tickets_amount + comfort_tickets_amount
    economy_percent = economy_tickets_amount / total
    comfort_percent = comfort_tickets_amount / total
    business_percent = business_tickets_amount / total
    print(f'Из всех билетов куплено:\n'
          f'\tВ эконом-класс: {economy_tickets_amount} ({"{:.3%}".format(economy_percent)})\n'
          f'\tВ комфорт-класс: {comfort_tickets_amount} ({"{:.3%}".format(comfort_percent)})\n'
          f'\tВ бизнесс-класс: {business_tickets_amount} ({"{:.3%}".format(business_percent)})')

    print(filler('='))


def filler(symbol: str):
    """
    Функция для графического отделения данных в консольном отображении.
    Знак '=' отделяется строчными отступами сверху и снизу
    :param symbol: Символ для повторения в консоли
    :raises: TypeError
    :return: Строка - заполнитель
    """
    # Разделитель между основными разделами
    if type(symbol) != str:
        raise TypeError('Метод принимает объект str в качестве аргумента')
    if symbol == '=':
        return '\n' + symbol * 50 + '\n'
    # Разделитель подразделов
    else:
        return symbol * 50


if __name__ == '__main__':
    start_time = datetime.datetime.now()  # Начальная временная метка для отслеживания времени выполнения скрипта
    pd.set_option('display.max_columns', 15)
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
        # Анализ данных по бортам
        planes_data(dataframe)
        # Анализ данных по полетам
        dataframe = psql.read_sql("SELECT * FROM flights WHERE status='Arrived'", connection)
        flights_data(dataframe)
        # Анализ данных по билетам
        dataframe = psql.read_sql("SELECT fare_conditions as type, amount as price FROM ticket_flights", connection)
        tickets_data(dataframe)

    except (Exception, Error) as error:
        print('Ошибка при выполнении скрипта:\n\t', error)

    finally:
        if connection:
            cursor.close()
            connection.close()
            print('Подключение к базе данных закрыто')
        print('Процесс выполнен за', datetime.datetime.now() - start_time)
