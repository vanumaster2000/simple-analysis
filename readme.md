# Скрипт для анализа данных из БД
Использует:
1. psycopg2; 
2. pandas;
3. numpy;
4. multiprocessing;
5. datetime;
6. fpdf;
7. os.

База данных - PostgreSQL. Анализ проводится для средней демо-версии
базы данных. [Скачать ее можно здесь](https://postgrespro.ru/education/demodb).  
Скрипт производит простой анализ различных показателей на основе данных, выгружаемых из БД:
1. Парк авиасудов:
    1. Количество судов каждого производителя, каждой модели производителя и количество мест в каждом из классов
   (Эконом, комфорт, бизнесс);
2. Полеты:
    1. Среднее время полета борта;
    2. Количество и процентное содержание от общего количества:
        1. Вылетов с опозданием;
        2. Вылетов раньше времени;
        3. Вылетов вовремя;
    3. Наиболее часто вылетающие вовремя рейсы и их маршруты;
    4. Наиболее часто вылетающие вовремя рейсы и их маршруты;
3. Билеты:
   1. Средняя цена билета для каждого класса;
   2. Количество и процентное содержание билетов каждого класса в общей массе купленных.
    
На данный момент остальные аналитические аспекты разрабатываются. <br>
Разрабатывается генерация отчетности в pdf-формате в т.ч. с использованием графиков. Примеры
отчетов находятся в директории output.

### Обратить внимание
В следующем блоке кода (основной блок) требуется указать данные для подключения к базе,
 согласно документации pyscopg2.
```python
connection = psycopg2.connect(
            host='localhost',
            port='5432',
            database='demo',
            user='superUser',
            password='superUserPassword'
        )
```
