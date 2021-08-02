# Скрипт для анализа данных из БД
Использует:
1. psycopg2; 
2. pandas;
3. numpy;
4. multiprocessing;
5. datetime.

База данных - PostgreSQL. Анализ проводится для средней демо-версии
базы данных. [Скачать ее можно здесь](https://postgrespro.ru/education/demodb).  
Производит простой анализ различных показателей на основе данных, выгружаемых из БД:
1. Парк авиасудов:
    1. Количество судов каждой модели;
    2. Количество судов каждого производителя;
2. Полеты:
    1. Среднее время полета борта;
    2. Количество и процентное содержание от общего количества:
        1. Вылетов с опозданием;
        2. Вылетов раньше времени;
        3. Вылетов вовремя;
    3. Наиболее часто вылетающие вовремя рейсы и их маршруты;
    4. Наиболее часто вылетающие вовремя рейсы и их маршруты.
    
На данный момент остальные аналитические аспекты разрабатываются. <br>
В будущем будет добавлена генерация отчетности в PDF-формате для удобства
хранения и представления.<br>
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
