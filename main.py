from pymongo import MongoClient
import csv
from datetime import datetime, timedelta
import logging
import os

def get_env_name(name):
    return os.environ.get(name)

logger = logging.getLogger(__name__)
logging.basicConfig(
    filename="mylog.log", #файл-обработчик
    level=logging.DEBUG, #уровень обработки - 10.
    format="%(asctime)s %(levelname)s at row #%(lineno)d %(message)s", datefmt="%m-%d-%Y %H:%M:%S",
)

Table_name = "zno_RESULTS_19_20"

Buffer_table = "buffer_table"
Years = [2019, 2020]

def custom_query(collection):
    '''Порівняти найгірший бал з Історії України у 2020 та 2019 роках серед тих кому було зараховано тест'''
    user_query = collection.aggregate(
        [
            {"$match": {"histTestStatus": "Зараховано"}},
            {"$group": {"_id": "$Year", "minimum": {"$min": "$histBall100"}}},
        ]
    )

    #запись результата в файл
    with open("custom_query_result.csv", "w") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=["_id", "maximum"])
        writer.writeheader()
        for data in user_query:
            writer.writerow(data)
    logger.info("CSV created")

#создаем 2 таблички: буферную и главную
def create_tables(conn):
    logger.info("Creating tables")

    connections = conn.list_collection_names()

    if "zno_RESULTS_19_20" in connections:
        coll1 = conn.zno_RESULTS_19_20
    else:
        coll1 = conn.zno_RESULTS_19_20


    if "buffer_table" in connections:
        coll2 = conn.buffer_table
    else:
        coll2 = conn.buffer_table
        coll2.insert_one({"execution_time": 0, "rows": 0, "year": 2019})
    logger.info("Created tables")
    return (coll1, coll2)


#вставляем данные
def insert_data(coll1, coll2, csv_filename, year, last_row_number, start_time):

    previous_stack_time = start_time
    logger.info(f"Inserting data from {csv_filename}")

    with open(csv_filename, encoding="cp1251") as csv_file:
        csv_r = csv.DictReader(csv_file, delimiter=";")

        i = 0
        array = []

        coll2.update_one({}, {"$set": {"year": year}})
        for row in csv_r:
            i += 1
            if i <= last_row_number:
                continue
            try:
                row["Year"] = year
                array.append(row)

            except Exception as e:
                logger.error(f"SMTH went wrong {e}")
                return 1

            #добавляем по 100 строк
            if i % 100 == 0:
                now = datetime.now()

                try:
                    coll1.insert_many(array)
                    coll2.update_one(
                        {},
                        {
                            "$set": {
                                "rows": i,
                            },
                            "$inc": {
                                "execution_time": (
                                    now - previous_stack_time
                                ).microseconds
                            },
                        },
                    )


                    print("№ ",i)
                    array = []

                except Exception as e:
                    logger.error(f"Connection is broken: {e}")
                    raise Exception

                previous_stack_time = now

    try:
        coll1.insert_many(array)
        coll2.update_one(
            {},
            {
                "$set": {
                    "rows": i,
                },
                "$inc": {"execution_time": (now - previous_stack_time).microseconds},
            },
        )
        print(i)
        array = []

    except Exception as e:
        logger.error(f"Connection is broken: {e}")
        raise Exception
    logger.info(f"Inserting from {csv_filename} done")


def main():
    #записываем начальное время
    start_time = datetime.now()
    logger.info(f"Start time is {start_time}")

    #коннектимся к базе данных
    client = MongoClient(port=int(get_env_name("PORT")))
    db = client["lab4"]
    coll1, coll2 = create_tables(db)

    #здесь делаем проверку на то, сколько уже данных успело подгрузиться в буферную таблицу.
    #В таком случае начинаем с последней строчки, на которй остановились
    try:
        last_row = coll2.find_one()
        row_number = last_row["rows"]
        year_zno = last_row["year"]

    except Exception as e:
        logger.warning(f"Cannot get data from {Buffer_table}: {e}")
        year_zno = Years[0]
        row_number = 0

    logger.info(
        f"Starting inserting from {row_number} row from file for {year_zno} year"
    )

    if year_zno:

        index = Years.index(year_zno)
        for year in Years[index:]:
            insert_data(
                coll1, coll2, f"Odata{year}File.csv", year, row_number, start_time)
            row_number = 0
    else:

        for year in Years:
            insert_data(
                coll1, coll2, f"Odata{year}File.csv", year, row_number, start_time)
            row_number = 0

    #наш запрос делаем
    custom_query(coll1)

    insert_t = coll2.find_one()

    end_t = datetime.now()
    logger.info(f"End time {end_t}")

    #ыводим в логи то,с колько времени у нас занял весь процесс программы
    logger.info(
        f"Inserting executing time {timedelta(microseconds=insert_t['execution_time'])}"
    )
    logger.info("Program is done!")

if __name__ == "__main__":
    main()
