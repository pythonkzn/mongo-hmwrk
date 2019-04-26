import csv
import re


from pymongo import MongoClient
import datetime


def db_connect():
    client = MongoClient('mongodb://localhost:27017/')
    db = client.hmwrk_db
    return db


def read_data(csv_file, db):
    """
    Загрузить данные в бд из CSV-файла
    """
    with open(csv_file, encoding='utf8') as csvfile:
        # прочитать файл с данными и записать в коллекцию
        reader = csv.DictReader(csvfile, delimiter=',')
        concerts = db.concerts
        for line in reader:
            line['Цена'] = int(line['Цена'])
            line['Дата'] = datetime.datetime(2019, int(line['Дата'][3:]), int(line['Дата'][:-3]), 0, 0)
            concerts.insert_one(line).inserted_id
        for item in concerts.find():
            print(item)


def find_cheapest(db):
    concerts = db.concerts
    for item in concerts.find().sort('Цена').limit(14):
        print(item)

    """
    Отсортировать билеты из базы по возрастания цены
    Документация: https://docs.mongodb.com/manual/reference/method/cursor.sort/
    """


def find_by_name(name, db):
    """
    Найти билеты по имени исполнителя (в том числе – по подстроке),
    и вернуть их по возрастанию цены
    """
    regex = re.compile('.*' + name + '.*')
    concerts = db.concerts
    for item in concerts.find({'Исполнитель': regex}).sort('Цена'):
        print(item)


def sort_by_date(db):
    concerts = db.concerts
    for item in concerts.find().sort('Дата'):
        print(item)


def main():
    database = db_connect()

    # для запуска необходимой функции необходимо раскомментировать строку
    
    #read_data('file.csv', database)
    #find_cheapest(database)
    #find_by_name('1975', database)
    #sort_by_date(database)


if __name__ == '__main__':
    main()

