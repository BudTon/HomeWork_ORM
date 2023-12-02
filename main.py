import sqlalchemy
import json
from sqlalchemy.orm import sessionmaker
from models import create_tables, Stock, Publisher, Book, Shop, Sale


def main(DSN):
    engine = sqlalchemy.create_engine(DSN)
    create_tables(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    with open('tests_data.json', 'r') as fd:
        data = json.load(fd)

        for record in data:
            model = {
                'publisher': Publisher,
                'shop': Shop,
                'book': Book,
                'stock': Stock,
                'sale': Sale,
            }[record.get('model')]
            session.add(model(id=record.get('pk'), **record.get('fields')))
            session.commit()
    session.close()
    return session


def get_data(input_data, session):
    db_session = (session.query(Book.title, Shop.name, Sale.price, Sale.count, Sale.date_sale)
                  .select_from(Shop).join(Stock).join(Book).join(Publisher).join(Sale))
    if input_data.isdigit():
        data_list = db_session.filter(Publisher.id == input_date).all()
    for d in data_list:
        print(f'{d[0]:<40}', '|', f'{d[1]:<10}', '|', f'{d[2]*d[3]:^7}', '|', d[4].strftime('%d-%m-%Y'))


if __name__ == '__main__':
    DSN = 'postgresql://postgres:postgres@localhost:5432/HomeWork_ORM'
    session = main(DSN)
    input_date = input('Введите Имя или Идентификатор издателя - ')
    get_data(input_date, session)




