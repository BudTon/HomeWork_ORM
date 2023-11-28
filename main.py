import sqlalchemy
import json
from sqlalchemy.orm import sessionmaker
from Models import create_tables, Stock, Publisher, Book, Shop, Sale

DSN = 'postgresql://postgres:postgres@localhost:5432/HomeWork_ORM'
engine = sqlalchemy.create_engine(DSN)

create_tables(engine)

Session = sessionmaker(bind=engine)
session = Session()

with open('tests_data.json', 'r') as fd:
    data = json.load(fd)
    for d in data:
        if d['model'] == 'publisher':
            publisher = Publisher(id=d['pk'], name=d['fields']['name'])
            session.add(publisher)
            session.commit()
        elif d['model'] == 'shop':
            shop = Shop(id=d['pk'], name=d['fields']['name'])
            session.add(shop)
            session.commit()
        elif d['model'] == 'book':
            book = Book(id=d['pk'], title=d['fields']['title'], id_publisher=d['fields']['id_publisher'])
            session.add(book)
            session.commit()
        elif d['model'] == 'stock':
            stock = Stock(id=d['pk'], id_shop=d['fields']['id_shop'], id_book=d['fields']['id_book'],
                          count=d['fields']['count'])
            session.add(stock)
            session.commit()
        elif d['model'] == 'sale':
            sale = Sale(id=d['pk'], price=d['fields']['price'], date_sale=d['fields']['date_sale'],
                          count=d['fields']['count'], id_stock=d['fields']['id_stock'])
            session.add(sale)
            session.commit()
        else:
            print(d, f'\n- Error Ошибка в названии Таблицы')


    # for record in data:
    #     model = {
    #         'publisher': Publisher,
    #         'shop': Shop,
    #         'book': Book,
    #         'stock': Stock,
    #         'sale': Sale,
    #     }[record.get('model')]
    #     session.add(model(id=record.get('pk'), **record.get('fields')))
    #     session.commit()

session.close()

input_date = input('Введите Имя или Идентификатор издателя - ')
if input_date.isnumeric():
    for sale_table in session.query(Sale).join(Stock).join(Book).join(Publisher).filter(Publisher.id == input_date).all():
        for shop_name in session.query(Shop).join(Stock).join(Sale).filter(Sale.id == sale_table.id).all():
            for book_title in session.query(Book).join(Stock).join(Sale).filter(Sale.id == sale_table.id).all():
                purchase_price = sale_table.price * sale_table.count
                print(f'{book_title.title:<40}','|',f'{shop_name.name:<10}','|',f'{purchase_price:^7}','|', sale_table.date_sale)
else:
    for sale_table in session.query(Sale).join(Stock).join(Book).join(Publisher).filter(Publisher.name == input_date).all():
        for shop_name in session.query(Shop).join(Stock).join(Sale).filter(Sale.id == sale_table.id).all():
            for book_title in session.query(Book).join(Stock).join(Sale).filter(Sale.id == sale_table.id).all():
                purchase_price = sale_table.price * sale_table.count
                print(f'{book_title.title:<40}','|',f'{shop_name.name:<10}','|',f'{purchase_price:^7}','|', sale_table.date_sale)




