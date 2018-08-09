import os
import sqlite3

BASE_PATH=os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(BASE_PATH, 'db', 'Trader.db')

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

stocks=[['159915',1.7],['510500',6.0],['510300',4.0],['510050',3.0],['510880',3.0]]
for stock in stocks:
    drop_table='drop table if exists grid_{}'.format(stock[0])
    print(drop_table)
    cursor.execute(drop_table)
    create_table='''CREATE TABLE grid_{} ( 
    id              INTEGER PRIMARY KEY ASC AUTOINCREMENT
                            NOT NULL
                            UNIQUE,
    price           DOUBLE  NOT NULL
                            UNIQUE,
    hold            INTEGER DEFAULT ( 0 ),
    contract_number INTEGER DEFAULT ( 0 ) 
)'''.format(stock[0])
    cursor.execute(create_table)
    grids=['%.3f' % (stock[1]*0.99**i) for i in range(50)]
    for g in grids:
        instert_sql='insert into grid_{} (price) values ({})'.format(stock[0],g)
        print(instert_sql)
        cursor.execute(instert_sql)

conn.commit()
conn.close()