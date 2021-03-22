from flask_setting import *
from sqlalchemy import create_engine
import os
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import MetaData

def drop_table():
    engine = create_engine(os.getenv('DB_URI'))
    print(engine.table_names())

    table_class = input('Input table object class:')

    eval(table_class).__table__.drop(engine)

    ConfigRulesInclExclPidBased

    print(engine.table_names())

def drop_table(table_name):
   engine = create_engine(os.getenv('DB_URI'))
   print('Existing tables:', engine.table_names())

   base = declarative_base()
   metadata = MetaData(engine, reflect=True)
   table = metadata.tables.get(table_name)
   if table is not None:
       base.metadata.drop_all(engine, [table], checkfirst=True)

   engine = create_engine(os.getenv('DB_URI'))
   print('Current tables:', engine.table_names())


if __name__=='__main__':
    table_name = input('Input table name to drop:')
    drop_table(table_name)
