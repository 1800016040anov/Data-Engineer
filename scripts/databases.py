import pandas as pd
from sqlalchemy import create_engine

import consts

database_loc = f"postgresql://{consts.USER}:{consts.PASSWORD}@{consts.HOST}:{consts.PORT}/{consts.DATABASE_NAME} "
engine = create_engine(database_loc)
conn = engine.connect()


    
def df_to_db(table_name,name,schema,conn):
    table_name.to_sql(name=name,
                 con=conn,
                 schema=schema,
                 if_exists='fail',
                 index=True,
                 index_label=None,
                 chunksize=None,
                 dtype=None,
                 method=None)
    print("success to save data ")
def query_sql(sql):
    pd.read_sql(sql,
            con=conn,)