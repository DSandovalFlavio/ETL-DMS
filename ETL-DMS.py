# %%
import pyodbc
from sqlalchemy import create_engine
import pandas as pd
pyodbc.drivers()
# %%
user='postgres'
password='postgresqlroot'
host='localhost'
port='5432'
database='BI_DB'
db_url = f'postgresql://{user}:{password}@{host}:{port}/{database}'
# %%
engine = create_engine(db_url)
try:
    conn = engine.connect()
    print('Connection successful')
except Exception as e:
    print('Connection failed')
    print(e)
# %%
query = '''
select "customerID"
from public.customers;
'''
df = pd.read_sql(query, engine)
# %%
