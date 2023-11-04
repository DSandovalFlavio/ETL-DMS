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
path = 'data\data_dms_20180101.csv'
df_raw_data = pd.read_csv(path)
# %%
query_ship_modes = '''
select *
from public.ship_modes
'''
df_ship_modes = pd.read_sql(query_ship_modes, engine)
# %%
df_unido = pd.merge(
    df_raw_data[['Order_ID', 'Ship_Mode']],
    df_ship_modes,
    left_on='Ship_Mode',
    right_on='shipmodeName',
    how='left'
)
# %%
