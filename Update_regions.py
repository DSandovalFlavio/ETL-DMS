# %%
from sqlalchemy import create_engine
import pandas as pd
import configparser
# %%
config = configparser.ConfigParser()
config.read('config.ini')
# %%
def create_engine_postgresql(config):
    db_url = 'postgresql://{user}:{password}@{host}:{port}/{database}'.format(
    user=config['PostgreSQL DMS']['user'],
    password=config['PostgreSQL DMS']['password'],
    host=config['PostgreSQL DMS']['host'],
    port=config['PostgreSQL DMS']['port'],
    database=config['PostgreSQL DMS']['database']
    )
    engine = create_engine(db_url)
    try:
        engine.connect()
        print('Connection successful')
        return [engine]
    except Exception as e:
        print('Connection failed')
        print(e)
        return [None, e]
engine = create_engine_postgresql(config)[0]
# %%
# Carga de archivo
path = 'catalogos de datos\warehouses.xlsx'
df_warehouses= pd.read_excel(path, sheet_name='Sheet2')
df_warehouses = df_warehouses.dropna() # eliminar filas con valores nulos
df_warehouses = df_warehouses[['RegionName_N']].drop_duplicates()
df_warehouses.columns = ['regionName']
# %%
df_warehouses.to_sql(name='region', con=engine, if_exists='append', index=False)
# %%
