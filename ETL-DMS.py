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
# %% Funciones para validar informacion faltante en las tablas dimensionales
def check_new_customers(df, engine):
    customers_name = list(df['Customer_Name'].unique()) 
    customers_name = str(customers_name).replace('[', '(').replace(']', ')')
    query = '''
    select "customerID", "customerName"
    from public.customers
    where customers."customerName" in {lista_de_nombres_de_clientes}
    '''.format(lista_de_nombres_de_clientes=customers_name)
    df_customers = pd.read_sql(query, engine)
    df_customers_validated = df[['Customer_Name']].drop_duplicates()
    df_customers_validated = pd.merge(
        df_customers_validated,
        df_customers,
        left_on='Customer_Name',
        right_on='customerName',
        how='left'
    )
    if df_customers_validated['customerID'].isnull().sum() > 0:
        print('Hay nuevos clientes')
        return df
        
    else:
        print('No hay nuevos clientes')
        df = pd.merge(
            df,
            df_customers_validated[['Customer_Name', 'customerID']],
            on='Customer_Name',
            how='left'
        )
        return df
def ingest_orders(df, engine):
    df_orders = df[['Order_ID', 'Order_Date', 'customerID', 'Sales']]
    df_orders.to_sql(tabla='customers',con=engine, if_exists='append')


# %%
path = 'data\data_dms_20180101.csv'
df_raw_data = pd.read_csv(path)
# eliminar filas con valores nulos
df_raw_data = df_raw_data.dropna()
# %% Validar si hay nuevos clientes
if engine is not None:
    # funciones de chack de nueva info
    df_raw_data = check_new_customers(df_raw_data, engine)
    
    
    # 3 funciones de ingesta de nuevos registros
    ingest_orders(df_raw_data, engine)
else:
    print('No hay conexion a la base de datos')
    print(f'Error: {create_engine_postgresql(config)[1]}')
# %%





















# %%
query = '''
select "customerID"
from public.customers;
'''
df = pd.read_sql(query, engine)
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
