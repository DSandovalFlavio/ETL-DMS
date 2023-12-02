# %%
from sqlalchemy import create_engine
import pandas as pd
import configparser
import os
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
# %% Funciones para validar informacion faltante en las tablas dimensionales
def check_new_customers(df, engine):
    # obtenemos los nombres de los clientes unicos dentro del archivo de ventas
    customers_name = list(df['Customer_Name'].unique())
    customers_name = str(customers_name).replace('[', '(').replace(']', ')')
    # obtenemos los clientes que ya se encuentran en la tabla de clientes
    query = '''
    select "customerID", "customerName"
    from public.customers
    where customers."customerName" in {lista_de_nombres_de_clientes}
    '''.format(lista_de_nombres_de_clientes=customers_name)
    df_customers = pd.read_sql(query, engine)
    # validamos si hay nuevos clientes
    df_customers_validated = df[['Customer_Name']].drop_duplicates() 
    df_customers_validated = pd.merge(
        df_customers_validated, # tenemos todos los clientes del csv
        df_customers, # tenemos todos los clientes que ya estaban registrados en la db
        left_on='Customer_Name',
        right_on='customerName',
        how='left'
    ) # el resultado de esta union nos dara valores nulos para los clientes que no estan en la db
    # validamos si hay nuevos clientes y generaremos un log con la lista d nombres de los nuevos clientes
    if len(df_customers_validated[df_customers_validated['customerID'].isnull()]) > 0:
        print('Hay nuevos clientes') # avisamos que hay nuevos clientes
        # vamos a generar un log con los nuevos clientes
        df_new_customers = df_customers_validated[df_customers_validated['customerID'].isnull()][['Customer_Name']]
        df_new_customers.to_csv('logs/new_customers.csv', index=False)
        # devolvemos el df con los clientes que ya estaban registrados en la db
        df_customers_validated = df_customers_validated[df_customers_validated['customerID'].notnull()]
        df = pd.merge(df,
                    df_customers_validated[['Customer_Name', 'customerID']],
                    on='Customer_Name',
                    how='left'
            )
        df = df.dropna() # eliminamos los registros que no tienen customerID
    else:
        print('No hay nuevos clientes')
        df = pd.merge(df,
                    df_customers_validated[['Customer_Name', 'customerID']],
                    on='Customer_Name',
                    how='left'
            )
    return df
def check_new_products(df, engine):
    # obtenemos los nombres de los productos unicos dentro del archivo de ventas
    products_name = list(df['Product_Name'].unique())
    products_name = str(products_name).replace('[', '(').replace(']', ')')
    # obtenemos los productos que ya se encuentran en la tabla de productos
    query = '''
    select "productID", "productName", "uPrice"
    from public.products
    where products."productName" in {lista_de_nombres_de_productos}
    '''.format(lista_de_nombres_de_productos=products_name)
    df_products = pd.read_sql(query, engine)
    # validamos si hay nuevos productos
    df_products_validated = df[['Product_Name']].drop_duplicates() 
    df_products_validated = pd.merge(
        df_products_validated, # tenemos todos los productos del csv
        df_products, # tenemos todos los productos que ya estaban registrados en la db
        left_on='Product_Name',
        right_on='productName',
        how='left'
    ) # el resultado de esta union nos dara valores nulos para los productos que no estan en la db
    # validamos si hay nuevos productos y generaremos un log con la lista d nombres de los nuevos productos
    if len(df_products_validated[df_products_validated['productID'].isnull()]) > 0:
        print('Hay nuevos productos') # avisamos que hay nuevos productos
        # vamos a generar un log con los nuevos productos
        df_new_products = df_products_validated[df_products_validated['productID'].isnull()][['Product_Name']]
        df_new_products.to_csv('logs/new_products.csv', index=False, encoding='utf8')
        # devolvemos el df con los productos que ya estaban registrados en la db
        df_products_validated = df_products_validated[df_products_validated['productID'].notnull()]
        df = pd.merge(df,
                    df_products_validated[['Product_Name', 'productID', 'uPrice']],
                    on='Product_Name',
                    how='left'
            )
        df = df.dropna() # eliminamos los registros que no tienen productID
    else:
        print('No hay nuevos productos')
        df = pd.merge(df,
                    df_products_validated[['Product_Name', 'productID', 'uPrice']],
                    on='Product_Name',
                    how='left'
            )
    return df
def check_new_ship_modes(df, engine):
    # obtenemos los nombres de los modos de envio unicos dentro del archivo de ventas
    ship_modes_name = list(df['Ship_Mode'].unique())
    ship_modes_name = str(ship_modes_name).replace('[', '(').replace(']', ')')
    # obtenemos los modos de envio que ya se encuentran en la tabla de modos de envio
    query = '''
    select "shipmodeID", "shipmodeName"
    from public.ship_modes
    where ship_modes."shipmodeName" in {lista_de_nombres_de_ship_modes}
    '''.format(lista_de_nombres_de_ship_modes=ship_modes_name)
    df_ship_modes = pd.read_sql(query, engine)
    # validamos si hay nuevos modos de envio
    df_ship_modes_validated = df[['Ship_Mode']].drop_duplicates() 
    df_ship_modes_validated = pd.merge(
        df_ship_modes_validated, # tenemos todos los modos de envio del csv
        df_ship_modes, # tenemos todos los modos de envio que ya estaban registrados en la db
        left_on='Ship_Mode',
        right_on='shipmodeName',
        how='left'
    ) # el resultado de esta union nos dara valores nulos para los modos de envio que no estan en la db
    # validamos si hay nuevos modos de envio y generaremos un log con la lista d nombres de los nuevos modos de envio
    if len(df_ship_modes_validated[df_ship_modes_validated['shipmodeID'].isnull()]) > 0:
        print('Hay nuevos modos de envio') # avisamos que hay nuevos modos de envio
        # vamos a generar un log con los nuevos modos de envio
        df_new_ship_modes = df_ship_modes_validated[df_ship_modes_validated['shipmodeID'].isnull()][['Ship_Mode']]
        df_new_ship_modes.to_csv('logs/new_ship_modes.csv', index=False, encoding='utf8')
        # devolvemos el df con los modos de envio que ya estaban registrados en la db
        df_ship_modes_validated = df_ship_modes_validated[df_ship_modes_validated['shipmodeID'].notnull()]
        df = pd.merge(df,
                    df_ship_modes_validated[['Ship_Mode', 'shipmodeID']],
                    on='Ship_Mode',
                    how='left'
            )
        df = df.dropna()
    else:
        print('No hay nuevos modos de envio')
        df = pd.merge(df,
                    df_ship_modes_validated[['Ship_Mode', 'shipmodeID']],
                    on='Ship_Mode',
                    how='left'
            )
    return df
def check_new_regions(df, engine):
    df_cat_warehouse = pd.read_excel(
        'catalogos de datos\warehouses.xlsx', 
        sheet_name='Cat_Reg')
    dic_corr = {
        'Veracruz Llave':'Veracruz', 'Québec':'Quebec' , 'Coahuila De Zaragoza':'Chihuahua', 
        'Mexico':'México','District of Columbia':'British Columbia', 
        'San Luis Potosi':'San Luis Potosí', 'Michoacan De Ocampo':'Michoacán',
        'Nuevo Leon':'Nuevo León'
    }
    df['State_Province'] = df['State_Province'].replace(dic_corr)
    df = pd.merge(
        df,
        df_cat_warehouse[['State', 'Region']],
        left_on='State_Province',
        right_on='State',
        how='left'
    )
    df_regions = pd.read_sql('select * from public.region', engine)
    df = pd.merge(df,
                df_regions,
                left_on='Region',
                right_on='regionName',
                how='left'
            )
    if len(df[df['regionID'].isnull()]) > 0:
        print('Hay nuevas regiones')
        df_new_regions = df[df['regionID'].isnull()][['State_Province', 'Region']]
        df_new_regions.to_csv('logs/new_regions_state.csv', index=False, encoding='utf8')
        df = df[df['regionID'].notnull()]
    else:
        print('No hay nuevas regiones')
    df = df.drop(columns=['State', 'Region', 'regionName'])
    return df
# %% Funcion de ingesta
def ingesta_dms(path, engine):
    print(f'Iniciando proceso de ingesta para el archivo {path}')
    # Carga de archivo
    df_raw_data = pd.read_csv(path, encoding='utf8')
    df_raw_data = df_raw_data.dropna() # eliminar filas con valores nulos
    print('El archivo tiene {} registros'.format(len(df_raw_data)))
    # Codigo de validaciones e ingesta para la tabla de orders
    df_raw_data = check_new_customers(df_raw_data, engine)
    df_raw_data = check_new_products(df_raw_data, engine)
    # para obtener sale necesitamos multiplicar quantity * uPrice * (1 - discount)
    df_raw_data['sale'] = df_raw_data['Quantity'] * df_raw_data['uPrice'] * (1 - df_raw_data['Discount'])
    # Codigo de validaciones e ingesta para la tabla de orders details
    # ya se obtuvo la informacion para la tabla de orders details en los pasos anteriores
    # Codigo de validaciones e ingesta para la tabla de ships
    df_raw_data = check_new_ship_modes(df_raw_data, engine)
    #codigo de validaciones e ingesta para la tabla de regions
    df_raw_data = check_new_regions(df_raw_data, engine)
    # ingesta de datos
    dict_country_code = {'United States':'US', 'Canada':'CA', 'Mexico':'MX'}
    df_raw_data['Order_ID'] = df_raw_data.Country_Region.replace(dict_country_code) +'-'+ pd.to_datetime(df_raw_data.Order_Date).dt.year.astype('str') +'-'+ df_raw_data.Order_ID.astype('str').str.zfill(6)
    # tabla orders : orderID, orderDate, customerID, sale
    orders = df_raw_data[['Order_ID', 'Order_Date', 'customerID', 'sale', 'Country_Region']]
    orders = orders.drop(columns=['Country_Region'])
    orders.columns = ['orderID', 'orderDate', 'customerID', 'sale']
    orders = orders.groupby(['orderID', 'orderDate', 'customerID']).agg({'sale':'sum'}).reset_index()
    orders.to_sql('orders', engine, if_exists='append', index=False)
    print('Se ingirieron {} registros en la tabla orders'.format(len(orders)))
    # tabla orders_details : orderID, productID, quantity, discount
    orders_details = df_raw_data[['Order_ID', 'productID', 'Quantity', 'Discount']]
    orders_details.columns = ['orderID', 'productID', 'quantity', 'discount']
    orders_details.to_sql('order_details', engine, if_exists='append', index=False)
    print('Se ingirieron {} registros en la tabla orders_details'.format(len(orders_details)))
    # tabla ships : shipID, shipDate, shipState, shipCity, postalCode, shipCountry, shipmodeID, warehouseID
    ships = df_raw_data[[
        'Order_ID', 'Order_Date','Ship_Date', 
        'State_Province', 'City', 'Postal_Code', 'Country_Region', 
        'shipmodeID', 'regionID']]
    ships['Ship_Date'] = pd.to_datetime(ships['Order_Date']) + pd.to_timedelta(ships['Ship_Date'], unit='d')
    ships = ships.drop(columns=['Order_Date'])
    ships.columns = ['shipID', 'shipDate', 
                    'shipState', 'shipCity', 'postalCode', 'shipCountry', 
                    'shipmodeID', 'wherehouseID']
    ships = ships.drop_duplicates()
    ships.to_sql('ships', engine, if_exists='append', index=False)
    print('Se ingirieron {} registros en la tabla ships'.format(len(ships)))

# %%
# path = 'data\data_dms_20180101.csv'
lista_path = os.listdir('data')
# Conexion a la base de datos
engine = create_engine_postgresql(config)[0]
# %%
for path in lista_path:
    ingesta_dms('data\\'+path, engine)




# %%



















