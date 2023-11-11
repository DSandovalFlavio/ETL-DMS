import pandas as pd

def carga_arch(path):
    df_raw_data = pd.read_csv(path)
    return df_raw_data

def transforma_arch(df_raw_data):
    print('Se aplican transformaciones al dataframe')
    print(f'que tiene {df_raw_data.shape[0]} filas y {df_raw_data.shape[1]} columnas')

def send_mail():
    print('Se envia el archivo por correo')

def error():
    print('Error en el proceso no continuar con el envio de correo')

def send_mail_arq():
    print('No se encuentra disponible el archivo correspondiente al dia de hoy')

if __name__ == '__main__':
    try:
        df_raw_data = carga_arch('data\data_dms_20180102.csv')
        transforma_arch(df_raw_data)
        send_mail()
    except Exception as e:
        error()
        send_mail_arq()
        print(str(e))
