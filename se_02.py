import sys
import re
import logging
import pypyodbc
import pyodbc
import pandas as pd
from sqlalchemy import create_engine

# Obtener la fecha y hora actual
df_registros = pd.DataFrame()
fecha_actual = pd.Timestamp.now()

# Convertir la variable a una cadena con formato personalizado
cadena_fecha = fecha_actual.strftime('%Y-%m-%d %H:%M:%S')
def eliminar_caracteres_no_alfanumericos(cadena):
    return ''.join(caracter for caracter in cadena if caracter.isalnum())
id_str = eliminar_caracteres_no_alfanumericos(cadena_fecha)

# Datos iniciales 'DataFrame'
data = [['$IDS','ECON_EXPR_DATA_RETURNS_FDS_ECON_BLSCUSR0000SA0', '2023-12-12', 0.16685745, 'Large'],
        ['$IDS','ECON_EXPR_DATA_RETURNS_FDS_ECON_BLSCUSR0000SA0', '2023-07-13', 0.16685745, 'Large'],
        ['$IDS','ECON_EXPR_DATA_RETURNS_FDS_ECON_BLSCUSR0000SA0', '2023-07-18', 0.16685745, 'Large'],
        ['$IDS','ECON_EXPR_DATA_RETURNS_FDS_ECON_US_CPICORE', '2023-07-12', 0.15958548, 'Large'],
        ['$IDS','ECON_EXPR_DATA_RETURNS_FDS_ECON_US_CPICORE', '2023-07-18', 0.15958548, 'Large'],
        ['$IDS','ECON_EXPR_DATA_FDS_ECON_BLSLNS14000000', '2023-07-12', 3.5, 'Large'],
        ['$IDS','ECON_EXPR_DATA_FDS_ECON_BLSLNS14000000', '2023-07-13', 3.5, 'Large'],
        ['$IDS','ECON_EXPR_DATA_FDS_ECON_BLSLNS14000000', '2023-07-14', 3.5, 'Large']]
df = pd.DataFrame(data, columns=['RequestId','Formula','Date','Values','Etiquet'])


# Configurar la bibloteca logging
logging.basicConfig(filename='logfile.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Código para conección a SQL SERVER
conn = pyodbc.connect(r'Driver={SQL Server};Server=LAPTOP-7DKONRCH\SQLJUNIOR;Database=IndicadorEconomico_Diario;Trusted_Connection=yes;')
cursor = conn.cursor()


# Funcion DATAFRAME BITACORA TEMPORAL:
def toBitacoraTemporal(mensaje, id_str, status):
    # Insertar la fecha del sistema y un texto en el DataFrame
    date_actual = pd.Timestamp.now()

    sql_insert = "INSERT INTO Bitacora_Errores (id_log, fecha, status, mensaje) VALUES (?, ?, ?, ?)"
    cursor.execute(sql_insert, (id_str, date_actual, status, mensaje))
    conn.commit()


try:
    # Insertar Dataframe into SQL SERVER
    mensaje_inicio = 'Iniciando inserción de los datos del DataFrame hacia la Tabla Temporal' 
    logging.info(mensaje_inicio)
    toBitacoraTemporal(mensaje_inicio, id_str,'INFO')
    
    # Código Insert Dataframe into SQL Server:
    cursor.execute('TRUNCATE TABLE IndicadorEconomico_Diario_Temporal')
    conn.commit()

    for index, row in df.iterrows():
        cursor.execute("INSERT INTO IndicadorEconomico_Diario_Temporal (RequestId, Formula, Date, [Values], Etiquet) values(?,?,?,?,?)", row.RequestId, row.Formula, row.Date, row.Values, row.Etiquet)
    conn.commit()

    mensaje_exito = 'Finalizado con exito inserción de los datos del DataFrame hacia la Tabla Temporal' 
    logging.info(mensaje_exito)
    toBitacoraTemporal(mensaje_exito, id_str,'INFO')

except Exception as e:
    mensaje_error = 'Error al insertar datos del DataFrame hacia la Tabla Temporal'
    logging.error(f'{mensaje_error}: %s', e)
    toBitacoraTemporal(mensaje_error, id_str,'ERROR')



try:
    # Insertar Dataframe into SQL SERVER
    mensaje_exito = 'Iniciando procedimiento almacenado de Tabla Temporal a Tabla Destino' 
    logging.info(mensaje_exito)
    toBitacoraTemporal(mensaje_exito, id_str,'INFO')

    # Ejecutar procedimiento almacenado:
    cursor.execute('EXEC SpUpsert')
    conn.commit()

    mensaje_exito = 'Finalizado con exito procedimiento almacenado de Tabla Temporal a Tabla Destino' 
    logging.info(mensaje_exito)
    toBitacoraTemporal(mensaje_exito, id_str,'INFO')

except Exception as e:
    mensaje_error = 'Error al ejecutar procedimiento almacenado de Tabla Temporal a Tabla Destino'
    logging.error(f'{mensaje_error}: %s', e)
    toBitacoraTemporal(mensaje_error, id_str,'ERROR')
