# Logger
import logging
import requests
#import cx_Oracle #puedes usar oracledb
import oracledb
import polars as pl

#funcion para conectarme  base de datos
def get_connection(user_db,password_db,dsn_db):
    logging.info(f'Iniciando proceso de conexion a la base de datos {dsn_db}')
    try:
        conexion= oracledb.connect(
            user=user_db,
            password=password_db,
            dsn=dsn_db
        )
        logging.info(f'Conexion exitosa a la base de datos {dsn_db}')
        return conexion
    except Exception as ex:
        logging.error(ex)

#funcion para cerrar la conexion a base de datos
def close_connection_db(conexion):
    logging.info('Iniciando proceso para cerrar conexion a base de datos')
    try:
        cierre_conexion= conexion.close()
        logging.info('Se cerro conexion de manera exitosa')
        return cierre_conexion
    except Exception as ex:
        logging.error(ex)


def read_database_db(sql_query,source_connection,dtypes):
        logging.info('Iniciando proceso para guardar la informacion en un dataframe')
        df_polars = pl.read_database(sql_query, source_connection,batch_size=0,schema_overrides=dtypes)
        logging.info('Se guardo la informacion en un dataframe')
        return df_polars

def leer_sql(archivo_sql):
    logging.info('Se inicia la funcion de leer contenido de archivo sql')
    with open(archivo_sql, 'r',encoding='utf-8') as archivo:
        x=archivo.read()
        logging.info('Se ha leido todo el contenido del archivo sql')
        return x

def Insert_dataframe_db(target_connection,df_polars,Insertar_Query):
    logging.info('Se inicia el proceso de ingresar dataframe a la tabla ')
    target_cursor = target_connection.cursor()
    logging.info('Se crea el cursor')
    datos_insertar = [tuple(row) for row in df_polars.to_numpy()]
    logging.info('Se convierte en tuplas el dataframe')
    start_pos = 0
    batch_size = 15000
    all_data = datos_insertar
    while start_pos < len(all_data):
        data = all_data[start_pos:start_pos + batch_size]
        start_pos += batch_size
        target_cursor.executemany(Insertar_Query, data)
    logging.info('Se ingreso dataframe en tabla ')
    target_connection.commit()
    logging.info('Se confirma dichos cambios a la tabla ')
    target_cursor.close
    logging.info('Se cierra el cursor')


def ejecutar_consultas(archivo_sql, conexion):
    logging.info('Se inicia la funcion de ejecutar consultas largas')
    try:
        
        with open(archivo_sql, 'r',encoding='utf-8') as archivo:
            consultas_sql = archivo.read().split(';')
        logging.info('Se abrio y se ha leido archivo sql')
        cursor = conexion.cursor()
        logging.info('Se crea cursor')
        for consulta in consultas_sql:
            if consulta.strip():  # Para evitar consultas vacías al final del archivo
                try:
                    cursor.execute(consulta)
                    logging.info(f'Se ejecuto correctamento la consulta')
                except oracledb.DatabaseError as error:
                    logging.info(f'Error al ejecutar el codigo {consulta}{error}')
        logging.info('Se ejecuto toda la consulta del archivo sql')
        conexion.commit()
        logging.info('Se confirma dichos cambios a la tabla')
        cursor.close()
        logging.info('Se cierra el cursor')
    except Exception as e:
        logging.error(e)
