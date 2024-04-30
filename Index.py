import logging
import os
import oracledb # puedes usar oracle_cx
import pandas as pd
import polars as pl
from dotenv import load_dotenv
from src.database.db_oracle import close_connection_db,read_database_db,leer_sql,get_connection,Insert_dataframe_db
from src.routes.Rutas import ruta_env,ruta_html,ruta_Refacturaciones_Corporativas
from src.models.Fun_Excel import Macros,Eliminar_Excel,leer_html,enviar_correo
from openpyxl import load_workbook
import datetime
import locale


logging.basicConfig(format="%(asctime)s::%(levelname)s::%(message)s",   
                    datefmt="%d-%m-%Y %H:%M:%S",    
                    level=10,   
                    filename='.//src//utils//log//app.log',filemode='a')


load_dotenv(ruta_env)

Conexion_Opercom=get_connection(os.getenv('USER_DB'),os.getenv('PASSWORD_DB'),os.getenv('DNS_DB'))

# Establecer el idioma a español
locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')
# Obtener el nombre del día actual en español
nombre_dia = datetime.datetime.now().strftime('%A').capitalize()
nombre_mes = datetime.datetime.now().strftime('%B').capitalize() 
from datetime import datetime, timedelta

 
# Obtén la fecha actual
fecha_actual = datetime.now()

# Formatea la fecha con ceros si es de un solo dígito
año = fecha_actual.strftime('%Y')
mes = fecha_actual.strftime('%m')
dia = fecha_actual.strftime('%d')

fecha_ayer = fecha_actual - timedelta(days=1)
 
# Formatea la fecha con ceros si es de un solo dígito
año = fecha_ayer.strftime('%Y')
mes = fecha_ayer.strftime('%m')
dia = fecha_ayer.strftime('%d')
#print(f"Año: {año}, Mes: {mes}, Día: {dia}")
fecha_anteayer = fecha_actual - timedelta(days=2)
año_1 = fecha_anteayer.strftime('%Y')
mes_1 = fecha_anteayer.strftime('%m')
dia_1 = fecha_anteayer.strftime('%d')

#mes actual
mes_corto=fecha_ayer.strftime('%h')[:-1]
año_corto=fecha_ayer.strftime('%g')

#mes actual menos 1
Fecha_1=fecha_ayer.replace(day=1)-timedelta(days=1)
mes_corto_1=Fecha_1.strftime('%h')[:-1]
año_corto_1=Fecha_1.strftime('%g')
#mes actual menos 2
Fecha_2=Fecha_1.replace(day=1)-timedelta(days=1)
mes_corto_2=Fecha_2.strftime('%h')[:-1]
año_corto_2=Fecha_2.strftime('%g')
#mes actual menos 3
Fecha_3=Fecha_2.replace(day=1)-timedelta(days=1)
mes_corto_3=Fecha_3.strftime('%h')[:-1]
año_corto_3=Fecha_3.strftime('%g')
#mes actual menos 4
Fecha_4=Fecha_3.replace(day=1)-timedelta(days=1)
mes_corto_4=Fecha_4.strftime('%h')[:-1]
año_corto_4=Fecha_4.strftime('%g')
#mes actual menos 5
Fecha_5=Fecha_4.replace(day=1)-timedelta(days=1)
mes_corto_5=Fecha_5.strftime('%h')[:-1]
año_corto_5=Fecha_5.strftime('%g')
#mes actual menos 6
Fecha_6=Fecha_5.replace(day=1)-timedelta(days=1)
mes_corto_6=Fecha_6.strftime('%h')[:-1]
año_corto_6=Fecha_6.strftime('%g')
#mes actual menos 7
Fecha_7=Fecha_6.replace(day=1)-timedelta(days=1)
mes_corto_7=Fecha_7.strftime('%h')[:-1]
año_corto_7=Fecha_7.strftime('%g')
#mes actual menos 8
Fecha_8=Fecha_7.replace(day=1)-timedelta(days=1)
mes_corto_8=Fecha_8.strftime('%h')[:-1]
año_corto_8=Fecha_8.strftime('%g')
#mes actual menos 9
Fecha_9=Fecha_8.replace(day=1)-timedelta(days=1)
mes_corto_9=Fecha_9.strftime('%h')[:-1]
año_corto_9=Fecha_9.strftime('%g')
#mes actual menos 10
Fecha_10=Fecha_9.replace(day=1)-timedelta(days=1)
mes_corto_10=Fecha_10.strftime('%h')[:-1]
año_corto_10=Fecha_10.strftime('%g')
#mes actual menos 11
Fecha_11=Fecha_10.replace(day=1)-timedelta(days=1)
mes_corto_11=Fecha_11.strftime('%h')[:-1]
año_corto_11=Fecha_11.strftime('%g')
#mes actual menos 12
Fecha_12=Fecha_11.replace(day=1)-timedelta(days=1)
mes_corto_12=Fecha_12.strftime('%h')[:-1]
año_corto_12=Fecha_12.strftime('%g')
#mes actual menos 13
Fecha_13=Fecha_12.replace(day=1)-timedelta(days=1)
mes_corto_13=Fecha_13.strftime('%h')[:-1]
año_corto_13=Fecha_13.strftime('%g')


Mes=mes_corto.capitalize()+'-'+año_corto
Mes_1=mes_corto_1.capitalize()+'-'+año_corto_1
Mes_2=mes_corto_2.capitalize()+'-'+año_corto_2
Mes_3=mes_corto_3.capitalize()+'-'+año_corto_3
Mes_4=mes_corto_4.capitalize()+'-'+año_corto_4
Mes_5=mes_corto_5.capitalize()+'-'+año_corto_5
Mes_6=mes_corto_6.capitalize()+'-'+año_corto_6
Mes_7=mes_corto_7.capitalize()+'-'+año_corto_7
Mes_8=mes_corto_8.capitalize()+'-'+año_corto_8
Mes_9=mes_corto_9.capitalize()+'-'+año_corto_9
Mes_10=mes_corto_10.capitalize()+'-'+año_corto_10
Mes_11=mes_corto_11.capitalize()+'-'+año_corto_11
Mes_12=mes_corto_12.capitalize()+'-'+año_corto_12
Mes_13=mes_corto_13.capitalize()+'-'+año_corto_13


dtypes = {
        "PERIODO":pl.Utf8,
        "DIA":pl.Utf8,
        "ORIGEN":pl.Utf8,
        "TIPO_NC":pl.Utf8,
        "TIPO_NC":pl.Utf8,
        "DIRECCION":pl.Utf8,
        "SEGMENTO":pl.Utf8,
        "SUB_SEGMENTO":pl.Utf8,
        "RUC":pl.Utf8,
        "RAZON_SOCIAL":pl.Utf8,
        "CUENTA":pl.Utf8,
        "CUSTOMER_ID":pl.Utf8,
        "CUENTA_LARGA":pl.Utf8,
        "NRO_NC":pl.Utf8,
        "EMISION":pl.Datetime,
        "MONEDA":pl.Utf8,
        "MONTO_ORIGINAL":pl.Float64,
        "MONTO_APLICADO":pl.Float64,
        "MONTO_APLICADO_SOLES":pl.Float64,
        "REGION":pl.Utf8,
        "DEPARTAMENTO":pl.Utf8,
        "PROVINCIA":pl.Utf8,
        "DISTRITO":pl.Utf8,
        "ESTADO":pl.Utf8,
        "SERVICIO":pl.Utf8,
        "EJECUTIVO":pl.Utf8,
        "SUPERVISOR":pl.Utf8,
        "SECTOR":pl.Utf8,
        "CARTERA":pl.Utf8,
        "COMMENTS":pl.Utf8,
        "DESCRIPTION":pl.Utf8,
        "F_DUMP":pl.Datetime
}

Df_Refacturaciones_Corporativas=read_database_db(leer_sql(ruta_Refacturaciones_Corporativas), Conexion_Opercom,dtypes)
Df_Refacturaciones_Corporativas=Df_Refacturaciones_Corporativas.fill_null("-").to_pandas()
Df_Refacturaciones_Corporativas['PERIODO2'] = Df_Refacturaciones_Corporativas['EMISION'].dt.strftime('%h').str.capitalize().str[:-1]+'-'+ Df_Refacturaciones_Corporativas['EMISION'].dt.strftime('%g')

df_1 = Df_Refacturaciones_Corporativas.pivot_table(index='PERIODO2' ,columns='TIPO_CORP', values='TOTAL',aggfunc='sum').reset_index().fillna(0)
total_columna = Df_Refacturaciones_Corporativas.pivot_table( index='PERIODO2', values='TOTAL',aggfunc='sum').reset_index().fillna(0)
df_1_t=pd.merge(df_1, total_columna,on="PERIODO2")

df_1_t['SEGMENTO1']=df_1_t['SEGMENTO1'].apply(lambda x:'{:,.0f}'.format(x))
df_1_t['SEGMENTO3']=df_1_t['SEGMENTO3'].apply(lambda x:'{:,.0f}'.format(x))
df_1_t['SEGMENTO2']=df_1_t['SEGMENTO2'].apply(lambda x:'{:,.0f}'.format(x))
df_1_t['TOTAL']=df_1_t['TOTAL'].apply(lambda x:'{:,.0f}'.format(x))
df_1_t=df_1_t.T
nuevos_encabezados = df_1_t.iloc[0]
df_1_t = df_1_t[1:]
df_1_t.columns = nuevos_encabezados

#df_1_t['TIPO_CORP'] = ['CARTERIZADO', 'GOBIERNO', 'NO CARTERIZADO','MONTO_APLICADO_SOLES']
df_1_t.reset_index(inplace=True)
df_1_t.rename(columns={'index': 'TIPO_CORP'}, inplace=True)
df_1_t=df_1_t.apply(lambda x: x.astype(str).str.capitalize())




df_2 = Df_Refacturaciones_Corporativas.pivot_table(index='PERIODO2' ,columns='SEGMENTO', values='TOTAL',aggfunc='sum').reset_index().fillna(0)
total_columna = Df_Refacturaciones_Corporativas.pivot_table( index='PERIODO2', values='TOTAL',aggfunc='sum').reset_index().fillna(0)
df_2_t=pd.merge(df_2, total_columna,on="PERIODO2")

order2 = ['PERIODO2','EMPRESAS', 'MAYORES', 'NEGOCIOS','-','TOTAL']
df_2_t=df_2_t[order2]

df_2_t['EMPRESAS']=df_2_t['EMPRESAS'].apply(lambda x:'{:,.0f}'.format(x))
df_2_t['MAYORES']=df_2_t['MAYORES'].apply(lambda x:'{:,.0f}'.format(x))
df_2_t['NEGOCIOS']=df_2_t['NEGOCIOS'].apply(lambda x:'{:,.0f}'.format(x))
df_2_t['-']=df_2_t['-'].apply(lambda x:'{:,.0f}'.format(x))
df_2_t['TOTAL']=df_2_t['TOTAL'].apply(lambda x:'{:,.0f}'.format(x))

df_2_t=df_2_t.T
nuevos_encabezados = df_2_t.iloc[0]
df_2_t = df_2_t[1:]
df_2_t.columns = nuevos_encabezados
df_2_t.reset_index(inplace=True)
df_2_t.rename(columns={'index': 'SEGMENTO'}, inplace=True)
df_2_t=df_2_t.apply(lambda x: x.astype(str).str.capitalize())

df_3 = Df_Refacturaciones_Corporativas.pivot_table(index=['PERIODO2'], columns='REGION', values='TOTAL',aggfunc='sum').reset_index().fillna(0)
order3 = ['PERIODO2','LIMA', 'NORTE', 'SUR','CENTRO']
df_3=df_3[order3]
df_3['CENTRO']=df_3['CENTRO'].apply(lambda x:'{:,.0f}'.format(x))
df_3['LIMA']=df_3['LIMA'].apply(lambda x:'{:,.0f}'.format(x))
df_3['NORTE']=df_3['NORTE'].apply(lambda x:'{:,.0f}'.format(x))
df_3['SUR']=df_3['SUR'].apply(lambda x:'{:,.0f}'.format(x))
df_3=df_3.pivot_table(index=None,columns='PERIODO2',values=['LIMA','CENTRO','NORTE','SUR'],aggfunc='first').reset_index().fillna(0)
df_3=df_3.apply(lambda x: x.astype(str).str.capitalize())


df_4 = Df_Refacturaciones_Corporativas.pivot_table(index=['PERIODO2'], columns='DEPARTAMENTO', values='TOTAL',aggfunc='sum').reset_index().fillna(0)
df_4['LIMA']=df_4['LIMA'].apply(lambda x:'{:,.0f}'.format(x))
df_4['AREQUIPA']=df_4['AREQUIPA'].apply(lambda x:'{:,.0f}'.format(x))
df_4['LA LIBERTAD']=df_4['LA LIBERTAD'].apply(lambda x:'{:,.0f}'.format(x))
df_4['LAMBAYEQUE']=df_4['LAMBAYEQUE'].apply(lambda x:'{:,.0f}'.format(x))
df_4['PIURA']=df_4['PIURA'].apply(lambda x:'{:,.0f}'.format(x))
df_4['CAJAMARCA']=df_4['CAJAMARCA'].apply(lambda x:'{:,.0f}'.format(x))
df_4['MOQUEGUA']=df_4['MOQUEGUA'].apply(lambda x:'{:,.0f}'.format(x))
df_4['APURIMAC']=df_4['APURIMAC'].apply(lambda x:'{:,.0f}'.format(x))
df_4['PUNO']=df_4['PUNO'].apply(lambda x:'{:,.0f}'.format(x))
df_4['CUSCO']=df_4['CUSCO'].apply(lambda x:'{:,.0f}'.format(x))
df_4['JUNIN']=df_4['JUNIN'].apply(lambda x:'{:,.0f}'.format(x))
df_4['TUMBES']=df_4['TUMBES'].apply(lambda x:'{:,.0f}'.format(x))
df_4['ICA']=df_4['ICA'].apply(lambda x:'{:,.0f}'.format(x))
df_4['ANCASH']=df_4['ANCASH'].apply(lambda x:'{:,.0f}'.format(x))
df_4['UCAYALI']=df_4['UCAYALI'].apply(lambda x:'{:,.0f}'.format(x))
df_4['AMAZONAS']=df_4['AMAZONAS'].apply(lambda x:'{:,.0f}'.format(x))
df_4['SAN MARTIN']=df_4['SAN MARTIN'].apply(lambda x:'{:,.0f}'.format(x))
df_4['TACNA']=df_4['TACNA'].apply(lambda x:'{:,.0f}'.format(x))
df_4['AYACUCHO']=df_4['AYACUCHO'].apply(lambda x:'{:,.0f}'.format(x))
df_4['LORETO']=df_4['LORETO'].apply(lambda x:'{:,.0f}'.format(x))
df_4['HUANCAVELICA']=df_4['HUANCAVELICA'].apply(lambda x:'{:,.0f}'.format(x))
df_4['MADRE DE DIOS']=df_4['MADRE DE DIOS'].apply(lambda x:'{:,.0f}'.format(x))
df_4['HUANUCO']=df_4['HUANUCO'].apply(lambda x:'{:,.0f}'.format(x))
df_4['PASCO']=df_4['PASCO'].apply(lambda x:'{:,.0f}'.format(x))
df_4['CALLAO']=df_4['CALLAO'].apply(lambda x:'{:,.0f}'.format(x))

df_4=df_4.pivot_table(index=None,columns='PERIODO2',values=['LIMA','AREQUIPA','LA LIBERTAD','LAMBAYEQUE','PIURA','CAJAMARCA','MOQUEGUA','APURIMAC','PUNO','CUSCO','JUNIN','TUMBES',
                                                           'ICA','ANCASH','UCAYALI','AMAZONAS','SAN MARTIN','TACNA','AYACUCHO','LORETO','HUANCAVELICA',
                                                           'MADRE DE DIOS','HUANUCO','PASCO','CALLAO'],aggfunc='first').reset_index().fillna(0)
df_4=df_4.apply(lambda x: x.astype(str).str.capitalize())


df_5 = Df_Refacturaciones_Corporativas.loc[(Df_Refacturaciones_Corporativas['PERIODO'] == max(Df_Refacturaciones_Corporativas['PERIODO']))]
df_5 = df_5.pivot_table(index=['TIPO_CORP','ORIGEN','RUC','RAZON_SOCIAL','EJECUTIVO','SEGMENTO'],aggfunc={'TOTAL': 'sum',   # Suma de los valores
                                          'NRO_NC': 'nunique'} ).reset_index().fillna(0)
df_5 = df_5.groupby('TIPO_CORP').apply(lambda x: x.nlargest(5, 'TOTAL')).reset_index(drop=True)
df_5['TOTAL']=df_5['TOTAL'].apply(lambda x:'{:,.0f}'.format(x))
df_5=df_5.apply(lambda x: x.astype(str).str.capitalize())



Ruta_libro = "./src/models/Reporte Refacturaciones Corportivas al "+dia+"."+mes+"."+año+".xlsx"  # Reemplaza con la ruta y nombre de tu archivo Excel

html=leer_html(ruta_html,df_1_t,df_2_t,df_3,df_4,df_5,Mes,Mes_1,Mes_2,Mes_3,Mes_4,Mes_5,Mes_6,Mes_7,Mes_8,Mes_9,Mes_10,Mes_11,Mes_12)

Name_File_1 = "Reporte Refacturaciones Corporativas al "+dia_1+"."+mes_1+"."+año_1+""  # Reemplaza con la ruta y nombre de tu archivo Excel


Eliminar_Excel(f'{Name_File_1}.html')
Eliminar_Excel(f'{Name_File_1}.pdf')

Name_File = "Reporte Refacturaciones Corporativas al "+dia+"."+mes+"."+año+""  # Reemplaza con la ruta y nombre de tu archivo Excel

with open(f'./{Name_File}.html', 'w') as f:
    f.write(html)

import pdfkit
 
path_to='D:\\wkhtmltopdf\\bin\\wkhtmltopdf.exe'
path_file = f'{Name_File}.html'
config=pdfkit.configuration(wkhtmltopdf=path_to)
 
pdf_options = {
    'page-size': 'Letter',
    'margin-top': '0in',
    'margin-right': '0in',
    'margin-bottom': '0in',
    'margin-left': '0in'
}

pdfkit.from_file(path_file,output_path=f'./{Name_File}.pdf',options=pdf_options,configuration=config)

enviar_correo(html)
close_connection_db(Conexion_Opercom)

