import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime
import json

# PARAMETROS
REGISTROS=('834261368', '834261373', '834261712')
REGQRY="""
'834261368',
'834261373',
'834261712'
"""

# DEFINIR FUNCIONES A UTILIZAR
def obtener_enlaces(url):
    # Realizar la petición HTTP a la página web
    respuesta = requests.get(url)
    # Si la petición fue exitosa, procesar el contenido
    if respuesta.status_code == 200:
        soup = BeautifulSoup(respuesta.content, 'html.parser')
        # Extraer todos los elementos <a> de la página
        enlaces = soup.find_all('a', href=True)
        # Crear una lista para almacenar los datos
        datos = []
        # Recorrer cada enlace y almacenar la información necesaria
        for enlace in enlaces:
            # Extraer el href del enlace
            href = enlace['href']
            # Extraer el texto del enlace, que es el nombre del archivo
            texto = enlace.text
            # Intentar extraer las observaciones que están en el contenido antes del enlace
            # Esto asume que el texto de observaciones está justo antes del enlace
            observaciones = enlace.previous_sibling
            if observaciones:
                observaciones = observaciones.strip()  #Remueve los espacios al principio y al final de la línea
                # Dividir las observaciones en partes
                partes = observaciones.split("   ")
                #print(partes)
                # Extraer la fecha y la hora (primeras dos partes)
                fecha_hora = ' '.join(partes[:1])
                #print(fecha_hora)
                # Extraer el tamaño (última parte)
                tamaño = partes[-1]
                #print(tamaño)
            else:
                fecha_hora = "No disponible"
                tamaño = "No disponible"
            # Añadir a la lista como una tupla
            datos.append((texto, "http://rtv-b2b-api-logs.vigiloo.net" + href, fecha_hora, tamaño))    # datos.append((texto, url + href, fecha_hora, tamaño))
        # Convertir la lista en un DataFrame
        df = pd.DataFrame(datos, columns=['NombreArchivo', 'URL', 'FechaHora', 'Tamaño'])
        df.drop(0,axis=0)
        # Convertir la columna 'FechaHora' a tipo datetime
        df['FechaHora'] = pd.to_datetime(df['FechaHora'], dayfirst=False, format='%m/%d/%Y %I:%M %p',errors='coerce') 
        # Convertir la columna 'Tamaño' a tipo numérico
        df['Tamaño'] = pd.to_numeric(df['Tamaño'], errors='coerce')
        df['Embarque'] = df['NombreArchivo'].str.extract(r'-(\d+)\.log')
        df.drop(0,axis=0, inplace=True)  # Borro la Primera línea que apunta al directorio padre
        
        return df
    else:
        return "Error al acceder a la página"

def process_deliveries(row):
    if row['DroppedNumber'] > 0 and pd.notna(row['Deliveries']):
        location = row['LocationName']
        # Convertir el contenido de la columna 'Deliveries' desde JSON a objeto Python si aún no está hecho
        if isinstance(row['Deliveries'], str):
            deliveries = eval(row['Deliveries'])
        else:
            deliveries = row['Deliveries']
        
        # Recorrer cada entrega en 'Deliveries'
        for delivery in deliveries:
            delivery_number = delivery['DeliveryNumber']            
            # Recorrer cada ítem en 'Items' de cada entrega
            for item in delivery['Items']:
                material_name = item['MaterialName']
                batch = item['Batch']
                qty = item['Qty']
                # Podemos elegir imprimir, almacenar o devolver esta información
                print(f"  DeliveryNumber: {delivery_number}, LocationName: {location}, MaterialName: {material_name}, Batch: {batch}, Qty: {qty}")

# Defino la function que hace un parse al JSON string y retorna un dictionary
def parse_json(x):
    try:
        return json.dumps(x)
    except:
        return 'Error'

########################################################################
# RUTINA PRINCIPAL 
########################################################################

# URL de la página de donde se desean extraer los enlaces
url = "http://rtv-b2b-api-logs.vigiloo.net/LoadUpload/"
archivos_df = obtener_enlaces(url)
if isinstance(archivos_df, pd.DataFrame):
    print(archivos_df.head(1))
else:
    print(archivos_df)

# FILTRAR POR CANTIDAD Tomar las últimas 50 fílas válidas
# salida= archivos_df.tail(51)
# salida.drop(salida.index[-1], axis=0, inplace = True) # Elimina la última fila a apunta a backup

# FILTRAR POR FECHA DE REGISTRO MAYOR A
salida = archivos_df[(archivos_df['FechaHora'] >=datetime.datetime(year=2024,month=5,day=1))]
salida.drop(salida.index[-1], axis=0, inplace = True) # Elimina la última fila a apunta a backup

# Convertir la columna 'FechaHora' a tipo datetime
# salida['FechaHora'] = pd.to_datetime(salida['FechaHora'], dayfirst=False, format='%m/%d/%Y %I:%M %p')
# salida['FechaHora'] = pd.to_datetime(salida['FechaHora'], dayfirst=False).dt.strftime('%m/%d/%Y %I:%M %p')


REGISTROS=('834261368', '834261373', '834261712')
print('\n===============================================================\n')
# Recorrer el Dataframe Salida para analizar los Embarques
for i in range(len(salida)):
    if salida.iloc[i]['Embarque'] in (REGISTROS):
        print('----------------------------------------------------------------')
        print('Embarque: ',salida.iloc[i]['Embarque'])
        url = salida.iloc[i]['URL']
        print('Archivo: ',salida.iloc[i]['FechaHora'],url)
        # Procesar cada Embarque
        embarque = pd.read_json(url)
        embarque['json_dict'] = embarque['StopList'].apply(parse_json)
        df_json = pd.json_normalize(embarque['StopList'])
        embarque = pd.concat([embarque, df_json], axis=1)
        print('Origen: ',embarque.iloc[0]['LogisticGroupCode'],' - Date: ',embarque.iloc[0]['Date'],' - Carrier: ',embarque.iloc[0]['CarrierName'])
        embarque.apply(process_deliveries, axis=1)   # Procesar detalles de cada embarque
        
# Controla los Registros efectivamente ingresados en la BASE de DATOS
import pyodbc

SERVER = '213.134.40.73,9595'
DATABASE = 'seamtrack'
USERNAME = 'eduardo.ettlin@vigiloo.com.ar'
PASSWORD = 'Aladelta10$'
OPTIONS= 'encrypt=no'

connectionString = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD};{OPTIONS}'
conn = pyodbc.connect(connectionString) 

SQL_QUERY = f"""
SELECT   DISTINCT     
A.[RefId] as EMBARQUE
,D.RefId as DELIVERY
,A.[Date] as FECHA
,D.[PurchaseOrderIdRef] as PEDIDO
,D.[DestinationCode]
,D.[DestinationName]
,E.[MaterialCode]
,E.[MaterialName]
,E.[Batch]
,STR(E.[Qty],6) as QTY
,A.[LogisticGroupId]

FROM [seamtrack].[vigiloo].[Shipment] A
LEFT JOIN [seamtrack].[vigiloo].[ShipmentDestination] B
ON B.[ShipmentId] = A.Id
LEFT JOIN [seamtrack].[vigiloo].[Delivery] D
ON D.[ShipmentId] = A.Id
LEFT JOIN [seamtrack].[vigiloo].[DeliveryItem] E
ON E.[DeliveryId] = D.Id
    
WHERE A.[RefId] IN ({REGQRY}) ORDER BY A.RefId,D.RefId, D.[DestinationCode],E.[MaterialCode];
"""
#print(SQL_QUERY)
print('\n DATOS EXISTENTES EN LA BASE DE DATOS ********************************')
cursor = conn.cursor()
cursor.execute(SQL_QUERY)
records = cursor.fetchall()

#Cargar Datos en el Dataframe
df = pd.DataFrame((tuple(t) for t in records)) 
df.columns = ['EMBARQUE','DELIVERY','FECHA','PEDIDO','DestinationCode','DestinationName','MaterialCode','MaterialName','Batch','QTY','LogisticGroupId']

# Adecuar el Grupo Logístico
df["LogisticGroupId"] = df["LogisticGroupId"].replace({ "75247427-34FD-41A3-B7E0-F0B8312EADA8": "Monsanto", "F21EE1F9-4823-42B4-AD40-8C81F2847818": "BAYER" })
print(df.head(10))