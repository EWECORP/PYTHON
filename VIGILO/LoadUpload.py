import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime
import json

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

def procesar_entregas_desde_url(url, modo='imprimir'):
    # Descargar el contenido del archivo JSON desde la URL
    respuesta = requests.get(url)
    respuesta.raise_for_status()  # Esto lanzará una excepción si ocurre un error

    # Leer el contenido JSON
    datos = respuesta.json()    
    resultados = []
    
    # Leer Datos de la Cabecera    
    resultado = f"\nOrigen: {datos['LogisticGroupCode']},  Embarque: {datos['Id']}, Date: {datos['Date']}, Carrier: {datos['CarrierName']}"
    resultados.append(resultado)
    # Imprimir la información si el modo es 'imprimir'
    if modo == 'imprimir':
        print(resultado)

    # Procesar y almacenar la información en una lista
    for stop in datos['StopList']:
        # Verificar que la parada tenga entregas
        if stop['OperationType'] == 'Dropped' and stop.get('Deliveries'):
            for delivery in stop['Deliveries']:
                delivery_number = delivery['DeliveryNumber']
                location = stop['LocationName']
                for item in delivery['Items']:
                    material_name = item['MaterialName']
                    batch = item.get('Batch', 'N/A')
                    qty = item['Qty']
                    
                    resultado = f"  Delivery: {delivery_number}, Location: {location}, Material: {material_name}, Batch: {batch}, Qty: {qty}"
                    resultados.append(resultado)

                    # Imprimir la información si el modo es 'imprimir'
                    if modo == 'imprimir':
                        print(resultado)
    
    # Devolver los resultados si el modo es 'devolver'
    if modo == 'devolver':
        return resultados

    # Almacenar los resultados en un archivo si el modo es 'almacenar'
    if modo == 'almacenar':
        with open('resultados.txt', 'a') as archivo_salida:
            for resultado in resultados:
                archivo_salida.write(resultado + '\n')


########################################################################
# RUTINA PRINCIPAL 
########################################################################

# URL de la página de donde se desean extraer los enlaces
url = "http://rtv-b2b-api-logs.vigiloo.net/LoadUpload/"
archivos_df = obtener_enlaces(url)
if isinstance(archivos_df, pd.DataFrame):
    #print(archivos_df.head(1))
    print('===============================================================')
    print('INICIANDO EL ANÁLISIS')

else:
    print('... Error....')

# FILTRAR POR CANTIDAD Tomar las últimas 50 fílas válidas
# salida= archivos_df.tail(51)
# salida.drop(salida.index[-1], axis=0, inplace = True) # Elimina la última fila a apunta a backup

# FILTRAR POR FECHA DE REGISTRO MAYOR A
entrada = archivos_df[(archivos_df['FechaHora'] >=datetime.datetime(year=2024,month=5,day=15))]  # Toma Registros desde esa fecha en adelante

# Extraer valores únicos de la columna 'Embarque'
embarques = entrada['Embarque'].unique()
# Formatear los valores entre apóstrofes y separados por comas
REGISTROS = ', '.join(f"'{embarque}'" for embarque in embarques)

# PARAMETROS MANUALES
REGISTROS=('834277796', '834277800', '834277808', '834277810', '834277840', '834277842', '834277843', '834277885', '834278471', '834278475', '834278476', '834278484', '834278501', '834278516')
print(REGISTROS)
print('===============================================================')

# ORDENAR y FILTRAR ENTRADAS
resumen = entrada.loc[entrada["Embarque"].isin(REGISTROS)]
salida = resumen.sort_values(by=['Embarque', 'FechaHora'])

# Recorrer el Dataframe Salida para analizar los Embarques
for i in range(len(salida)):
    if salida.iloc[i]['Embarque'] in (REGISTROS):
        print('----------------------------------------------------------------')
        print('Embarque: ',salida.iloc[i]['Embarque'])
        url = salida.iloc[i]['URL']
        print('Archivo: ',salida.iloc[i]['FechaHora'],url)
        
        # Procesar cada Embarque
        resultado = procesar_entregas_desde_url(url, modo='imprimir')
        
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
    
WHERE A.[RefId] IN {REGISTROS} 
ORDER BY A.RefId,D.RefId, D.[DestinationCode],E.[MaterialCode];
"""

#print(SQL_QUERY)
print('\n DATOS EXISTENTES EN LA BASE DE DATOS ********************************\n')
print(SQL_QUERY)
print('\n *********************************************************************\n')
cursor = conn.cursor()
cursor.execute(SQL_QUERY)
records = cursor.fetchall()

#Cargar Datos en el Dataframe
df = pd.DataFrame((tuple(t) for t in records)) 
df.columns = ['EMBARQUE','DELIVERY','FECHA','PEDIDO','DestinationCode','DestinationName','MaterialCode','MaterialName','Batch','QTY','LogisticGroupId']

# Adecuar el Grupo Logístico
df["LogisticGroupId"] = df["LogisticGroupId"].replace({ "75247427-34FD-41A3-B7E0-F0B8312EADA8": "Monsanto", "F21EE1F9-4823-42B4-AD40-8C81F2847818": "BAYER" })
print(df.head(10))