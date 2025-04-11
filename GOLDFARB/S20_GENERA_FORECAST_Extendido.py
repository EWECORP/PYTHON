"""
Nombre del módulo: S20_GENERA_Forecast_Extendido.py

Descripción:
Partiendo de los datos generados con estado 20, se adicionan al archivo local todos los datos relevantes que necesitanrán los próximos procesos.
Al finalizar se actualiza el estado a 30 en la base de datos.

Autor: EWE - Zeetrex
Fecha de creación: [2025-03-22]
"""

# Solo importa lo necesario desde el módulo de funciones
from funciones_forecast import (
    Open_Conn_Postgres,
    Close_Connection,
    get_execution_execute_by_status,
    update_execution,
    update_execution_execute
)

import pandas as pd # uso localmente la lectura de archivos.
import ace_tools_open as tools

from dotenv import dotenv_values
secrets = dotenv_values(".env")
folder = secrets["FOLDER_DATOS"]

# También podés importar funciones adicionales si tu módulo las necesita
def extender_datos_forecast(algoritmo, name, id_proveedor):
    # Recuperar Historial de Ventas
    df_ventas = pd.read_csv(f'{folder}/{name}_Ventas.csv')
    df_ventas['Codigo_Articulo']= df_ventas['Codigo_Articulo'].astype(int)
    df_ventas['Sucursal']= df_ventas['Sucursal'].astype(int)
    df_ventas['Fecha']= pd.to_datetime(df_ventas['Fecha'])

    # Recuperar Maestro de Artículos
    articulos = pd.read_csv(f'{folder}/{name}_Articulos.csv')

    # Recuperando Forecast Calculado
    df_forecast = pd.read_csv(f'{folder}/{algoritmo}_Solicitudes_Compra.csv')
    df_forecast.fillna(0)   # Por si se filtró algún missing value
    print(f"-> Datos Recuperados del CACHE: {id_proveedor}, Label: {name}")
    
        # AGREGAR DATOS COMPLEMENTARIOS para Subir a CONNEXA
    conn =Open_Conn_Postgres()
    query = """
    SELECT code, name, id FROM public.fnd_site
    ORDER BY code 
    """
    # Ejecutar la consulta SQL
    stores = pd.read_sql(query, conn)
    # Intentar convertir el campo 'code' a int, eliminando las filas que no se puedan convertir
    stores = stores[pd.to_numeric(stores['code'], errors='coerce').notna()].copy()
    stores['code'] = stores['code'].astype(int)

    # Leer Dataframe de los PRODUCTOS
    query = """
    SELECT ext_code, description, id FROM public.fnd_product
    ORDER BY ext_code;
    """
    # Ejecutar la consulta SQL
    products = pd.read_sql(query, conn)
    # Intentar convertir el campo 'code' a int, eliminando las filas que no se puedan convertir
    products = products[pd.to_numeric(products['ext_code'], errors='coerce').notna()].copy()
    products['ext_code'] = products['ext_code'].astype(int)
    
    Close_Connection(conn)

    #Unir los dataframes por Codigo_Articulo = ext_code
    df_merged = df_forecast.merge(products, left_on='Codigo_Articulo', right_on='ext_code', how='left')
    df_merged.rename(columns={'id': 'product_id'}, inplace=True)
    df_merged.drop(columns=['ext_code','description'], inplace=True)

    df_merged = df_merged.merge(stores, left_on = 'Sucursal', right_on='code', how='left')
    df_merged.rename(columns={'id': 'site_id'}, inplace=True)
    df_merged.drop(columns=['code','name'], inplace=True)
    
    # SUBIR INFORMACIÓN DE ARTICULOS y ESTIDISTICA REPOSICIÓN
    # Seleccionar las columnas requeridas en un nuevo dataframe  FALTA ,I_COSTO_ESTADISTICO,I_PRECIO_VTA
    columnas_seleccionadas = [
        'C_PROVEEDOR_PRIMARIO', 'C_ARTICULO', 'C_SUCU_EMPR', 'I_PRECIO_VTA', 'I_COSTO_ESTADISTICO', 'Q_FACTOR_VTA_SUCU',
        'Q_STOCK_UNIDADES', 'Q_STOCK_PESO', 
        #'Q_BULTOS_PENDIENTE_OC', 'Q_PESO_PENDIENTE_OC', 'Q_UNID_PESO_PEND_RECEP_TRANSF',  
        #'M_FOLDER','C_CLASIFICACION_COMPRA', 'M_HABILITADO_SUCU',  'M_BAJA', 'Q_VENTA_ACUM_30',
        'F_ULTIMA_VTA', 'Q_VTA_ULTIMOS_15DIAS', 'Q_VTA_ULTIMOS_30DIAS', 'Q_TRANSF_PEND', 'Q_TRANSF_EN_PREP', 
        'C_FAMILIA', 'C_RUBRO',  'Q_DIAS_CON_STOCK', 'M_OFERTA_SUCU',
        'Q_REPONER', 'Q_REPONER_INCLUIDO_SOBRE_STOCK', 'Q_VENTA_DIARIA_NORMAL', 'Q_DIAS_STOCK', 'Q_DIAS_SOBRE_STOCK', 
        'Q_DIAS_ENTREGA_PROVEEDOR'
    ]
    
    # ['Q_BULTOS_PENDIENTE_OC', 'Q_PESO_PENDIENTE_OC', 'Q_UNID_PESO_PEND_RECEP_TRANSF', 'Q_STOCK_UNIDADES', 'Q_STOCK_PESO',
    # 'M_FOLDER', 'C_CLASIFICACION_COMPRA', 'Q_VENTA_ACUM_30']

    # Filtrar el dataframe con las columnas seleccionadas
    df_nuevo = articulos[columnas_seleccionadas].copy()
    df_nuevo['C_SUCU_EMPR']= df_nuevo['C_SUCU_EMPR'].astype(int)
    
    # Realizar la fusión de los DataFrames utilizando 'Sucursal' y 'Codigo_Articulo' como claves
    df_merged = df_merged.merge(
        df_nuevo, 
        left_on=['Sucursal', 'Codigo_Articulo'], 
        right_on=['C_SUCU_EMPR', 'C_ARTICULO'], 
        how='left'
    )
    df_merged.drop(columns=['C_SUCU_EMPR','C_ARTICULO'], inplace=True)
    
    return df_merged

# Punto de entrada
if __name__ == "__main__":
    fes = get_execution_execute_by_status(20)

    # Filtrar registros con supply_forecast_execution_status_id = 20  #FORECAST OK
    for index, row in fes[fes["fee_status_id"] == 20].iterrows():
        algoritmo = row["name"]
        name = algoritmo.split('_ALGO')[0]
        execution_id = row["forecast_execution_id"]
        id_proveedor = row["ext_supplier_code"]
        forecast_execution_execute_id = row["forecast_execution_execute_id"]
        print("Algoritmo: " + algoritmo + "  - Name: " + name + " exce_id:" + str(execution_id) + " id: Proveedor "+id_proveedor)
        
        try:
            # Llamar a la función que genera los gráficos y datos extendidos
            df_extendido = extender_datos_forecast(algoritmo, name, id_proveedor)

            # Guardar el archivo CSV
            file_path = f"{folder}/{algoritmo}_Pronostico_Extendido.csv"
            df_extendido.to_csv(file_path, index=False)
            print(f"Archivo guardado: {file_path}")

            # Actualizar el status_id a 40 en el DataFrame original
            fes.at[index, "supply_forecast_execution_status_id"] = 30
            # ✅ Actualizar directamente en la base de datos el estado a 30
            update_execution_execute(forecast_execution_execute_id, supply_forecast_execution_status_id=30)
            print(f"Estado actualizado a 30 para {execution_id}")

        except Exception as e:
            print(f"Error procesando {name}: {e}")

