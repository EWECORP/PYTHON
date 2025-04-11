"""
Nombre del módulo: S10_GENERA_Foregast_Algoritmos.py

Descripción:   FUNCIONES OPTIMIZADA PARA EL PRONOSTICO DE DEMANDA
Esta función articula y ejecuta los algoritmos definidos en la pantalla de CONNEXA.
Se ejecuta en forma programada cada vez que se requiere generar un pronóstico de demanda.
Parte de los FORECAST_executIONS de estado 10
Genera toda la base de los datos que se utilizarán en el resto del proceso.

Todos los algoritmos see encuentran definidos en la libreria central funciones_forecast.py

Autor: EWE - Zeetrex
Fecha de creación: [2025-05-23]
"""
import pandas as pd

# Solo importa lo necesario desde el módulo de funciones
from funciones_forecast import (
    get_forecast,
    generar_datos,
    Procesar_ALGO_01,
    Procesar_ALGO_02,
    Procesar_ALGO_03,
    Procesar_ALGO_04,
    Procesar_ALGO_05,
    Procesar_ALGO_06,    
    generar_datos,    
    get_execution_execute_by_status,
    get_full_parameters,
    update_execution,
    update_execution_execute
)

# FUNCIONES LOCALES
# RUTINA PRINCIPAL para obtener el pronóstico
def get_forecast( id_proveedor, lbl_proveedor, period_lengh=30, algorithm='basic', f1=None, f2=None, f3=None, current_date=None ):
    """
    Genera la predicción de demanda según el algoritmo seleccionado.

    Parámetros:
    - id_proveedor: ID del proveedor.
    - lbl_proveedor: Etiqueta del proveedor.
    - period_lengh: Número de días del período a analizar (por defecto 30).
    - algorithm: Algoritmo a utilizar.
    - current_date: Fecha de referencia; si es None, se toma la fecha máxima de los datos.
    - factores de ponderación: F1, F2, F3  (No importa en que unidades estén, luego los hace relativos al total del peso)

    Retorna:
    - Un DataFrame con las predicciones.
    """
    
    print('Dentro del get_forecast')
    print(f'FORECAST control: {id_proveedor} - {lbl_proveedor} - ventana: {period_lengh} - {algorithm} factores: {f1} - {f2} - {f3}')
    # Generar los datos de entrada
    data, articulos = generar_datos(id_proveedor, lbl_proveedor, period_lengh)

    # Determinar la fecha base
    if current_date is None:
        current_date = data['Fecha'].max()  # Se toma la última fecha en los datos
    else:
        current_date = pd.to_datetime(current_date)  # Se asegura que sea un objeto datetime
    print(f'Fecha actual {current_date}')
    

    # Selección del algoritmo de predicción
    match algorithm:
        case 'ALGO_01':
            return Procesar_ALGO_01(data, id_proveedor, lbl_proveedor, period_lengh, current_date, f1, f2, f3)  # Promedio Ponderado x 3 Factores
        case 'ALGO_02':
            return Procesar_ALGO_02(data, id_proveedor, lbl_proveedor, period_lengh, current_date) # Doble Exponencial - Modelo Holt (Tendencia)
        case 'ALGO_03':
            return Procesar_ALGO_03(data, id_proveedor, lbl_proveedor, period_lengh, current_date, f1, f2, f3) # Triple Exponencial Holt-WInter (Tendencia + Estacionalidad) (periodos, add, add)
        case 'ALGO_04':
            return Procesar_ALGO_04(data, id_proveedor, lbl_proveedor, period_lengh, current_date, f1) # EWMA con Factor alpha
        case 'ALGO_05':
            return Procesar_ALGO_05(data, id_proveedor, lbl_proveedor, period_lengh, current_date) # Promedio Venta Simple en Ventana
        case 'ALGO_06':
            return Procesar_ALGO_06(data, id_proveedor, lbl_proveedor, period_lengh, current_date) # Tendencias Ventas Semanales
        case _:
            raise ValueError(f"Error: El algoritmo '{algorithm}' no está implementado.")

#----------------------------------------------------------------
# RUTINA PRINCIPAL
#----------------------------------------------------------------       

if __name__ == "__main__":
    # Aquí se inicia la ejecución programada del pronóstico
    print("🕒 Iniciando ejecución programada del FORECAST ...")
    try:
        # Ejecuta la rutina completa
        fes = get_execution_execute_by_status(10)
        for index, row in fes[fes["fee_status_id"] == 10].iterrows():
            algoritmo = row["name"]
            name = algoritmo.split('_ALGO')[0]
            method = row["method"]
            execution_id = row["forecast_execution_id"]
            id_proveedor = row["ext_supplier_code"]
            forecast_execution_execute_id = row["forecast_execution_execute_id"]
            supplier_id = row["supplier_id"]
            supply_forecast_model_id = row["forecast_model_id"]

            print(f"Procesando ejecución: {name} - Método: {method}")

            try:
                df_params = get_full_parameters(supply_forecast_model_id, execution_id) 
                ventana = 30
                f1 = f2 = f3 = None

                try:
                    if df_params is not None and not df_params.empty:
                        if len(df_params) >= 1:
                            ventana = int(float(df_params.iloc[0]['value']))
                        if len(df_params) >= 2:
                            f1 = df_params.iloc[1]['value']
                        if len(df_params) >= 3:
                            f2 = df_params.iloc[2]['value']
                        if len(df_params) >= 4:
                            f3 = df_params.iloc[3]['value']
                except Exception as e:
                    print(f"⚠️ Error interpretando parámetros: {e}")
                    ventana = 30
                    f1 = f2 = f3 = None
                
                update_execution_execute(forecast_execution_execute_id, supply_forecast_execution_status_id=15)
                ## RUTINA PRINCIPAL
                get_forecast(id_proveedor, name, ventana, method, f1, f2, f3)
                
                update_execution_execute(forecast_execution_execute_id, supply_forecast_execution_status_id=20)

                print("✅ Ejecución completada con éxito.")
            except Exception as e:
                print(f"❌ Error durante la ejecución del forecast: {e}")
    except Exception as e:
        print(f"❌ Error general al iniciar ejecuciones programadas: {e}")

