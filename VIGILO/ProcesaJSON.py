import pandas as pd
import json

def procesar_json(file_path):
    # Cargar el archivo JSON
    with open(file_path, 'r') as file:
        data = json.load(file)
    
    # Procesar el primer nivel de datos
    primer_nivel = pd.DataFrame(data['primer_nivel'])
    
    # Procesar el segundo nivel de datos, si existe
    if 'segundo_nivel' in data:
        segundo_nivel = pd.DataFrame(data['segundo_nivel'])
    else:
        segundo_nivel = None
    
    # Procesar el tercer nivel de datos, si existe
    if 'tercer_nivel' in data:
        tercer_nivel = pd.DataFrame(data['tercer_nivel'])
    else:
        tercer_nivel = None
    
    return primer_nivel, segundo_nivel, tercer_nivel

# Ruta al archivo JSON
archivo_json = 'datos.json'

# Procesar el archivo JSON y obtener los DataFrames de pandas
nivel1, nivel2, nivel3 = procesar_json(archivo_json)

# Mostrar los DataFrames resultantes
print("Primer nivel:")
print(nivel1)

if nivel2 is not None:
    print("\nSegundo nivel:")
    print(nivel2)

if nivel3 is not None:
    print("\nTercer nivel:")
    print(nivel3)