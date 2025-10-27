import json
import os

CONFIG_FILE = 'config/config.json'

def cargar_configuracion():
    """
    Carga el archivo de configuración JSON.
    Devuelve un diccionario con la configuración o lanza un error si no se encuentra.
    """
    if not os.path.exists(CONFIG_FILE):
        raise FileNotFoundError(f"El archivo de configuración no se encontró en: {CONFIG_FILE}")
    
    with open(CONFIG_FILE, 'r') as f:
        config = json.load(f)
    return config

def obtener_ruta(nombre_ruta):
    """
    Obtiene una ruta específica del archivo de configuración.
    Ej: obtener_ruta('ruta_excel_bruto')
    """
    config = cargar_configuracion()
    path = config.get(nombre_ruta)
    
    if not path:
        raise KeyError(f"La clave de ruta '{nombre_ruta}' no se encontró en el config.json")
    
    # Aseguramos que la ruta exista si es para guardar archivos
    if 'limpio' in nombre_ruta or 'salida' in nombre_ruta:
        os.makedirs(path, exist_ok=True)
        
    return path