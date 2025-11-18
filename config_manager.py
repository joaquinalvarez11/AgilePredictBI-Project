import json
import os

# 1. Obtener la ruta absoluta de este archivo
#    __file__ contiene la ruta del script actual.
script_dir = os.path.dirname(os.path.abspath(__file__))

# 2. Construir la ruta al config.json
#    Como config_manager.py está en el raíz, el config está en "config/config.json"
CONFIG_FILE = os.path.join(script_dir, 'config', 'config.json')

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
    """
    config = cargar_configuracion()
    path = config.get(nombre_ruta)
    
    if not path:
        raise KeyError(f"La clave de ruta '{nombre_ruta}' no se encontró en el config.json")
    
    if not os.path.isabs(path):
        path = os.path.join(script_dir, path)
        
    path = os.path.normpath(path)

    # Aseguramos que la carpeta exista antes de devolver la ruta
    if 'database' in nombre_ruta:
        # Si es un archivo .db, creamos la carpeta que lo contiene
        os.makedirs(os.path.dirname(path), exist_ok=True)
    elif 'limpio' in nombre_ruta or 'predicciones' in nombre_ruta:
        # Si es una ruta de carpeta, la creamos
        os.makedirs(path, exist_ok=True)
    
    return path