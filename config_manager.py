import json
import os
import sys

# 1. Obtener la ruta absoluta de este archivo
#    __file__ contiene la ruta del script actual.
script_dir = os.path.dirname(os.path.abspath(__file__))

# Función para obtener la ruta de recursos (sin uso por el momento)
def resource_path(relative_path: str) -> str:
    """
    Devuelve la ruta absoluta al recurso, compatible con ejecución normal y compilada.
    """
    # Si está compilado
    if hasattr(sys, "_MEIPASS"):
        return os.path.join(sys._MEIPASS, relative_path)
    
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), relative_path)

# 2. Construir la ruta al config.json
#    Como config_manager.py está en el raíz, el config está en "config/config.json"
CONFIG_FILE = resource_path(os.path.join('config', 'config.json'))

APPDATA_DIR = os.path.join(os.getenv("APPDATA"), "SCRDA")
USER_BASE_FILE = os.path.join(APPDATA_DIR, "user_base.txt")

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

def cargar_ruta_base():
    """
    Lee la carpeta principal definida por el usuario.
    """
    if os.path.exists(USER_BASE_FILE):
        with open(USER_BASE_FILE, "r", encoding="utf-8") as f:
            return f.read().strip()
    return ""

def guardar_ruta_base(ruta_base):
    """
    Guarda la carpeta principal.
    """
    os.makedirs(APPDATA_DIR, exist_ok=True)
    with open(USER_BASE_FILE, "w", encoding="utf-8") as f:
        f.write(ruta_base)

def obtener_ruta(nombre_ruta):
    """
    Obtiene una ruta específica del archivo de configuración + la carpeta principal.
    """
    config = cargar_configuracion()
    relativa = config.get(nombre_ruta)
    
    if not relativa:
        raise KeyError(f"La clave de ruta '{nombre_ruta}' no se encontró en el config.json")
    
    ruta_base = cargar_ruta_base()
    if not ruta_base:
        raise RuntimeError("La carpeta base no está definida. Selecciónela en el menú principal antes de continuar.")
    
    path = os.path.join(ruta_base, relativa)
    path = os.path.normpath(path)

    # Aseguramos que la carpeta exista antes de devolver la ruta
    if 'database' in nombre_ruta:
        # Si es un archivo .db, creamos la carpeta que lo contiene
        os.makedirs(os.path.dirname(path), exist_ok=True)
    elif 'limpio' in nombre_ruta or 'predicciones' in nombre_ruta:
        # Si es una ruta de carpeta, la creamos
        os.makedirs(path, exist_ok=True)
    
    return path
