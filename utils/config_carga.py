import json
import os

CONFIG_PATH = os.path.join("config", "config.json")

def load_config():
    """Carga el archivo de configuración JSON una sola vez."""
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)

# Cargarlo inmediatamente al importar este módulo
config = load_config()

# Variables listas para usar en cualquier parte
BRUTOS_PATH = config["ruta_excel_bruto"]
LIMPIOS_PATH = config["ruta_csv_limpio"]
PREDICCIONES_PATH = config["ruta_predicciones"]
# Insertar el siguiente código al inicio de cada vista que utilice los archivos:
# from utils.config_carga import BRUTOS_PATH, LIMPIOS_PATH, PREDICCIONES_PATH