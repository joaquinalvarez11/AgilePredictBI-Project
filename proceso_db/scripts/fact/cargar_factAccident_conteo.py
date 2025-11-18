import sqlite3
import time
import os
import sys

# 1. Añadir el directorio raíz del proyecto al path
current_dir = os.path.dirname(__file__)
# Subimos TRES niveles (desde /proceso_db/scripts/dim/ hasta el raíz)
project_root = os.path.abspath(os.path.join(current_dir, '..', '..', '..'))
sys.path.append(project_root)

# 2. Importar el gestor de configuración
from config_manager import obtener_ruta

try:
    # 3. Obtener la ruta desde config.json
    ruta_db = obtener_ruta("ruta_database")
except Exception as e:
    print(f"Error al cargar ruta_database desde config_manager: {e}")
    sys.exit(1)

# === 2. Consulta de Actualización ===
sql_update_query = """
UPDATE factAccident
SET totalVehicles = (
    SELECT COUNT(*) 
    FROM factVehicleAccident
    WHERE factVehicleAccident.idAccident = factAccident.idAccident
);
"""

# === 3. Proceso Principal ===
print("Iniciando script de actualización de conteos...")
start_time = time.time()

try:
    conn = sqlite3.connect(ruta_db)
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON;")

    print("Ejecutando actualización de 'totalVehicles' en factAccident...")
    cursor.execute(sql_update_query)
    
    # Obtener el número de filas actualizadas
    rows_updated = cursor.rowcount
    
    conn.commit()
    
    end_time = time.time()
    print(f"\n--- ¡Éxito! ---")
    print(f"Se actualizaron {rows_updated} accidentes.")
    print(f"Duración: {end_time - start_time:.2f} segundos.")

except Exception as e:
    print(f"ERROR: No se pudo completar la actualización.")
    print(f"Error: {e}")
    if conn:
        conn.rollback()
finally:
    if conn:
        conn.close()
    print("Conexión cerrada.")