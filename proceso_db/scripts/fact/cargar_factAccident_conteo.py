import sqlite3
import time
from config_manager import obtener_ruta

# === Consulta de Actualización ===
sql_update_query = """
UPDATE factAccident
SET totalVehicles = (
    SELECT COUNT(*) 
    FROM factVehicleAccident
    WHERE factVehicleAccident.idAccident = factAccident.idAccident
);
"""

def run(callback):
    """
    Actualiza el campo totalVehicles en factAccident.
    Recibe un callback para enviar mensajes de log.
    """
    start_time = time.time()
    try:
        ruta_db = obtener_ruta("ruta_database")
        print("Iniciando script de actualización de conteos...")
        conn = sqlite3.connect(ruta_db)
        cursor = conn.cursor()
        cursor.execute("PRAGMA foreign_keys = ON;")

        callback("Ejecutando actualización de 'totalVehicles' en factAccident...")
        cursor.execute(sql_update_query)
        
        # Obtener el número de filas actualizadas
        rows_updated = cursor.rowcount
        
        conn.commit()
        
        end_time = time.time()
        callback(f"\n--- ¡Éxito! ---")
        callback(f"Se actualizaron {rows_updated} accidentes.")
        print(f"Duración: {end_time - start_time:.2f} segundos.")

    except Exception as e:
        callback(f"ERROR: No se pudo completar la actualización.")
        callback(f"Error: {e}")
        if 'conn' in locals():
            conn.rollback()
        raise
    finally:
        if 'conn' in locals():
            conn.close()
            print("Conexión cerrada.")
