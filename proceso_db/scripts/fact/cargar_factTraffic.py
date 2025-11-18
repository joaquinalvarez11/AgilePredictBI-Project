import sqlite3
import pandas as pd
import os
import sys
import glob
from datetime import datetime
import re
import numpy as np
import time

# --- INICIO DE INTEGRACIÓN ---
# 1. Añadir el directorio raíz del proyecto al path
# Esto permite que el script encuentre 'config_manager'
current_dir = os.path.dirname(__file__)
# Subimos dos niveles (desde /proceso_db/scripts/fact/ hasta el raíz)
project_root = os.path.abspath(os.path.join(current_dir, '..', '..', '..'))
sys.path.append(project_root)

# 2. Importar el gestor de configuración
from config_manager import obtener_ruta

# 3. Obtener rutas desde config.json
try:
    ruta_db = obtener_ruta("ruta_database")
    ruta_csv_limpio = obtener_ruta("ruta_csv_limpio")
except Exception as e:
    print(f"Error al cargar rutas desde config_manager: {e}")
    sys.exit(1) # Detener el script si la config falla

# === 1. Definir Ruta ===
ruta_base_csv_trafico = os.path.join(ruta_csv_limpio, "Tráfico Mensual")

# === 2. Funciones Helper ===
def safe_int_convert(value, default=None):
    val_num = pd.to_numeric(value, errors='coerce')
    if pd.isna(val_num):
        return default
    return int(val_num)

# === 3. Proceso ETL Principal ===
try:
    conn = sqlite3.connect(ruta_db)
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON;")

    print("--- Cargando Hechos: factTraffic ---")

    print("Consultando log de archivos ya procesados (Tráfico)...")
    cursor.execute("SELECT FileName FROM etl_log_trafico")
    processed_files = set(row[0] for row in cursor.fetchall())
    print(f"Se encontraron {len(processed_files)} archivos en el log.")

    print(f"Buscando archivos CSV en: {ruta_base_csv_trafico} y subcarpetas...")
    patron_busqueda = os.path.join(ruta_base_csv_trafico, '**', '*.csv')
    todos_los_archivos_csv = glob.glob(patron_busqueda, recursive=True)
    
    nuevos_archivos_csv = []
    for f in todos_los_archivos_csv:
        file_name = os.path.basename(f)
        if file_name not in processed_files:
            nuevos_archivos_csv.append(f)

    if not nuevos_archivos_csv:
        print("No se encontraron archivos CSV nuevos para Tráfico. El proceso ha finalizado.")
        conn.close()
        exit()
    
    print(f"Se encontraron {len(nuevos_archivos_csv)} archivos CSV NUEVOS. Iniciando carga...")

    lista_dataframes = []
    for archivo_csv in nuevos_archivos_csv:
        print(f"  Leyendo: {os.path.basename(archivo_csv)}")
        try:
            df_temp = pd.read_csv(
                archivo_csv, 
                encoding="utf-8", 
                dtype=str
            )
            df_temp['__SourceFileName'] = os.path.basename(archivo_csv)
            lista_dataframes.append(df_temp)
        except Exception as e:
            print(f"    ERROR: No se pudo leer o procesar el archivo {archivo_csv}. Error: {e}")

    if not lista_dataframes:
        print("Error: Ningún archivo CSV nuevo de Tráfico pudo ser leído. Saliendo.")
        conn.close()
        exit()
        
    df_trafico_full = pd.concat(lista_dataframes, ignore_index=True)
    print(f"\nCarga de CSVs de Tráfico completada. {len(df_trafico_full)} filas totales leídas.")

    # --- Preparar Mapas de Dimensiones (Lookups) ---
    print("Creando mapas de dimensiones desde la BD...")
    
    def crear_mapa(tabla, campo_nombre, campo_id):
        df_mapa = pd.read_sql(f"SELECT {campo_id}, {campo_nombre} FROM {tabla}", conn)
        # Convertir nombres a minúsculas y sin espacios para una búsqueda robusta
        return {str(k).strip().lower(): v for k, v in zip(df_mapa[campo_nombre], df_mapa[campo_id])}

    mapa_plaza = crear_mapa("dim_Plaza", "PlazaName", "idPlaza")
    mapa_direccion = crear_mapa("dim_Direction", "DirectionName", "idDirection")
    mapa_categoria = crear_mapa("dim_Category", "CategoryName", "idCategory")
    
    default_plaza_id = mapa_plaza.get("desconocido")
    default_direction_id = mapa_direccion.get("sin dato")

    # === Carga de mapa de DateTime eficiente ===
    print("Creando mapa de dim_DateTime (eficiente)...")
    df_trafico_full['Fecha_dt'] = pd.to_datetime(df_trafico_full['Fecha'], errors='coerce')
    df_trafico_full['Hora_int'] = pd.to_numeric(df_trafico_full['Hora'], errors='coerce').fillna(0).astype(int)
    
    # --- ¡CAMBIO CLAVE! ---
    # Crear la clave de búsqueda en el formato correcto 'YYYY-MM-DD HH:MM:SS'
    df_trafico_full['LookupKey'] = df_trafico_full.apply(
        lambda r: f"{r['Fecha_dt'].strftime('%Y-%m-%d')} {r['Hora_int']:02d}:00:00" if pd.notna(r['Fecha_dt']) else None,
        axis=1
    )
    claves_needed = tuple(df_trafico_full['LookupKey'].dropna().unique())
    
    mapa_datetime = {}
    if claves_needed: # Solo consultar si hay claves que buscar
        print(f"Buscando {len(claves_needed)} claves de fecha únicas en la BD...")
        df_fecha = pd.read_sql(f"""
            SELECT idDateTime, DateTime 
            FROM dim_DateTime 
            WHERE DateTime IN {claves_needed}
        """, conn)
        mapa_datetime = dict(zip(df_fecha['DateTime'], df_fecha['idDateTime']))
        del df_fecha
    print("Mapas creados.")

    # --- Procesar y Cargar Fila por Fila ---
    failed_rows_details = [] 
    dirty_files = set()
    total_rows = len(df_trafico_full)
    
    print(f"Procesando {total_rows} filas de tráfico...")
    start_loop = time.time()

    # --- INICIAR TRANSACCIÓN ---
    # Mover el cursor.execute("BEGIN...") fuera del bucle mejora drásticamente la velocidad.
    # Pero para feedback, lo dejaremos dentro con un contador.
    # Para la carga masiva real, considera usar df_cargable.to_sql como hizo tu compañero.
    
    filas_procesadas = 0
    filas_insertadas = 0

    for index, row in df_trafico_full.iterrows():
        source_file = row['__SourceFileName']
        try:
            # 1. Obtener IDs de Dimensiones
            
            # Búsqueda robusta (minúsculas, sin espacios)
            nombre_plaza_csv = row.get('Plaza')
            id_plaza = mapa_plaza.get(str(nombre_plaza_csv).strip().lower(), default_plaza_id)
            
            nombre_direccion_csv = row.get('Direccion')
            id_direction = mapa_direccion.get(str(nombre_direccion_csv).strip().lower(), default_direction_id)
            
            id_category = mapa_categoria.get(str(row.get('Categoria')).strip().lower())

            # idDateTime (clave pre-calculada)
            lookup_key = row['LookupKey']
            id_datetime = mapa_datetime.get(lookup_key)

            # 2. Obtener Hecho
            traffic_volume = safe_int_convert(row['Contar'], default=0)

            # 3. Validar Llaves Foráneas
            fks = {
                'idDateTime': id_datetime, 'idPlaza': id_plaza, 
                'idDirection': id_direction, 'idCategory': id_category
            }
            if None in fks.values():
                # Corregir los nombres en el CSV o en los mapas de cargar_dimensiones.py
                raise sqlite3.IntegrityError(f"FOREIGN KEY no encontrada. CSV: Plaza='{nombre_plaza_csv}', Dir='{nombre_direccion_csv}', Cat='{row.get('Categoria')}'. Valores: {fks}")

            # 4. Insertar
            cursor.execute("""
                INSERT INTO factTraffic (
                    idDateTime, idPlaza, idDirection, idCategory, trafficVolume
                ) VALUES (?, ?, ?, ?, ?)
            """, (
                id_datetime, id_plaza, id_direction, id_category, traffic_volume
            ))
            filas_insertadas += 1

        except Exception as e:
            error_msg = f"Error: {e}"
            failed_rows_details.append((row.to_dict(), error_msg, source_file))
            dirty_files.add(source_file)
        
        filas_procesadas += 1
        # === Log de Progreso Mejorado ===
        if filas_procesadas % 50000 == 0:
            elapsed = time.time() - start_loop
            rows_per_sec = filas_procesadas / elapsed
            print(f"  ... Procesadas {filas_procesadas} de {total_rows} ({rows_per_sec:.0f} filas/seg). Insertadas: {filas_insertadas}. Errores: {len(failed_rows_details)}")
            # Hacer commit en lotes para no perder todo si falla
            conn.commit()
            cursor.execute("BEGIN IMMEDIATE") # Iniciar nuevo lote

    # --- Commit final y Registrar Logs ---
    conn.commit() # Asegurarse de guardar el último lote
    print("...procesamiento de filas de Tráfico finalizado.")

    print("Registrando archivos procesados en el log (Tráfico)...")
    now_str = datetime.now().isoformat()
    
    files_to_log = []
    for f_path in nuevos_archivos_csv:
        f_name = os.path.basename(f_path)
        if f_name not in dirty_files:
            files_to_log.append((f_name, now_str))
            
    if files_to_log:
        cursor.executemany("INSERT OR IGNORE INTO etl_log_trafico (FileName, LoadedTimestamp) VALUES (?, ?)", files_to_log)
        print(f"{len(files_to_log)} archivos 100% limpios registrados en el log etl_log_trafico.")
    
    if dirty_files:
        print(f"ADVERTENCIA: {len(dirty_files)} archivos con errores no se registraron en etl_log_trafico y serán reintentados.")

    if failed_rows_details:
        print("\n--- ⚠️ Reporte Detallado de Errores de Carga (Tráfico) ---")
        print(f"Se omitieron {len(failed_rows_details)} filas por errores de FK o datos.")
        for i, (fila, error, archivo) in enumerate(failed_rows_details[:10]):
            print(f"\n  - Archivo: {archivo}")
            print(f"    Error: {error}")
            
    conn.commit() # Commit final para los logs
    print("Proceso ETL para Tráfico completado.")

except Exception as e:
    print(f"Error: {e}")
    conn.rollback() 
finally:
    if conn:
        conn.close()
        print("Conexión cerrada.")