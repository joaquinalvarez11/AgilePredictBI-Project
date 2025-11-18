import sqlite3
import pandas as pd
import os
import sys
import glob
from datetime import datetime
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
ruta_base_csv_ficha0 = os.path.join(ruta_csv_limpio, "Siniestralidad", "Ficha 0")

# === 2. Función Helper ===
def obtener_fk(cursor, row, mapa_datetime_local):
    fks = {}
    
    # --- Lookup dim_DateTime ---
    try:
        csv_timestamp_str = str(row['FECHA/HORA'])
        if pd.isna(csv_timestamp_str) or csv_timestamp_str == 'nan':
             print(f"Error fatal procesando fecha: {row['FECHA/HORA']}. Se usará NULL.")
             fks['idDateTime'] = None # Marcar como None
        else:
            dt_obj = None
            try:
                dt_obj = pd.to_datetime(csv_timestamp_str, format='%d/%m/%Y %H:%M')
            except ValueError:
                try:
                    dt_obj = pd.to_datetime(csv_timestamp_str, format='%Y-%m-%d %H:%M:%S')
                except Exception as e2:
                    print(f"Error procesando fecha '{csv_timestamp_str}' en ambos formatos: {e2}. Se usará NULL.")
                    fks['idDateTime'] = None

            if dt_obj is not None:
                # Formato de búsqueda :SS (segundos)
                search_timestamp_str = dt_obj.strftime('%Y-%m-%d %H:%M:00')
                resultado_id = mapa_datetime_local.get(search_timestamp_str)
                
                if resultado_id:
                    fks['idDateTime'] = resultado_id
                else:
                    print(f"Advertencia: No se encontró la fecha/minuto {search_timestamp_str} (de {csv_timestamp_str}) en dim_DateTime. Se usará NULL.")
                    fks['idDateTime'] = None

    except Exception as e:
        print(f"Error fatal procesando fecha: {row['FECHA/HORA']} -> {e}. Se usará NULL.")
        fks['idDateTime'] = None

    # --- Lookups 1:N (Directos) ---
    try:
        def safe_int_convert(value, default=0):
            val_num = pd.to_numeric(value, errors='coerce')
            if pd.isna(val_num):
                return default
            return int(val_num)

        fks['idAccidentType'] = safe_int_convert(row['Tipo Accidente'], 0)
        fks['idRelativeLocation'] = safe_int_convert(row['Ubicación Relativa'], 0)
        fks['idSurfaceCondition'] = safe_int_convert(row['Condición calzada'], 0)
        fks['idWeather'] = safe_int_convert(row['Estado Atmosférico'], 0)
        fks['idLuminosity'] = safe_int_convert(row['Luminosidad'], 0)
        fks['idArtificialLight'] = safe_int_convert(row['Luz artificial'], 0)
        fks['idSection'] = safe_int_convert(row['Tramo'], 0)

    except Exception as e:
        print(f"Error inesperado en Lookups 1:N: {e}. Fila: {row}")
        fks.update({k: 0 for k in ['idAccidentType', 'idRelativeLocation', 'idSurfaceCondition', 'idWeather', 'idLuminosity', 'idArtificialLight', 'idSection'] if k not in fks})

    return fks

# === 3. Proceso ETL Principal ===
try:
    conn = sqlite3.connect(ruta_db)
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON;")

    print("--- Cargando Ficha 0: factAccident ---")
    
    print("Consultando log de archivos ya procesados...")
    cursor.execute("SELECT FileName FROM etl_log_ficha0")
    processed_files = set(row[0] for row in cursor.fetchall())
    print(f"Se encontraron {len(processed_files)} archivos en el log.")

    # ... Lectura de CSVs ...
    print(f"Buscando archivos CSV en: {ruta_base_csv_ficha0} y subcarpetas...")
    patron_busqueda = os.path.join(ruta_base_csv_ficha0, '**', '*.csv')
    todos_los_archivos_csv = glob.glob(patron_busqueda, recursive=True)
    
    nuevos_archivos_csv = []
    for f in todos_los_archivos_csv:
        file_name = os.path.basename(f)
        if file_name not in processed_files:
            nuevos_archivos_csv.append(f)

    if not nuevos_archivos_csv:
        print("No se encontraron archivos CSV nuevos para cargar. El proceso ha finalizado.")
        conn.close()
        exit()
    
    print(f"Se encontraron {len(nuevos_archivos_csv)} archivos CSV NUEVOS. Iniciando carga...")
    
    lista_dataframes = []
    for archivo_csv in nuevos_archivos_csv:
        print(f"  Leyendo: {os.path.basename(archivo_csv)}")
        try:
            df_temp = pd.read_csv(archivo_csv, encoding="utf-8", dtype=str, sep='|', skiprows=1)
            df_temp['__SourceFileName'] = os.path.basename(archivo_csv)
            lista_dataframes.append(df_temp)
        except Exception as e:
            print(f"    ERROR: No se pudo leer o procesar el archivo {archivo_csv}. Error: {e}")
            
    if not lista_dataframes:
        print("Error: Ningún archivo CSV nuevo pudo ser leído correctamente. Saliendo.")
        conn.close()
        exit()
        
    df_ficha0 = pd.concat(lista_dataframes, ignore_index=True)
    print(f"\nCarga de CSVs completada. {len(df_ficha0)} filas totales leídas de {len(nuevos_archivos_csv)} archivos.")
    
    # --- Pre-cargar mapa de DateTime ---
    print("Optimizando búsquedas de fecha...")
    df_main_accident_dates = df_ficha0.drop_duplicates(subset=['ID Accidente']).copy()
    
    df_main_accident_dates['dt_obj'] = pd.to_datetime(df_main_accident_dates['FECHA/HORA'], format='%d/%m/%Y %H:%M', errors='coerce')
    mask_failed = df_main_accident_dates['dt_obj'].isna()
    df_main_accident_dates.loc[mask_failed, 'dt_obj'] = pd.to_datetime(df_main_accident_dates.loc[mask_failed, 'FECHA/HORA'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
    
    # --- El formato de búsqueda debe coincidir con la BD ---
    df_main_accident_dates['LookupKey'] = df_main_accident_dates['dt_obj'].dt.strftime('%Y-%m-%d %H:%M:00')
    
    claves_needed = tuple(df_main_accident_dates['LookupKey'].dropna().unique())
    
    mapa_datetime = {}
    if claves_needed:
        print(f"Buscando {len(claves_needed)} claves de fecha únicas en la BD...")
        df_fecha = pd.read_sql(f"SELECT idDateTime, DateTime FROM dim_DateTime WHERE DateTime IN {claves_needed}", conn)
        mapa_datetime = dict(zip(df_fecha['DateTime'], df_fecha['idDateTime']))
        del df_fecha
    print(f"Mapa de dim_DateTime creado con {len(mapa_datetime)} claves encontradas.") # Log para verificar

    # --- Pre-cargar todos los mapas de puentes ---
    print("Creando mapas para tablas puente...")
    def crear_mapa_doble(tabla, id_col, col1, col2):
        df_mapa = pd.read_sql(f"SELECT {id_col}, {col1}, {col2} FROM {tabla}", conn)
        return { (str(row[col1]), int(row[col2])): row[id_col] for index, row in df_mapa.iterrows() } # Convertir col2 a int

    def crear_mapa_simple(tabla, id_col, col1):
        df_mapa = pd.read_sql(f"SELECT {id_col}, {col1} FROM {tabla}", conn)
        if pd.api.types.is_numeric_dtype(df_mapa[col1]):
             return {int(k): v for k, v in zip(df_mapa[col1], df_mapa[id_col])}
        return {str(k): v for k, v in zip(df_mapa[col1], df_mapa[id_col])}

    mapa_response = crear_mapa_doble("dim_Response", "idResponse", "ResponseType", "ResponseValue")
    mapa_probablecause = crear_mapa_doble("dim_ProbableCause", "idProbableCause", "ProbableCauseType", "CauseValue")
    mapa_environment = crear_mapa_doble("dim_Environment", "idEnvironment", "EnvironmentCondition", "EnvironmentValue")
    mapa_consequence = crear_mapa_simple("dim_Consequence", "idConsequence", "ConsequenceType")
    mapa_affected = crear_mapa_simple("dim_Affected", "idAffected", "AffectedType")
    mapa_lane = crear_mapa_simple("dim_Lane", "idLane", "LaneValue")
    
    print("Mapas de puentes creados.")

    # --- Cargar mapas de IDs 1:N para validación ---
    print("Creando mapas de validación 1:N...")
    def crear_mapa_ids(tabla, id_col):
        try:
            return set(pd.read_sql(f"SELECT {id_col} FROM {tabla}", conn)[id_col])
        except Exception as e:
            print(f"Error al cargar mapa de IDs para {tabla}: {e}")
            # Retorna un set con el ID 'Sin dato' como mínimo
            return {0} 

    map_ids_section = crear_mapa_ids("dim_Section", "idSection")
    map_ids_accidenttype = crear_mapa_ids("dim_AccidentType", "idAccidentType")
    map_ids_relativelocation = crear_mapa_ids("dim_RelativeLocation", "idRelativeLocation")
    map_ids_surfacecondition = crear_mapa_ids("dim_SurfaceCondition", "idSurfaceCondition")
    map_ids_weather = crear_mapa_ids("dim_Weather", "idWeather")
    map_ids_luminosity = crear_mapa_ids("dim_Luminosity", "idLuminosity")
    map_ids_artificiallight = crear_mapa_ids("dim_ArtificialLight", "idArtificialLight")
    
    print("Mapas de validación 1:N creados.")

    # --- 1. Cargar factAccident (Hechos Principales) ---
    print("Cargando factAccident...")
    df_ficha0['ID Accidente'] = df_ficha0['ID Accidente'].str.strip()
    df_main_accident = df_ficha0.drop_duplicates(subset=['ID Accidente']).reset_index() 
    
    failed_accident_details = {}
    filas_para_fact_accident = []
    total_rows = len(df_main_accident)
    start_loop = time.time()

    for index, row in df_main_accident.iterrows():
        if (index + 1) % 10000 == 0:
            elapsed = time.time() - start_loop
            rows_per_sec = (index + 1) / elapsed
            print(f"  ... Procesando Accidente {index + 1} de {total_rows} ({rows_per_sec:.0f} filas/seg)")
        
        fks = obtener_fk(cursor, row, mapa_datetime) # Llamada a la función global
        id_acc = row['ID Accidente']
        
        damage_text = str(row['Daños Ocasionados a la Infraestructura vial'])
        total_veh = 0
        
        try:
            error_fk = None
            if fks['idDateTime'] is None:
                error_fk = "idDateTime es Nulo (fecha nan o no encontrada)"
            elif fks['idSection'] not in map_ids_section:
                error_fk = f"idSection {fks['idSection']} no existe en dim_Section"
            elif fks['idAccidentType'] not in map_ids_accidenttype:
                error_fk = f"idAccidentType {fks['idAccidentType']} no existe en dim_AccidentType"
            elif fks['idRelativeLocation'] not in map_ids_relativelocation:
                error_fk = f"idRelativeLocation {fks['idRelativeLocation']} no existe en dim_RelativeLocation"
            elif fks['idSurfaceCondition'] not in map_ids_surfacecondition:
                error_fk = f"idSurfaceCondition {fks['idSurfaceCondition']} no existe en dim_SurfaceCondition"
            elif fks['idWeather'] not in map_ids_weather:
                error_fk = f"idWeather {fks['idWeather']} no existe en dim_Weather"
            elif fks['idLuminosity'] not in map_ids_luminosity:
                error_fk = f"idLuminosity {fks['idLuminosity']} no existe en dim_Luminosity"
            elif fks['idArtificialLight'] not in map_ids_artificiallight:
                error_fk = f"idArtificialLight {fks['idArtificialLight']} no existe en dim_ArtificialLight"

            if error_fk:
                # Usar la misma clase de excepción para que el logger la capture
                raise sqlite3.IntegrityError(f"FOREIGN KEY no encontrada: {error_fk}. Valores: {fks}")
            
            # Si pasa la validación, se añade a la lista
            filas_para_fact_accident.append((
                id_acc, fks['idDateTime'], fks['idSection'],
                fks['idAccidentType'], fks['idRelativeLocation'],
                fks['idSurfaceCondition'], fks['idWeather'], fks['idLuminosity'], fks['idArtificialLight'],
                damage_text, row['Descripción del Accidente'], total_veh
            ))
            
        except sqlite3.IntegrityError as e:
            if "FOREIGN KEY" in str(e) or "FOREIGN KEY no encontrada" in str(e):
                print(f"Error de Integridad FK al insertar {id_acc}: {e}")
                print(f"Valores FK que fallaron: {fks}")
            elif "UNIQUE" in str(e) or "PRIMARY KEY" in str(e):
                print(f"Info: {id_acc} ya existe. Omitiendo (PK/UNIQUE).")
            else:
                print(f"Error de Integridad (otro) al insertar {id_acc}: {e}")
            failed_accident_details[id_acc] = (fks, row['__SourceFileName'])

    if filas_para_fact_accident:
        print(f"Insertando {len(filas_para_fact_accident)} filas válidas en factAccident...")
        try:
            cursor.executemany("""
                INSERT OR IGNORE INTO factAccident (
                    idAccident, idDateTime, idSection,
                    idAccidentType, idRelativeLocation,
                    idSurfaceCondition, idWeather, idLuminosity, idArtificialLight,
                    InfrastructureDamage, Description, totalVehicles
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, filas_para_fact_accident)
        except sqlite3.IntegrityError as e:
            print(f"ERROR FATAL en carga por lotes de factAccident. Revisar duplicados o FKs. Error: {e}")
            conn.rollback()
            raise e

    print("...factAccident procesado.")

    # --- 2. Cargar Tablas Puente (M:N) y Detalle ---
    print("Cargando tablas puente y de detalle...")
    df_full = df_ficha0.drop_duplicates().reset_index()

    filas_bridge_response = []
    filas_bridge_probablecause = []
    filas_bridge_environment = []
    filas_fact_affected = []
    filas_bridge_lane = []
    filas_bridge_km = []

    total_rows_full = len(df_full)
    start_loop_bridges = time.time()

    for index, row in df_full.iterrows():
        id_acc = row['ID Accidente']
        
        if (index + 1) % 50000 == 0:
            elapsed = time.time() - start_loop_bridges
            rows_per_sec = (index + 1) / elapsed
            print(f"  ... Procesando fila M:N {index + 1} de {total_rows_full} ({rows_per_sec:.0f} filas/seg)")

        if id_acc in failed_accident_details:
            continue

        try:
            val_concurrencia = pd.to_numeric(row['Valor Concurrencia'], errors='coerce')
            if pd.notna(val_concurrencia):
                id_resp_result = mapa_response.get((str(row['Concurrencia']), int(val_concurrencia)))
                if id_resp_result:
                    filas_bridge_response.append((id_acc, id_resp_result))

            val_causa = pd.to_numeric(row['Valor Causa Probable'], errors='coerce')
            if pd.notna(val_causa):
                id_cause_result = mapa_probablecause.get((str(row['Causa Probable']), int(val_causa)))
                if id_cause_result:
                    filas_bridge_probablecause.append((id_acc, id_cause_result))

            val_entorno = pd.to_numeric(row['Valor Condiciones del Entorno'], errors='coerce')
            if pd.notna(val_entorno):
                id_env_result = mapa_environment.get((str(row['Condiciones del Entorno']), int(val_entorno)))
                if id_env_result:
                    filas_bridge_environment.append((id_acc, id_env_result))

            id_cons_result = mapa_consequence.get(str(row['Consecuencia']))
            id_aff_result = mapa_affected.get(str(row['Afectado']))
            val_afectados = pd.to_numeric(row['Cantidad Afectados'], errors='coerce')
            
            if id_cons_result and id_aff_result and pd.notna(val_afectados):
                filas_fact_affected.append((id_acc, id_cons_result, id_aff_result, int(val_afectados)))
        
        except Exception as e_inner:
            print(f"Error procesando fila M:N para {id_acc} (Fila CSV {index}): {e_inner}")

    df_lanes_km = df_main_accident[['ID Accidente', 'Km', 'P1', 'P2', 'P3', 'P4', 'P5', 'P6']]
    
    for index, row in df_lanes_km.iterrows():
        id_acc = row['ID Accidente']
        
        if id_acc in failed_accident_details:
            continue
            
        for i in range(1, 7):
            col_name = f'P{i}'
            if pd.to_numeric(row[col_name], errors='coerce') > 0: 
                id_lane_result = mapa_lane.get(i) # Usar mapa
                if id_lane_result:
                    filas_bridge_lane.append((id_acc, id_lane_result))
                else:
                    print(f"Error: No se encontró idLane para LaneValue = {i}. Saltando.")
        
        try:
            km_val_csv = pd.to_numeric(row['Km'], errors='coerce')
            if pd.notna(km_val_csv):
                km_rounded = round(float(km_val_csv), 3) 
                km_result = cursor.execute("SELECT idKm FROM dim_Km WHERE Km = ?", (km_rounded,)).fetchone()
                if km_result:
                    filas_bridge_km.append((id_acc, km_result[0]))
                else:
                    print(f"Error: No se encontró idKm para Km = {km_rounded} (Accidente {id_acc}).")
        except Exception as e_km:
            print(f"Error procesando puente Km para {id_acc}: {e_km}")
    
    print("Insertando datos en tablas puente...")
    if filas_bridge_response:
        cursor.executemany("INSERT OR IGNORE INTO bridge_Accident_Response (idAccident, idResponse) VALUES (?, ?)", filas_bridge_response)
    if filas_bridge_probablecause:
        cursor.executemany("INSERT OR IGNORE INTO bridge_Accident_ProbableCause (idAccident, idProbableCause) VALUES (?, ?)", filas_bridge_probablecause)
    if filas_bridge_environment:
        cursor.executemany("INSERT OR IGNORE INTO bridge_Accident_Environment (idAccident, idEnvironment) VALUES (?, ?)", filas_bridge_environment)
    if filas_fact_affected:
        cursor.executemany("INSERT OR IGNORE INTO factAccidentAffected (idAccident, idConsequence, idAffected, AffectedCount) VALUES (?, ?, ?, ?)", filas_fact_affected)
    if filas_bridge_lane:
        cursor.executemany("INSERT OR IGNORE INTO bridge_Accident_Lane (idAccident, idLane) VALUES (?, ?)", filas_bridge_lane)
    if filas_bridge_km:
        cursor.executemany("INSERT OR IGNORE INTO bridge_Accident_Km (idAccident, idKm) VALUES (?, ?)", filas_bridge_km)

    print("...tablas puente y de detalle cargadas.")

    conn.commit() 
    print("Datos guardados en base de datos.")

    # --- Lógica de Log ---
    print("Registrando archivos procesados en el log (Ficha 0)...")
    now_str = datetime.now().isoformat()
    
    dirty_files = set()
    if failed_accident_details:
        for fks, filename in failed_accident_details.values():
            dirty_files.add(filename)
    
    files_to_log = []
    for f_path in nuevos_archivos_csv:
        f_name = os.path.basename(f_path)
        if f_name not in dirty_files:
            files_to_log.append((f_name, now_str))
            
    if files_to_log:
        cursor.executemany("INSERT OR IGNORE INTO etl_log_ficha0 (FileName, LoadedTimestamp) VALUES (?, ?)", files_to_log)
        print(f"{len(files_to_log)} archivos 100% limpios registrados en el log etl_log_ficha0.")
    
    if dirty_files:
        print(f"ADVERTENCIA: {len(dirty_files)} archivos con errores no se registraron en etl_log_ficha0 y serán reintentados.")

    if failed_accident_details:
        print("\n--- ⚠️ Reporte Detallado de Errores de Carga (Ficha 0) ---")
        print(f"Se omitieron {len(failed_accident_details)} accidentes por IDs de dimensión inválidos.")
        print("Los valores FK de los accidentes que fallaron son:")
        
        for failed_id, (fks_error, filename) in sorted(failed_accident_details.items()):
            print(f"\n  - ID Accidente: {failed_id} (Del archivo: {filename})")
            print(f"    Valores FK: {fks_error}")
            
    conn.commit()
    print("Proceso ETL para Ficha 0 completado.")

except Exception as e:
    print(f"Error: {e}")
    conn.rollback() 
finally:
    if conn:
        conn.close()
        print("Conexión cerrada.")