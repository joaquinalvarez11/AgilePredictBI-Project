import sqlite3
import pandas as pd
import os
import glob
from datetime import datetime
import time
from config_manager import obtener_ruta

# === 1. Rutas ===
def get_paths():
    try:
        # Asegurarse de devolver la ruta correcta a Ficha 0
        return obtener_ruta("ruta_database"), os.path.join(obtener_ruta("ruta_csv_limpio"), "Siniestralidad", "Ficha 0")
    except Exception as e:
        raise RuntimeError(f"Error config: {e}")

# === 2. Helper Obtener FK ===
def obtener_fk(cursor, row, mapa_datetime_local, callback):
    fks = {}
    
    # 2.1 Validación de Fecha
    try:
        csv_timestamp_str = str(row['FECHA/HORA'])
        if pd.isna(csv_timestamp_str) or csv_timestamp_str == 'nan':
            fks['idDateTime'] = None 
        else:
            dt_obj = None
            try: dt_obj = pd.to_datetime(csv_timestamp_str, format='%d/%m/%Y %H:%M')
            except: 
                try: dt_obj = pd.to_datetime(csv_timestamp_str, format='%Y-%m-%d %H:%M:%S')
                except: pass
            
            if dt_obj:
                search_key = dt_obj.strftime('%Y-%m-%d %H:%M:00')
                fks['idDateTime'] = mapa_datetime_local.get(search_key)
            else:
                fks['idDateTime'] = None
    except: 
        fks['idDateTime'] = None

    # 2.2 Helper conversión segura a entero
    def safe_int(v):
        # Convertimos a numérico, forzando NaN si falla
        val = pd.to_numeric(v, errors='coerce')
        # Si es NaN, devolvemos 0
        if pd.isna(val):
            return 0
        # Si es válido, devolvemos el entero
        return int(val)
    
    fks['idAccidentType'] = safe_int(row['Tipo Accidente'])
    fks['idRelativeLocation'] = safe_int(row['Ubicación Relativa'])
    fks['idSurfaceCondition'] = safe_int(row['Condición calzada'])
    fks['idWeather'] = safe_int(row['Estado Atmosférico'])
    fks['idLuminosity'] = safe_int(row['Luminosidad'])
    fks['idArtificialLight'] = safe_int(row['Luz artificial'])
    fks['idSection'] = safe_int(row['Tramo'])
    
    return fks

# === 3. ETL Principal ===
def run(callback):
    conn = None
    try:
        ruta_db, ruta_base_ficha0 = get_paths()
        conn = sqlite3.connect(ruta_db)
        cursor = conn.cursor()
        cursor.execute("PRAGMA foreign_keys = ON;")

        callback("--- Cargando Ficha 0: factAccident ---")
        
        # --- Archivos Procesados ---
        cursor.execute("SELECT FileName FROM etl_log_ficha0")
        processed_files = set(row[0] for row in cursor.fetchall())
        callback(f"Se encontraron {len(processed_files)} archivos en el log.")
        
        # --- Buscar Nuevos ---
        patron = os.path.join(ruta_base_ficha0, '**', '*.csv')
        todos = glob.glob(patron, recursive=True)
        nuevos_archivos_csv = [f for f in todos if os.path.basename(f) not in processed_files]

        if not nuevos_archivos_csv:
            callback("No se encontraron archivos CSV nuevos. El proceso ha finalizado.")
            return

        callback(f"Se encontraron {len(nuevos_archivos_csv)} archivos CSV NUEVOS. Iniciando carga...")
        
        lista_dfs = []
        for f in nuevos_archivos_csv:
            try:
                # Importante: dtype=str para no perder ceros a la izquierda
                df_t = pd.read_csv(f, encoding="utf-8", dtype=str, sep='|', skiprows=1)
                df_t['__SourceFileName'] = os.path.basename(f)
                lista_dfs.append(df_t)
            except Exception as e: 
                callback(f"Error leyendo {os.path.basename(f)}: {e}")
        
        if not lista_dfs: return
        df_ficha0 = pd.concat(lista_dfs, ignore_index=True)
        # Limpieza clave de IDs
        df_ficha0['ID Accidente'] = df_ficha0['ID Accidente'].str.strip()

        # --- Cargar Mapas de Validación ---
        def get_ids(tbl, col): 
            try: return set(pd.read_sql(f"SELECT {col} FROM {tbl}", conn)[col])
            except: return {0}

        map_ids_section = get_ids("dim_Section", "idSection")
        map_ids_accidenttype = get_ids("dim_AccidentType", "idAccidentType")
        map_ids_relativelocation = get_ids("dim_RelativeLocation", "idRelativeLocation")
        map_ids_surfacecondition = get_ids("dim_SurfaceCondition", "idSurfaceCondition")
        map_ids_weather = get_ids("dim_Weather", "idWeather")
        map_ids_luminosity = get_ids("dim_Luminosity", "idLuminosity")
        map_ids_artificiallight = get_ids("dim_ArtificialLight", "idArtificialLight")

        # --- Mapa Fechas (Optimizado) ---
        callback("Creando mapa de fechas...")
        df_ficha0['dt_obj'] = pd.to_datetime(df_ficha0['FECHA/HORA'], format='%d/%m/%Y %H:%M', errors='coerce')
        # Fallback format
        mask_nan = df_ficha0['dt_obj'].isna()
        df_ficha0.loc[mask_nan, 'dt_obj'] = pd.to_datetime(df_ficha0.loc[mask_nan, 'FECHA/HORA'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
        
        df_ficha0['LookupKey'] = df_ficha0['dt_obj'].dt.strftime('%Y-%m-%d %H:%M:00')
        keys = tuple(df_ficha0['LookupKey'].dropna().unique())
        
        mapa_datetime = {}
        if keys:
            q = pd.read_sql(f"SELECT idDateTime, DateTime FROM dim_DateTime WHERE DateTime IN {keys}", conn)
            mapa_datetime = dict(zip(q['DateTime'], q['idDateTime']))

        # --- PREPARACIÓN DE PUENTES (Mapas) ---
        def crear_mapa_doble(tabla, id_col, col1, col2):
            df_mapa = pd.read_sql(f"SELECT {id_col}, {col1}, {col2} FROM {tabla}", conn)
            return { (str(row[col1]), int(row[col2])): row[id_col] for index, row in df_mapa.iterrows() }

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

        # --- Loop Principal ---
        failed_accident_details = {}
        filas_para_insertar = []
        df_main = df_ficha0.drop_duplicates(subset=['ID Accidente'])
        
        total = len(df_main)
        callback(f"Procesando {total} accidentes...")

        for idx, row in df_main.iterrows():
            id_acc = row['ID Accidente']
            fks = obtener_fk(cursor, row, mapa_datetime, callback)
            
            # --- VALIDACIÓN MANUAL PARA GENERAR MENSAJE LIMPIO ---
            err_msg = None
            
            if fks['idDateTime'] is None:
                err_msg = f"Fecha inválida o no encontrada: '{row.get('FECHA/HORA')}'"
            elif fks['idAccidentType'] not in map_ids_accidenttype:
                err_msg = f"Tipo Accidente ({fks['idAccidentType']}) no existe en la base de datos."
            elif fks['idRelativeLocation'] not in map_ids_relativelocation:
                err_msg = f"Ubicación Relativa ({fks['idRelativeLocation']}) no existe en la base de datos."
            elif fks['idSection'] not in map_ids_section:
                err_msg = f"Tramo/Sección ({fks['idSection']}) no existe en la base de datos."
            elif fks['idSurfaceCondition'] not in map_ids_surfacecondition:
                err_msg = f"Condición Calzada ({fks['idSurfaceCondition']}) no existe en la base de datos."
            elif fks['idWeather'] not in map_ids_weather:
                err_msg = f"Clima ({fks['idWeather']}) no existe en la base de datos."
            elif fks['idLuminosity'] not in map_ids_luminosity:
                err_msg = f"Luminosidad ({fks['idLuminosity']}) no existe en la base de datos."
            elif fks['idArtificialLight'] not in map_ids_artificiallight:
                err_msg = f"Luz Artificial ({fks['idArtificialLight']}) no existe en la base de datos."

            if err_msg:
                # Guardamos solo el mensaje limpio y el archivo
                failed_accident_details[id_acc] = (err_msg, row['__SourceFileName'])
            else:
                # Si pasa la validación manual, preparamos la tupla
                filas_para_insertar.append((
                    id_acc, fks['idDateTime'], fks['idSection'], fks['idAccidentType'],
                    fks['idRelativeLocation'], fks['idSurfaceCondition'], fks['idWeather'],
                    fks['idLuminosity'], fks['idArtificialLight'], 
                    str(row.get('Daños Ocasionados a la Infraestructura vial','')), 
                    str(row.get('Descripción del Accidente','')), 
                    0 # totalVehicles placeholder
                ))

        # --- Inserción Masiva (factAccident) ---
        if filas_para_insertar:
            callback(f"Insertando {len(filas_para_insertar)} accidentes válidos...")
            try:
                cursor.executemany("""
                    INSERT OR IGNORE INTO factAccident (
                        idAccident, idDateTime, idSection, idAccidentType, idRelativeLocation,
                        idSurfaceCondition, idWeather, idLuminosity, idArtificialLight,
                        InfrastructureDamage, Description, totalVehicles
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """, filas_para_insertar)
            except sqlite3.IntegrityError as e:
                # Error en lote (poco probable con validación previa, pero por si acaso)
                callback(f"Error en inserción masiva: {e}")
                conn.rollback()
                raise e
        
        # --- CARGA DE PUENTES (Tablas M:N) ---
        callback("Cargando tablas puente y detalle...")
        # Iteramos sobre el dataframe completo original con duplicados para puentes
        df_full = df_ficha0.drop_duplicates().reset_index()

        filas_bridge_response = []
        filas_bridge_probablecause = []
        filas_bridge_environment = []
        filas_fact_affected = []
        filas_bridge_lane = []
        filas_bridge_km = []

        for index, row in df_full.iterrows():
            id_acc = row['ID Accidente']
            
            # Si el accidente falló en la carga principal, NO cargamos sus puentes
            if id_acc in failed_accident_details:
                continue

            try:
                # Logica Response
                val_resp = pd.to_numeric(row['Valor Concurrencia'], errors='coerce')
                if pd.notna(val_resp):
                    res_id = mapa_response.get((str(row['Concurrencia']), int(val_resp)))
                    if res_id: filas_bridge_response.append((id_acc, res_id))

                # Logica Causa
                val_causa = pd.to_numeric(row['Valor Causa Probable'], errors='coerce')
                if pd.notna(val_causa):
                    cau_id = mapa_probablecause.get((str(row['Causa Probable']), int(val_causa)))
                    if cau_id: filas_bridge_probablecause.append((id_acc, cau_id))

                # Logica Entorno
                val_ent = pd.to_numeric(row['Valor Condiciones del Entorno'], errors='coerce')
                if pd.notna(val_ent):
                    ent_id = mapa_environment.get((str(row['Condiciones del Entorno']), int(val_ent)))
                    if ent_id: filas_bridge_environment.append((id_acc, ent_id))

                # Logica Afectados
                cons_id = mapa_consequence.get(str(row['Consecuencia']))
                aff_id = mapa_affected.get(str(row['Afectado']))
                cnt_aff = pd.to_numeric(row['Cantidad Afectados'], errors='coerce')
                if cons_id and aff_id and pd.notna(cnt_aff):
                    filas_fact_affected.append((id_acc, cons_id, aff_id, int(cnt_aff)))

                # Logica Pistas (Lanes)
                for i in range(1, 7):
                    if pd.to_numeric(row[f'P{i}'], errors='coerce') > 0:
                        ln_id = mapa_lane.get(i)
                        if ln_id: filas_bridge_lane.append((id_acc, ln_id))

                # Logica Km
                km_val = pd.to_numeric(row['Km'], errors='coerce')
                if pd.notna(km_val):
                    km_rnd = round(float(km_val), 3)
                    cur_km = cursor.execute("SELECT idKm FROM dim_Km WHERE Km = ?", (km_rnd,)).fetchone()
                    if cur_km: filas_bridge_km.append((id_acc, cur_km[0]))

            except Exception:
                pass # Ignoramos errores menores en puentes para no detener carga masiva

        # Inserción de Puentes
        if filas_bridge_response: cursor.executemany("INSERT OR IGNORE INTO bridge_Accident_Response (idAccident, idResponse) VALUES (?,?)", filas_bridge_response)
        if filas_bridge_probablecause: cursor.executemany("INSERT OR IGNORE INTO bridge_Accident_ProbableCause (idAccident, idProbableCause) VALUES (?,?)", filas_bridge_probablecause)
        if filas_bridge_environment: cursor.executemany("INSERT OR IGNORE INTO bridge_Accident_Environment (idAccident, idEnvironment) VALUES (?,?)", filas_bridge_environment)
        if filas_fact_affected: cursor.executemany("INSERT OR IGNORE INTO factAccidentAffected (idAccident, idConsequence, idAffected, AffectedCount) VALUES (?,?,?,?)", filas_fact_affected)
        if filas_bridge_lane: cursor.executemany("INSERT OR IGNORE INTO bridge_Accident_Lane (idAccident, idLane) VALUES (?,?)", filas_bridge_lane)
        if filas_bridge_km: cursor.executemany("INSERT OR IGNORE INTO bridge_Accident_Km (idAccident, idKm) VALUES (?,?)", filas_bridge_km)

        conn.commit()

        # --- Logs de Archivos ---
        now_str = datetime.now().isoformat()
        # Identificar archivos sucios tuvieron al menos un error
        dirty_files = set(v[1] for v in failed_accident_details.values())
        
        to_log = []
        for f in nuevos_archivos_csv:
            fname = os.path.basename(f)
            if fname not in dirty_files:
                to_log.append((fname, now_str))
        
        if to_log:
            cursor.executemany("INSERT OR IGNORE INTO etl_log_ficha0 VALUES (?,?)", to_log)
        
        conn.commit()

        # === REPORTE EJECUTIVO LIMPIO PARA EL GESTOR ===
        if failed_accident_details:
            # Encabezado exacto que busca el Gestor
            callback("\n--- Reporte Detallado de Errores de Carga (Ficha 0) ---")
            callback(f"Se omitieron {len(failed_accident_details)} accidentes.")
            
            # Limitar a los primeros 20 errores para no saturar si son muchos
            limit_count = 0
            for id_acc, (error_msg, archivo) in failed_accident_details.items():
                if limit_count >= 20:
                    callback(f"... y {len(failed_accident_details) - 20} errores más.")
                    break
                
                # FORMATO LIMPIO:
                callback(f"- ID Accidente: {id_acc}")
                callback(f"  Error: {error_msg} (Archivo: {archivo})")
                limit_count += 1

        callback("Proceso ETL para Ficha 0 completado.")

    except Exception as e:
        callback(f"Error: {e}")
        if conn: conn.rollback()
        raise
    finally:
        if conn: conn.close()