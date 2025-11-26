import sqlite3
import pandas as pd
import os
import glob
from datetime import datetime
from config_manager import obtener_ruta

# --- INICIO DE INTEGRACIÓN ---

# === 1. Definir Rutas ===
def get_paths():
    try:
        ruta_db = obtener_ruta("ruta_database")
        ruta_csv_limpio = obtener_ruta("ruta_csv_limpio")
        ruta_base_csv_ficha1 = os.path.join(ruta_csv_limpio, "Siniestralidad", "Ficha 1")

        return ruta_db, ruta_base_csv_ficha1
    except Exception as e:
        raise RuntimeError(f"Error al cargar rutas desde config_manager: {e}")

# === 2. Funciones Helper ===
def safe_int_convert(value, default=0):
    """ Convierte a int de forma segura, usando un default (ej. 0 para 'Sin Dato') """
    val_num = pd.to_numeric(value, errors='coerce')
    if pd.isna(val_num):
        return default
    return int(val_num)

def crear_mapa_ids(conn, tabla, id_col, callback):
    """ Carga un set de IDs válidos desde una tabla de dimensión. """
    try:
        return set(pd.read_sql(f"SELECT {id_col} FROM {tabla}", conn)[id_col])
    except Exception as e:
        callback(f"Error al cargar mapa de IDs para {tabla}: {e}")
        return {0} # Retorna un set con el ID 'Sin dato' como mínimo

def crear_mapa_simple(conn, tabla, id_col, col1):
    """ Carga un mapa clave->valor (ej. LaneValue -> idLane) """
    df_mapa = pd.read_sql(f"SELECT {id_col}, {col1} FROM {tabla}", conn)
    if pd.api.types.is_numeric_dtype(df_mapa[col1]):
         return {int(k): v for k, v in zip(df_mapa[col1], df_mapa[id_col])}
    return {str(k): v for k, v in zip(df_mapa[col1], df_mapa[id_col])}

# === 3. Proceso ETL Principal ===

def run(callback):
    """
    Carga los hechos de Ficha 1 en factVehicleAccident.
    Recibe un callback para enviar mensajes de log.
    """
    try:
        ruta_db, ruta_base_csv_ficha1 = get_paths()
        conn = sqlite3.connect(ruta_db)
        cursor = conn.cursor()
        cursor.execute("PRAGMA foreign_keys = ON;")
        
        callback("--- Cargando Ficha 1: factVehicleAccident ---")

        # --- 1. Cargar Logs de Archivos Procesados ---
        print("Consultando log de archivos ya procesados (Ficha 1)...")
        cursor.execute("SELECT FileName FROM etl_log_ficha1_vehiculos")
        processed_files = set(row[0] for row in cursor.fetchall())
        callback(f"Se encontraron {len(processed_files)} archivos en el log.")

        # --- 2. Leer todos los CSVs nuevos ---
        print(f"Buscando archivos CSV en: {ruta_base_csv_ficha1} y subcarpetas...")
        patron_busqueda = os.path.join(ruta_base_csv_ficha1, '**', '*.csv')
        todos_los_archivos_csv = glob.glob(patron_busqueda, recursive=True)
        
        nuevos_archivos_csv = []
        for f in todos_los_archivos_csv:
            file_name = os.path.basename(f)
            if file_name not in processed_files:
                nuevos_archivos_csv.append(f)

        if not nuevos_archivos_csv:
            callback("No se encontraron archivos CSV nuevos para Ficha 1. El proceso ha finalizado.")
            conn.close()
            return
        
        callback(f"Se encontraron {len(nuevos_archivos_csv)} archivos CSV NUEVOS. Iniciando carga...")

        # --- 3. Cargar todos los mapas de validación de FKs ---
        print("Creando mapas de validación de FKs (Ficha 1)...")
        
        # FKs de Hechos
        map_ids_accidents = crear_mapa_ids(conn, "factAccident", "idAccident", callback)
        
        # FKs de Dimensiones (Puentes)
        map_ids_service = crear_mapa_ids(conn, "dim_ServiceType", "idServiceType", callback)
        map_ids_vehicletypevalue = crear_mapa_ids(conn, "dim_VehicleTypeValue", "idVehicleTypeValue", callback)
        map_ids_maneuver = crear_mapa_ids(conn, "dim_ManeuverType", "idManeuverType", callback)
        map_ids_consequence = crear_mapa_ids(conn, "dim_ConsequenceType", "idConsequenceType", callback)
        
        # Mapa de Búsqueda (LaneValue -> idLane)
        map_lane = crear_mapa_simple(conn, "dim_Lane", "idLane", "LaneValue")
        
        # Mapa de Búsqueda (Registration -> idVehicleDescription)
        # Se carga ahora y se actualiza en vivo durante el bucle
        map_vehicle_desc = crear_mapa_simple(conn, "dim_VehicleDescription", "idVehicleDescription", "Registration")
        
        callback(f"Mapas creados. {len(map_ids_accidents)} accidentes válidos encontrados.")
        
        # --- 4. Procesar Archivos (Uno por uno con Transacción) ---
        dirty_files = set()
        failed_rows_details = []

        for archivo_csv in nuevos_archivos_csv:
            callback(f" Procesando Archivo: {os.path.basename(archivo_csv)}")
            
            try:
                df = pd.read_csv(
                    archivo_csv, 
                    encoding="utf-8", 
                    dtype=str,
                    sep=','
                )
                df = df.where(pd.notna(df), None) # Convertir NaNs a None
                
            except Exception as e:
                callback(f" ERROR: No se pudo leer {archivo_csv}. Error: {e}")
                dirty_files.add(os.path.basename(archivo_csv))
                continue

            file_had_errors = False
            
            try:
                cursor.execute("BEGIN IMMEDIATE")
                
                for index, row in df.iterrows():
                    
                    try:
                        # --- A. Obtener datos de la fila ---
                        id_acc = str(row['ID Accidente']).strip() if row['ID Accidente'] else None
                        registration = str(row['Patente']).strip() if row['Patente'] else None
                        brand = str(row['Marca']).strip() if row['Marca'] else "Sin Marca"

                        # --- B. Obtener valores de FK (con default 0 = 'Sin dato') ---
                        val_service = safe_int_convert(row.get("Servicio"), 0)
                        val_veh_type = safe_int_convert(row.get("Tipo Vehículo"), 0)
                        val_maneuver = safe_int_convert(row.get("Maniobra"), 0)
                        val_consequence = safe_int_convert(row.get("Consecuencia"), 0)
                        val_lane = safe_int_convert(row.get("Pista/Vía"), None) # None si está vacío

                        # --- C. Validar todos los FKs antes de insertar ---
                        error_fk = None
                        if not id_acc or not registration:
                            error_fk = f"Campo obligatorio vacío: ID Accidente ({id_acc}) o Patente ({registration})"
                        elif id_acc not in map_ids_accidents:
                            error_fk = f"idAccident '{id_acc}' no existe en factAccident"
                        elif val_service not in map_ids_service:
                            error_fk = f"idServiceType '{val_service}' no existe en dim_ServiceType"
                        elif val_veh_type not in map_ids_vehicletypevalue:
                            error_fk = f"idVehicleTypeValue '{val_veh_type}' no existe en dim_VehicleTypeValue"
                        elif val_maneuver not in map_ids_maneuver:
                            error_fk = f"idManeuverType '{val_maneuver}' no existe en dim_ManeuverType"
                        elif val_consequence not in map_ids_consequence:
                            error_fk = f"idConsequenceType '{val_consequence}' no existe en dim_ConsequenceType"

                        if error_fk:
                            raise sqlite3.IntegrityError(error_fk)

                        # Búsqueda de Pista (Lane)
                        id_lane_fk = None
                        if val_lane is not None:
                            id_lane_fk = map_lane.get(val_lane)
                            if id_lane_fk is None:
                                raise sqlite3.IntegrityError(f"LaneValue '{val_lane}' no existe en dim_Lane")
                        
                        # --- D. Si todo es válido, proceder con la lógica de "Upsert" ---
                        
                        # 1. Upsert dim_VehicleDescription
                        id_vd = map_vehicle_desc.get(registration)
                        if not id_vd:
                            cursor.execute("INSERT OR IGNORE INTO dim_VehicleDescription (Registration, Brand) VALUES (?, ?)", 
                                        (registration, brand))
                            id_vd = cursor.lastrowid
                            
                            if id_vd == 0: # Fue ignorado (ya existía pero no estaba en el mapa)
                                cursor.execute("SELECT idVehicleDescription FROM dim_VehicleDescription WHERE Registration = ?", (registration,))
                                id_vd = cursor.fetchone()[0]
                            
                            map_vehicle_desc[registration] = id_vd # Actualizar mapa en vivo

                        # 2. Insertar Hecho (factVehicleAccident)
                        cursor.execute("INSERT INTO factVehicleAccident (idAccident, idVehicleDescription) VALUES (?, ?)", 
                                    (id_acc, id_vd))
                        idVA = cursor.lastrowid # Obtener el PK del hecho insertado

                        # 3. Insertar Puentes
                        cursor.execute("INSERT OR IGNORE INTO bridge_VehicleAccident_ServiceType (idVehicleAccident, idServiceType) VALUES (?, ?)", (idVA, val_service))
                        cursor.execute("INSERT OR IGNORE INTO bridge_VehicleAccident_VehicleTypeValue (idVehicleAccident, idVehicleTypeValue) VALUES (?, ?)", (idVA, val_veh_type))
                        cursor.execute("INSERT OR IGNORE INTO bridge_VehicleAccident_ManeuverType (idVehicleAccident, idManeuverType) VALUES (?, ?)", (idVA, val_maneuver))
                        cursor.execute("INSERT OR IGNORE INTO bridge_VehicleAccident_ConsequenceType (idVehicleAccident, idConsequenceType) VALUES (?, ?)", (idVA, val_consequence))
                        
                        if id_lane_fk:
                            cursor.execute("INSERT OR IGNORE INTO bridge_VehicleAccident_Lane (idVehicleAccident, idLane) VALUES (?, ?)", (idVA, id_lane_fk))

                    except Exception as e_row:
                        # Error a nivel de FILA
                        file_had_errors = True
                        failed_rows_details.append( (row.to_dict(), str(e_row), os.path.basename(archivo_csv)) )

                # --- E. Decidir Commit o Rollback para el ARCHIVO ---
                if file_had_errors:
                    dirty_files.add(os.path.basename(archivo_csv))
                    conn.rollback()
                    callback(f" ADVERTENCIA: Se encontraron errores en {os.path.basename(archivo_csv)}. Se revirtió la carga de este archivo.")
                else:
                    conn.commit()
                    callback(f" Archivo {os.path.basename(archivo_csv)} cargado exitosamente.")

            except Exception as e_file:
                # Error a nivel de TRANSACCIÓN/ARCHIVO
                conn.rollback()
                callback(f"  ERROR de Base de Datos al procesar {os.path.basename(archivo_csv)}: {e_file}.")
                dirty_files.add(os.path.basename(archivo_csv))
                failed_rows_details.append( (None, str(e_file), os.path.basename(archivo_csv)) )

        if conn:
            conn.commit()
            print("Asegurando guardado de datos (Commit final).")

        # --- 5. Registrar Logs y Reportar Errores ---
        callback("...procesamiento de Ficha 1 finalizado.")

        callback("Registrando archivos procesados en el log (Ficha 1)...")
        now_str = datetime.now().isoformat()
        
        files_to_log = []
        for f_path in nuevos_archivos_csv:
            f_name = os.path.basename(f_path)
            if f_name not in dirty_files:
                files_to_log.append((f_name, now_str))
                
        if files_to_log:
            cursor.executemany("INSERT OR IGNORE INTO etl_log_ficha1_vehiculos (FileName, LoadedTimestamp) VALUES (?, ?)", files_to_log)
            callback(f"{len(files_to_log)} archivos 100% limpios registrados en el log etl_log_ficha1_vehiculos.")
        
        if dirty_files:
            callback(f"ADVERTENCIA: {len(dirty_files)} archivos con errores no se registraron en etl_log_ficha1_vehiculos y serán reintentados.")

        if failed_rows_details:
            callback("\n--- ⚠️ Reporte Detallado de Errores de Carga (Ficha 1) ---")
            callback(f"Se omitieron {len(failed_rows_details)} filas por errores de FK, validación o de BD.")
            for i, (fila, error, archivo) in enumerate(failed_rows_details[:10]): # Mostrar solo los primeros 10
                callback(f"\n - Archivo: {archivo}")
                callback(f" Error: {error}")
                if fila:
                    callback(f" Datos: ID Accidente {fila.get('ID Accidente')}, Patente {fila.get('Patente')}")
                    
        conn.commit() # Commit final para los logs
        callback("Proceso ETL para Ficha 1 completado.")

    except Exception as e:
        callback(f"Error: {e}")
        if 'conn' in locals():
            conn.rollback()
        raise
    finally:
        if 'conn' in locals():
            conn.close()
            print("Conexión cerrada.")
