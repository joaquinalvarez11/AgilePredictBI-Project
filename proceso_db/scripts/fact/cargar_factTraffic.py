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
        ruta_db = obtener_ruta("ruta_database")
        ruta_csv_limpio = obtener_ruta("ruta_csv_limpio")
        ruta_base_csv_trafico = os.path.join(ruta_csv_limpio, "Tráfico Mensual")
        return ruta_db, ruta_base_csv_trafico
    except Exception as e:
        raise RuntimeError(f"Error config: {e}")

# === 2. Helper ===
def safe_int_convert(value, default=None):
    val_num = pd.to_numeric(value, errors='coerce')
    if pd.isna(val_num):
        return default
    return int(val_num)

# === 3. ETL Principal ===
def run(callback):
    conn = None
    try:
        ruta_db, ruta_base_csv_trafico = get_paths()
        conn = sqlite3.connect(ruta_db)
        cursor = conn.cursor()
        cursor.execute("PRAGMA foreign_keys = ON;")

        callback("--- Cargando Hechos: factTraffic ---")

        # --- Archivos Procesados ---
        print("Consultando log de archivos ya procesados (Tráfico)...")
        cursor.execute("SELECT FileName FROM etl_log_trafico")
        processed_files = set(row[0] for row in cursor.fetchall())
        callback(f"Se encontraron {len(processed_files)} archivos en el log.")

        # --- Buscar Nuevos ---
        print(f"Buscando archivos CSV en: {ruta_base_csv_trafico}...")
        patron = os.path.join(ruta_base_csv_trafico, '**', '*.csv')
        todos = glob.glob(patron, recursive=True)
        nuevos_archivos_csv = [f for f in todos if os.path.basename(f) not in processed_files]

        if not nuevos_archivos_csv:
            callback("No se encontraron archivos CSV nuevos para Tráfico. El proceso ha finalizado.")
            return
        
        callback(f"Se encontraron {len(nuevos_archivos_csv)} archivos CSV NUEVOS. Iniciando carga...")

        lista_dfs = []
        for f in nuevos_archivos_csv:
            callback(f"  Leyendo: {os.path.basename(f)}")
            try:
                df_temp = pd.read_csv(f, encoding="utf-8", dtype=str)
                df_temp['__SourceFileName'] = os.path.basename(f)
                lista_dfs.append(df_temp)
            except Exception as e:
                callback(f"    ERROR: No se pudo leer {f}: {e}")

        if not lista_dfs:
            callback("Error: Ningún archivo CSV nuevo pudo ser leído. Saliendo.")
            return
            
        df_trafico_full = pd.concat(lista_dfs, ignore_index=True)
        callback(f"\nCarga de CSVs completada. {len(df_trafico_full)} filas totales.")

        # --- Mapas de Dimensiones ---
        print("Creando mapas de dimensiones...")
        def crear_mapa(tabla, col_nombre, col_id):
            df = pd.read_sql(f"SELECT {col_id}, {col_nombre} FROM {tabla}", conn)
            return {str(k).strip().lower(): v for k, v in zip(df[col_nombre], df[col_id])}

        mapa_plaza = crear_mapa("dim_Plaza", "PlazaName", "idPlaza")
        mapa_direccion = crear_mapa("dim_Direction", "DirectionName", "idDirection")
        mapa_categoria = crear_mapa("dim_Category", "CategoryName", "idCategory")
        
        default_plaza = mapa_plaza.get("desconocido")
        default_dir = mapa_direccion.get("sin dato")

        # --- Mapa DateTime (Eficiente) ---
        callback("Creando mapa de dim_DateTime...")
        df_trafico_full['Fecha_dt'] = pd.to_datetime(df_trafico_full['Fecha'], errors='coerce')
        df_trafico_full['Hora_int'] = pd.to_numeric(df_trafico_full['Hora'], errors='coerce').fillna(0).astype(int)
        
        df_trafico_full['LookupKey'] = df_trafico_full.apply(
            lambda r: f"{r['Fecha_dt'].strftime('%Y-%m-%d')} {r['Hora_int']:02d}:00:00" if pd.notna(r['Fecha_dt']) else None,
            axis=1
        )
        keys = tuple(df_trafico_full['LookupKey'].dropna().unique())
        mapa_datetime = {}
        if keys:
            q = pd.read_sql(f"SELECT idDateTime, DateTime FROM dim_DateTime WHERE DateTime IN {keys}", conn)
            mapa_datetime = dict(zip(q['DateTime'], q['idDateTime']))
        callback("Mapas creados.")

        # --- Procesamiento Fila por Fila ---
        failed_rows_details = [] 
        dirty_files = set()
        filas_insertadas = 0
        total_rows = len(df_trafico_full)
        
        callback(f"Procesando {total_rows} filas...")
        start_loop = time.time()
        cursor.execute("BEGIN IMMEDIATE") # Iniciar transacción

        for index, row in df_trafico_full.iterrows():
            source_file = row['__SourceFileName']
            try:
                # 1. Lookups
                nom_plaza = row.get('Plaza')
                id_plaza = mapa_plaza.get(str(nom_plaza).strip().lower(), default_plaza)
                
                nom_dir = row.get('Direccion')
                id_dir = mapa_direccion.get(str(nom_dir).strip().lower(), default_dir)
                
                nom_cat = row.get('Categoria')
                id_cat = None
                if pd.notna(nom_cat) and nom_cat:
                    id_cat = mapa_categoria.get(str(nom_cat).strip().lower())

                id_dt = mapa_datetime.get(row['LookupKey'])
                volumen = safe_int_convert(row['Contar'], default=0)

                # 2. Validación Manual
                err_msg = None
                if id_dt is None: err_msg = f"Fecha no encontrada: {row.get('Fecha')} {row.get('Hora')}"
                elif id_plaza is None: err_msg = f"Plaza desconocida: {nom_plaza}"
                elif id_dir is None: err_msg = f"Dirección desconocida: {nom_dir}"
                elif id_cat is None: err_msg = f"Categoría desconocida: {nom_cat}"

                if err_msg:
                    failed_rows_details.append((f"Fila {index+1}", err_msg, source_file))
                    dirty_files.add(source_file)
                else:
                    # 3. Insertar
                    cursor.execute("""
                        INSERT INTO factTraffic (idDateTime, idPlaza, idDirection, idCategory, trafficVolume) 
                        VALUES (?, ?, ?, ?, ?)
                    """, (id_dt, id_plaza, id_dir, id_cat, volumen))
                    filas_insertadas += 1

            except Exception as e:
                failed_rows_details.append((f"Fila {index+1}", str(e), source_file))
                dirty_files.add(source_file)
            
            # Log de progreso
            if (index + 1) % 50000 == 0:
                elapsed = time.time() - start_loop
                rps = (index + 1) / elapsed
                callback(f"  ... Procesadas {index + 1} ({rps:.0f} filas/seg). Insertadas: {filas_insertadas}.")
                conn.commit()
                cursor.execute("BEGIN IMMEDIATE")

        conn.commit() # Commit final de datos
        callback("...procesamiento finalizado.")

        # --- Logs de Archivos ---
        now_str = datetime.now().isoformat()
        files_to_log = []
        for f in nuevos_archivos_csv:
            fname = os.path.basename(f)
            if fname not in dirty_files:
                files_to_log.append((fname, now_str))
        
        if files_to_log:
            cursor.executemany("INSERT OR IGNORE INTO etl_log_trafico VALUES (?,?)", files_to_log)
            callback(f"{len(files_to_log)} archivos limpios registrados.")
        
        if dirty_files:
            callback(f"ADVERTENCIA: {len(dirty_files)} archivos con errores no se registraron.")

        conn.commit()

        # === REPORTE EJECUTIVO LIMPIO (PARA EL GESTOR) ===
        if failed_rows_details:
            # Encabezado exacto que busca el Gestor (Sin emojis)
            callback("\n--- Reporte Detallado de Errores de Carga (Tráfico) ---")
            callback(f"Se omitieron {len(failed_rows_details)} filas.")
            
            # Mostrar solo los primeros 15 errores
            for i, (fila_idx, error, archivo) in enumerate(failed_rows_details[:15]):
                # Formato exacto: - Archivo: ... / Error: ...
                callback(f"- Archivo: {archivo}")
                callback(f"  Error: {error} ({fila_idx})")
            
            if len(failed_rows_details) > 15:
                callback(f"... y {len(failed_rows_details) - 15} errores más.")

        # String final exacto
        callback("Proceso ETL para Tráfico completado.")

    except Exception as e:
        callback(f"Error crítico Tráfico: {e}")
        if conn: conn.rollback()
        raise 
    finally:
        if conn: conn.close()