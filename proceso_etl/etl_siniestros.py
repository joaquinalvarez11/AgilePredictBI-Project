import pandas as pd
import numpy as np
import re
import os
import glob
from pathlib import Path
from datetime import datetime, date, time
import csv
from config_manager import obtener_ruta

class ETLSiniestralidad():
    __RENAMINGS = {
        "Column1": "Correlativo",
        "Column2": "ID Contrato",
        "Column3": "ID Tramo",
        "Column4": "Administrador",
        "Column5": "Nombre Administrador",
        "Column6": "Código accidente",
        "Column7": "Fecha",
        "Column8": "Hora",
        "Column9": "Km",
        "Column10": "P6",
        "Column11": "P4",
        "Column12": "P2",
        "Column13": "P1",
        "Column14": "P3",
        "Column15": "P5",
        "Column16": "Tramo",
        "Column17": "Tipo Accidente",
        "Column18": "Ubicación Relativa",
        "Column19": "Condiciones del Entorno - Punto Duro",
        "Column20": "Condiciones del Entorno - Defensas Camineras",
        "Column21": "Condiciones del Entorno - Desnivel en la Faja",
        "Column22": "Condiciones del Entorno - Estado cerco",
        "Column23": "Condiciones del Entorno - Trabajos en la Vía",
        "Column24": "Condiciones del Entorno - Banderero",
        "Column25": "Condiciones del Entorno - Velocidad máxima del sector",
        "Column26": "Concurrencia - Carabineros",
        "Column27": "Concurrencia - Ambulancia",
        "Column28": "Concurrencia - Bomberos",
        "Column29": "Concurrencia - Operadora",
        "Column30": "Concurrencia - ITE",
        "Column31": "Consecuencias - Muertos - Conductores",
        "Column32": "Consecuencias - Muertos - Pasajeros",
        "Column33": "Consecuencias - Muertos - Peatones",
        "Column34": "Consecuencias - Muertos - S/ identificar",
        "Column35": "Consecuencias - Graves - Conductores",
        "Column36": "Consecuencias - Graves - Pasajeros",
        "Column37": "Consecuencias - Graves - Peatones",
        "Column38": "Consecuencias - Graves - S/ identificar",
        "Column39": "Consecuencias - Menos Graves - Conductores",
        "Column40": "Consecuencias - Menos Graves - Pasajeros",
        "Column41": "Consecuencias - Menos Graves - Peatones",
        "Column42": "Consecuencias - Menos Graves - S/ identificar",
        "Column43": "Consecuencias - Leves - Conductores",
        "Column44": "Consecuencias - Leves - Pasajeros",
        "Column45": "Consecuencias - Leves - Peatones",
        "Column46": "Consecuencias - Leves - S/ identificar",
        "Column47": "Consecuencias - Ilesos - Conductores",
        "Column48": "Consecuencias - Ilesos - Pasajeros",
        "Column49": "Consecuencias - Ilesos - Peatones",
        "Column50": "Consecuencias - Ilesos - S/ identificar",
        "Column51": "Condición calzada",
        "Column52": "Estado Atmosférico",
        "Column53": "Luminosidad",
        "Column54": "Luz artificial",
        "Column55": "Causa Probable (Contratos de Corredores urbanos)",
        "Column56": "Causa Probable (Contratos de Interurbanos y Urbanos) - Falla humana",
        "Column57": "Causa Probable (Contratos de Interurbanos y Urbanos) - Falla mecánica",
        "Column58": "Causa Probable (Contratos de Interurbanos y Urbanos) - Reventón neumático",
        "Column59": "Causa Probable (Contratos de Interurbanos y Urbanos) - Peatón en la vía",
        "Column60": "Causa Probable (Contratos de Interurbanos y Urbanos) - Ciclista en la vía",
        "Column61": "Causa Probable (Contratos de Interurbanos y Urbanos) - Animal u obstáculo en la vía",
        "Column62": "Causa Probable (Contratos de Interurbanos y Urbanos) - Pavimento resbaladizo",
        "Column63": "Causa Probable (Contratos de Interurbanos y Urbanos) - Carga mal estibada",
        "Column64": "Causa Probable (Contratos de Interurbanos y Urbanos) - Condición climática",
        "Column65": "Causa Probable (Contratos de Interurbanos y Urbanos) - No definida",
        "Column66": "Daños Ocasionados a la Infraestructura vial",
        "Column67": "Descripción del Accidente",
    }
    
    __MESES_TEXTO = {
        'ENERO': '01', 'FEBRERO': '02', 'MARZO': '03', 'ABRIL': '04', 'MAYO': '05', 'JUNIO': '06',
        'JULIO': '07', 'AGOSTO': '08', 'SEPTIEMBRE': '09', 'OCTUBRE': '10', 'NOVIEMBRE': '11', 'DICIEMBRE': '12'
    }

    # -----------------------------------------
    # MÉTODOS PÚBLICOS
    # -----------------------------------------
    def __init__(self):
        """Inicializa las rutas usando el config_manager."""
        # Se obtienen las rutas del archivo de configuración centralizado
        base_brutos = obtener_ruta('ruta_excel_bruto')
        base_limpios = obtener_ruta('ruta_csv_limpio')
        
        self.__ruta_bruta_general = base_brutos
        self.__ruta_limpia_base = os.path.join(base_limpios, 'Siniestralidad', 'Ficha 0') # La salida sí es específica
        self.__CSV_SEP = "|"
        os.makedirs(self.__ruta_limpia_base, exist_ok=True)
        self.__log("ETL de Siniestralidad (Ficha 0) inicializada.")

    def procesar_archivo(self, ruta_archivo_excel):
            """
            Punto de entrada para el controlador. Procesa un único archivo que se le entrega.
            Devuelve True si tuvo éxito, False si falló.
            """
            self.__log(f"Iniciando procesamiento para: {ruta_archivo_excel}")

            nombre_base = Path(ruta_archivo_excel).stem

            # === LISTA DE OBSERVACIONES ===
            obs_calidad = []

            # Construir la ruta de salida completa
            anio_str = self.__extraer_anio_de_ruta(ruta_archivo_excel)
            if not anio_str:
                obs_calidad.append("No se determinó año para '{nombre_base}'. Se guardará en raíz.")
                anio_str = ""

            ruta_salida_anio = os.path.join(self.__ruta_limpia_base, anio_str)
            os.makedirs(ruta_salida_anio, exist_ok=True)
            ruta_csv_salida = os.path.join(ruta_salida_anio, f"{nombre_base}_Limpio.csv")

            # Verificar si el archivo ya fue procesado
            if os.path.exists(ruta_csv_salida):
                self.__log(f"El archivo '{nombre_base}' ya ha sido procesado. Saltando.")
                # Es importante notificar al controlador que no hubo error, simplemente no se hizo nada nuevo.
                return True

            try:
                # Llamada a la lógica principal de transformación
                df_transformado = self.__transformar_excel(ruta_archivo_excel, obs_calidad, anio_carpeta=anio_str)

                # Verificar si la transformación produjo un DataFrame válido
                if df_transformado is None or df_transformado.empty:
                    self.__log(f"ADVERTENCIA: '{nombre_base}' no produjo datos válidos.")
                    # Consideramos esto un éxito parcial (no error), pero no guardamos nada.
                    return True

                # Guardar el resultado si la transformación fue exitosa
                self.__guardar_csv(df_transformado, ruta_csv_salida)
                
                # --- IMPRIMIR REPORTE AL FINAL ---
                if obs_calidad:
                    self.__log(f"--- REPORTE CALIDAD: {nombre_base} ---")
                    for o in obs_calidad:
                        self.__log(f"   • {o}")
                    self.__log("------------------------------------------")
                
                return True # Indicar éxito al controlador

            except Exception as e:
                self.__log(f"ERROR CRÍTICO durante la transformación de '{nombre_base}': {e}")
                # Propagar la excepción para que el controlador (deteccion_auto) la capture
                raise e


    # -----------------------------------------
    # MÉTODOS PRIVADOS - Lógica del ETL
    # -----------------------------------------
    def __extraer_anio_de_ruta(self, ruta_archivo):
        """ Recuperado: Extrae el año de la carpeta contenedora """
        try:
            parts = Path(ruta_archivo).parts
            for part in reversed(parts[:-1]):
                if len(part) == 4 and part.isdigit():
                    return part
        except: pass
        return None
    
    def __extraer_info_flexible(self, nombre_archivo, anio_carpeta):
        nombre_clean = nombre_archivo.upper()
        
        # 1. Intentar buscar mes en texto (ENERO, FEBRERO...)
        mes_num = "00"
        for mes_nom, num_str in self.__MESES_TEXTO.items():
            if mes_nom in nombre_clean:
                mes_num = num_str
                break
        
        # 2. Intentar buscar año 4 dígitos en nombre
        match_anio = re.search(r'(20\d{2})', nombre_clean)
        anio_final = match_anio.group(1) if match_anio else anio_carpeta
        
        if not anio_final: anio_final = "1900"
        
        prefijo = f"{anio_final}{mes_num}"
        return prefijo, anio_final

    def __transformar_excel(self, ruta_excel, obs_calidad, anio_carpeta=""):
        """Orquesta el proceso de transformación para un archivo Excel."""
        self.__log(f"Transformando archivo: {Path(ruta_excel).name}")
        # 1. Leer y encontrar encabezado
        df, _ = self.__read_raw_sheet(ruta_excel)
        if df is None:
            # Si no se puede leer, no podemos continuar.
            obs_calidad.append("No se encontró encabezado 'Correlativo'.")
            return None # Devolver None para indicar fallo controlado

        # Se usará esta fecha para TODOS los IDs del archivo, unificando el criterio
        nombre_archivo = Path(ruta_excel).name
        prefijo_fecha_archivo, anio_archivo = self.__extraer_info_flexible(nombre_archivo, anio_carpeta)
        
        if prefijo_fecha_archivo.endswith("00"):
            obs_calidad.append(f"Advertencia: No se detectó MES en el nombre del archivo. IDs usarán mes '00'.")

        # 2. Limpieza inicial
        df = df.rename(columns=self.__RENAMINGS)
        df = self.__limpieza_inicial(df, prefijo_fecha_archivo, anio_archivo, obs_calidad)
        
        if df is None or df.empty:
             return None

        # 3. Unpivot y combinar
        df = self.__unpivot_y_combinar(df)
        if df.empty:
             return None

        # 4. Imputar y expandir
        df = self.__imputar_y_expandir(df)
        if df.empty:
             return None

        # 5. Pasos finales
        df = self.__pasos_finales(df)
        if df.empty:
             return None

        self.__log(f"Transformación completada exitosamente. {len(df)} filas generadas.")
        return df

    def __limpieza_inicial(self, df, prefijo_fecha_para_id, anio_archivo, obs_calidad):
        """Realiza los primeros pasos de limpieza, formato y creación de ID."""
        # 1. === LIMPIEZA CRÍTICA DE CORRELATIVO ===
        df["Correlativo"] = pd.to_numeric(df["Correlativo"], errors='coerce')
        
        filas_antes = len(df)
        # En lugar de solo dropna, exigimos que sea mayor a 0. Esto elimina NaNs, ceros y cualquier basura numérica.
        df = df[df["Correlativo"] > 0]
        filas_despues = len(df)
        
        self.__log(f"DEBUG: Filas antes: {filas_antes} | Filas después: {filas_despues} (Se borraron {filas_antes - filas_despues})")

        diff = filas_antes - filas_despues
        if diff > 0:
            obs_calidad.append(f"Se eliminaron {diff} filas sin 'Correlativo' numérico válido (posibles filas fantasmas).")

        if df.empty: return df

        # 2. === SOLUCIÓN A DUPLICADOS ===
        # Si existe el mismo correlativo repetido, nos quedamos con el primero.
        columnas_clave = ["Correlativo"]
        
        if "Descripción del Accidente" in df.columns:
            # Aseguramos que la descripción sea string y quitamos espacios extra para comparar bien
            df["Descripción del Accidente"] = df["Descripción del Accidente"].astype(str).str.strip()
            columnas_clave.append("Descripción del Accidente")

        # Chequeo de duplicados
        duplicados = df.duplicated(subset=columnas_clave, keep='first').sum()
        
        if duplicados > 0:
            df = df.drop_duplicates(subset=columnas_clave, keep='first')
            obs_calidad.append(f"Se eliminaron {duplicados} registros duplicados (Criterio: {columnas_clave}).")

        # Conversión final del Correlativo
        df["Correlativo"] = df["Correlativo"].astype('Int64')
        
        # === Detectar y corregir columnas 'Luminosidad' y 'Estado Atmosférico' invertidas ===
        try:
            if "Luminosidad" in df.columns and "Estado Atmosférico" in df.columns:
                self.__log("Iniciando chequeo de columnas invertidas (Luminosidad/Estado Atmosférico)...")
                
                # Estos son los únicos valores que 'Luminosidad' DEBERÍA tener.
                valid_lum = {0, 1, 2, 3, 4} 
                
                # Convertir la columna 'Luminosidad' a números para chequear
                lum_values_numeric = pd.to_numeric(df["Luminosidad"], errors='coerce')
                
                # Encontrar valores que NO están en el set válido (y no son NaN)
                # Ej: Si encuentra 5 o 6, esta máscara será True
                invalid_lum_mask = ~lum_values_numeric.isin(valid_lum) & lum_values_numeric.notna()
                
                if invalid_lum_mask.any():
                    # Si hay CUALQUIER valor inválido (ej: 5, 6), asumimos que las columnas están cambiadas
                    invalid_samples = list(lum_values_numeric[invalid_lum_mask].unique())[:3] # Muestra hasta 3
                    obs_calidad.append(f"Columnas invertidas detectadas (Lum/Atm). Muestras inválidas en Lum: {invalid_samples}. Se realizó intercambio.")
                    
                    # Guardar temporalmente, hacer el swap
                    lum_temp_data = df["Luminosidad"].copy()
                    df["Luminosidad"] = df["Estado Atmosférico"]
                    df["Estado Atmosférico"] = lum_temp_data
                    
        except Exception as e_swap:
            obs_calidad.append(f"Error chequeando columnas invertidas: {e_swap}")
        
        # Limpieza Km/Hora
        self.__log("Iniciando limpieza y preparación de datos...")
        if "Km" in df.columns: df["Km"] = df["Km"].apply(self.__fix_km)
        if "Hora" in df.columns: df["Hora"] = df["Hora"].apply(self.__fix_time)

        # Control de Filas Eliminadas por falta de Correlativo
        filas_antes = len(df)
        df = df.dropna(subset=["Correlativo"])
        # Asegurarse que Correlativo no sea un string vacío después de quitar NaNs
        df = df[df["Correlativo"].astype(str).str.strip() != ""]
        filas_despues = len(df)

        diff = filas_antes - filas_despues
        if diff > 0:
            obs_calidad.append(f"Se eliminaron {diff} filas sin 'Correlativo' válido.")
        
        if df.empty: return df

        cols_to_drop = [c for c in df.columns if 'Column' in str(c)] + [
            "ID Contrato", "ID Tramo", "Administrador", "Nombre Administrador"
        ]
        df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])

        if len(df) > 0 and pd.isna(df.iloc[-1].get("Correlativo")):
            df = df.iloc[:-1]

        # Conversión de tipos
        df["Correlativo"] = pd.to_numeric(df["Correlativo"], errors='coerce').astype('Int64')
        # ===  Usar función robusta __fix_fecha ===
        self.__log(f"Normalizando columna 'Fecha' usando el año: {anio_archivo}")
        
        df["Fecha"] = df["Fecha"].apply(self.__fix_fecha, anio_default=anio_archivo)
        fechas_nulas = df["Fecha"].isna().sum()
        if fechas_nulas > 0:
            obs_calidad.append(f"Hay {fechas_nulas} filas con Fecha inválida o vacía (se intentó usar año default {anio_archivo}).")
        
        for col in ["P6", "P4", "P2", "P1", "P3", "P5", "Tramo"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

        # Crear ID Accidente con fecha de archivo
        self.__log(f"Generando 'ID Accidente' con prefijo: {prefijo_fecha_para_id}")
        df["ID Accidente"] = df["Correlativo"].apply(
            lambda correl: f"ACC-{prefijo_fecha_para_id}-{correl:03d}" if pd.notna(correl) else None)
        self.__log("Preparación inicial completada.")
        return df

    def __unpivot_y_combinar(self, df):
        """Realiza la lógica central de anulación de dinamización y producto cartesiano."""
        self.__log("Iniciando transformación principal (unpivot y merge)...")
        context_cols = [c for c in [
            "ID Accidente", "Fecha", "Hora", "Km", "P1", "P2", "P3", "P4", "P5", "P6",
            "Tramo", "Tipo Accidente", "Ubicación Relativa", "Ubicacion Relativa",
            "Condición calzada", "Luminosidad", "Estado Atmosférico", "Luz artificial",
            "Daños Ocasionados a la Infraestructura vial", "Descripción del Accidente"
        ] if c in df.columns]

        df_cond = self.__unpivot_section(df, ["ID Accidente"], "Condiciones del Entorno", ["Condiciones del Entorno", "Valor Condiciones del Entorno"])
        df_conc = self.__unpivot_section(df, ["ID Accidente"], "Concurrencia", ["Concurrencia", "Valor Concurrencia"])
        df_consq = self.__unpivot_section(df, ["ID Accidente"], "Consecuencias", ["Consecuencia", "Afectado", "Cantidad Afectados"])
        df_causa = self.__unpivot_section(df, ["ID Accidente"], "Causa Probable", ["Causa Probable", "Valor Causa Probable"])

        df_base = df[context_cols].drop_duplicates(subset=["ID Accidente"]).reset_index(drop=True)
        work_df = df_base.copy()
        for seccion_df in [df_cond, df_conc, df_consq, df_causa]:
            if len(seccion_df.columns) > 1:
                work_df = pd.merge(work_df, seccion_df, on="ID Accidente", how="left")
        
        self.__log("Merges completados.")
        return work_df

    def __imputar_y_expandir(self, df):
        """Maneja los casos sin consecuencias y expande columnas con múltiples valores."""
        self.__log("Imputando valores y expandiendo columnas...")
        mask_sin_consecuencias = df["Consecuencia"].isnull()
        df.loc[mask_sin_consecuencias, "Consecuencia"] = "Ninguna"
        df.loc[mask_sin_consecuencias, "Afectado"] = "N/A"
        df.loc[mask_sin_consecuencias, "Cantidad Afectados"] = 0
        df["Cantidad Afectados"] = df["Cantidad Afectados"].astype(int)

        cols_to_expand = [
            "Valor Condiciones del Entorno", "Tipo Accidente", "Ubicación Relativa",
            "Condición calzada", "Luminosidad", "Estado Atmosférico", "Luz artificial"
        ]
        for col in cols_to_expand:
            col_name_in_df = col
            if col == "Ubicación Relativa" and col not in df.columns and "Ubicacion Relativa" in df.columns:
                col_name_in_df = "Ubicacion Relativa"
            
            if col_name_in_df in df.columns:
                df = self.__split_and_explode(df, col_name_in_df, col)
        
        self.__log("Imputación y expansión completadas.")
        return df

    def __pasos_finales(self, df):
        """Aplica los filtros finales y crea columnas derivadas."""
        if "Descripción del Accidente" in df.columns:
            cond_desc = df["Descripción del Accidente"].notnull() & (df["Descripción del Accidente"].astype(str).str.strip() != "")
            df = df[cond_desc].reset_index(drop=True)

        df["FECHA/HORA"] = df.apply(self.__combine_fecha_hora, axis=1)
        self.__log("Proceso final completado.")
        return df
        
    # -----------------------------------------
    # MÉTODOS PRIVADOS - FUNCIONES AUXILIARES
    # -----------------------------------------

    def __log(self, msg):
        """Imprime un mensaje con marca de tiempo."""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{now}] [ETL Siniestralidad] {msg}")

    def __read_raw_sheet(self, path):
        """Lee la hoja de Excel y localiza la fila de encabezado."""
        self.__log(f"Cargando archivo: {path}")
        try:
            # --- Cambio: Añadir engine ---
            engine = 'xlrd' if path.lower().endswith('.xls') else 'openpyxl'
            self.__log(f"Usando motor: {engine}") # Ya usa __log

            df_raw = pd.read_excel(path, header=None, engine=engine, dtype=object)
            first_col = df_raw.iloc[:, 0].astype(str).fillna("")
            matches = first_col[first_col.str.strip().str.lower() == "correlativo"]

            if matches.empty:
                 self.__log("Error: No se encontró 'Correlativo'.") # Ya usa __log
                 return None, None

            header_row_idx = matches.index[0]
            self.__log(f"Encabezado encontrado en fila Excel {header_row_idx + 2}")
            df_after = df_raw.iloc[header_row_idx + 1:].reset_index(drop=True)
            if not df_after.empty:
                 df_after.columns = [f"Column{i+1}" for i in range(df_after.shape[1])]
            df_after = df_after.dropna(how='all').copy()
            return df_after, header_row_idx
        except Exception as e:
            self.__log(f"Error al leer Excel {path}: {e}") # Ya usa __log
            return None, None

    def __unpivot_section(self, df, id_vars, section_name, new_col_names):
        """Lógica de anulación de dinamización para una sección específica."""
        cols_to_unpivot = [c for c in df.columns if section_name in str(c)]
        if not cols_to_unpivot:
            return pd.DataFrame({id_vars[0]: df[id_vars[0]].unique()})

        unpivoted = df.melt(id_vars=id_vars, value_vars=cols_to_unpivot, var_name="Atributo", value_name="Valor")
        unpivoted = unpivoted.dropna(subset=['Valor'])

        is_text_range = unpivoted['Valor'].astype(str).str.contains('-')
        numeric_values = pd.to_numeric(unpivoted.loc[~is_text_range, 'Valor'], errors='coerce').fillna(0)
        
        keep_mask = is_text_range | (numeric_values != 0)
        
        is_not_numeric_or_range = pd.to_numeric(unpivoted['Valor'], errors='coerce').isna() & ~is_text_range
        keep_mask = keep_mask | is_not_numeric_or_range

        unpivoted = unpivoted[keep_mask]
        unpivoted = unpivoted[unpivoted['Valor'].astype(str).str.strip().str.upper() != 'FALSE']

        if unpivoted.empty:
            return pd.DataFrame({id_vars[0]: df[id_vars[0]].unique()})

        if section_name == "Consecuencias":
            unpivoted['Atributo'] = unpivoted['Atributo'].str.replace(f"{section_name} - ", "", regex=False)
            split_data = unpivoted['Atributo'].str.split(" - ", n=1, expand=True)
            unpivoted[new_col_names[0]] = split_data.get(0)
            unpivoted[new_col_names[1]] = split_data.get(1)
        elif section_name == "Causa Probable":
            unpivoted[new_col_names[0]] = unpivoted['Atributo'].str.replace(
                "Causa Probable (Contratos de Interurbanos y Urbanos) - ", "", regex=False
            ).str.replace(
                "Causa Probable (Contratos de Corredores urbanos)", "Corredores urbanos", regex=False
            )
        else:
            unpivoted[new_col_names[0]] = unpivoted['Atributo'].str.replace(f"{section_name} - ", "", regex=False)

        unpivoted[new_col_names[-1]] = unpivoted["Valor"]
        return unpivoted[[id_vars[0]] + new_col_names]


    # Funciones como métodos privadas
    
    def __fix_km(self, value):
        if pd.isna(value): return np.nan
        s = str(value).strip().replace("–", "-")
        if "+" in s:
            parts = s.split("+")
            if len(parts) > 1 and parts[1]:
                return self.__try_float(f"{parts[0]}.{parts[1][0]}")
        s2 = s.replace("-", ".").replace(",", ".")
        return self.__try_float(s2)

    def __try_float(self, s):
        try: return float(s)
        except (ValueError, TypeError): return np.nan

    def __fix_time(self, v):
        if pd.isna(v): return pd.NaT
        if isinstance(v, (pd.Timestamp, datetime)): return pd.to_datetime(v).time()
        if isinstance(v, (int, float)):
            try:
                h = int(v)
                m = int(round((v - h) * 100))
                if m >= 60: m = m % 60
                return time(hour=h % 24, minute=m)
            except Exception: return pd.NaT
        s = str(v).strip()
        try: return pd.to_datetime(s, errors="coerce").time()
        except Exception: return pd.NaT

    def __fix_fecha(self, value, anio_default):
        """
        Intenta normalizar una fecha que puede venir en múltiples formatos
        (DD/MM/YY, DD-MM-YY, DD.MM, DD-MM, números seriales de Excel, etc.).
        Usa 'anio_default' (del nombre de archivo) si la fecha no tiene año.
        """
        if pd.isna(value):
            return pd.NaT

        # 1. Si ya es una fecha (leída correctamente por pandas)
        if isinstance(value, (datetime, date)):
            return value.date() if isinstance(value, datetime) else value

        # 2. Si es un número serial de Excel (común en .xls)
        if isinstance(value, (int, float)):
            try:
                # El origen de fechas de Excel en Windows es 1899-12-30
                return pd.to_datetime(value, unit='D', origin='1899-12-30').date()
            except Exception:
                # Si falla, podría ser un float como 13.01 (13 de enero), tratar como string
                pass 

        # 3. Tratar como string para formatos DD/MM/YY, DD/MM, DD.MM, etc.
        s = str(value).strip().replace('.', '/').replace('-', '/')
        parts = s.split('/')

        try:
            if len(parts) == 3: # Formato "DD/MM/YY" o "DD/MM/YYYY"
                d, m, y = int(parts[0]), int(parts[1]), int(parts[2])
                if y < 100: y += 2000 # Convierte "16" a "2016"
                return date(y, m, d)

            if len(parts) == 2: # Formato "DD/MM" (ej: "13.1" o "13-01")
                d, m = int(parts[0]), int(parts[1])
                y = int(anio_default) # Usar el año del nombre del archivo
                return date(y, m, d)
        except Exception:
            # Falló el parseo manual
            pass

        # 4. Último intento: dejar que Pandas intente con dayfirst=True
        try:
            return pd.to_datetime(s, errors='coerce', dayfirst=True).date()
        except Exception:
            return pd.NaT # Falla definitiva

    def __split_and_explode(self, df, column, new_column):
        if column not in df.columns: return df
        df_copy = df.copy()
        df_copy[new_column] = df_copy[column].astype(str).replace({'nan': None, 'None': None})
        is_false = df_copy[new_column].str.strip().str.upper() == "FALSE"
        df_copy.loc[is_false, new_column] = "0"
        df_copy[new_column] = df_copy[new_column].str.split("-")
        exploded = df_copy.explode(new_column).reset_index(drop=True)
        exploded[new_column] = exploded[new_column].str.strip()
        return exploded

    def __make_id_acc(self, row):
        try:
            correl = int(row["Correlativo"])
            fecha = row["Fecha"]
            fecha_text = "000000" if pd.isna(fecha) else fecha.strftime("%Y%m")
            return f"ACC-{fecha_text}-{correl:03d}"
        except (ValueError, TypeError): return None

    def __combine_fecha_hora(self, row):
        f = row.get("Fecha")
        h = row.get("Hora")
        if pd.isna(f) or pd.isna(h): return pd.NaT
        try: return pd.to_datetime(f"{f} {h}")
        except Exception: return pd.NaT
        
    def __guardar_csv(self, df, ruta_csv):
        """Guarda el DataFrame final en un archivo CSV."""
        self.__log(f"Guardando CSV limpio en: {ruta_csv}")
        try:
            os.makedirs(os.path.dirname(ruta_csv), exist_ok=True)
            with open(ruta_csv, "w", encoding="utf-8-sig", newline='') as f:
                f.write(f"sep={self.__CSV_SEP}\n")
                df.to_csv(f, sep=self.__CSV_SEP, index=False, quoting=csv.QUOTE_ALL)
            self.__log(f"CSV guardado correctamente. Filas finales: {len(df)}")
        except Exception as e:
            self.__log(f"Error al guardar CSV: {e}")
            raise