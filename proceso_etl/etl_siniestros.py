import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime, time
from pathlib import Path
import csv


class ETLSiniestralidad():
    # Mapeo de columnas en atributo de clase
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
        "Column52": "Luminosidad",
        "Column53": "Estado Atmosférico",
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

    def __init__(self):
        """Inicializa las rutas y configuraciones para el ETL de Siniestralidad."""
        # Se asume que las rutas base vienen de un archivo de configuración
        self.__ruta_bruta = os.path.join(BASE_BRUTOS_PATH, 'Siniestralidad/')
        self.__ruta_limpia = os.path.join(BASE_LIMPIOS_PATH, 'Siniestralidad/')
        self.__CSV_SEP = "|"
        os.makedirs(self.__ruta_limpia, exist_ok=True)
        self.__log("ETL de Siniestralidad inicializada.")

    # -----------------------------------------
    # MÉTODOS PÚBLICOS
    # -----------------------------------------

    def ejecutar_etl(self):
        """
        Punto de entrada principal. Busca el archivo más reciente y ejecuta
        el proceso de transformación si no ha sido procesado antes.
        """
        self.__log("Iniciando ETL de Siniestralidad...")
        archivo_excel = self.__get_most_recent_excel(self.__ruta_bruta)

        if not archivo_excel:
            self.__log("No se encontraron archivos Excel en la carpeta de entrada. Finalizando.")
            return

        nombre_base = Path(archivo_excel).stem
        ruta_csv_salida = os.path.join(self.__ruta_limpia, f"{nombre_base}_Limpio.csv")

        if os.path.exists(ruta_csv_salida):
            self.__log(f"El archivo '{nombre_base}' ya ha sido procesado. Saltando.")
            return

        try:
            df_transformado = self.__transformar_excel(archivo_excel)
            
            # Guardado del archivo
            self.__guardar_csv(df_transformado, ruta_csv_salida)

        except Exception as e:
            self.__log(f"❌ Ocurrió un error crítico durante la transformación de '{nombre_base}': {e}")


    # -----------------------------------------
    # MÉTODOS PRIVADOS - Lógica del ETL
    # -----------------------------------------

    def __transformar_excel(self, ruta_excel):
        """
        Orquesta todo el proceso de transformación para un único archivo Excel.
        """
        # 1. Leer y encontrar encabezado
        df, _ = self.__read_raw_sheet(ruta_excel)
        if df is None:
            raise ValueError("No se pudo leer la hoja o encontrar el encabezado 'Correlativo'.")

        # 2. Limpieza y preparación inicial
        df = df.rename(columns=self.__RENAMINGS)
        df = self.__limpieza_inicial(df)

        # 3. Anulación de dinamización y combinación
        df = self.__unpivot_y_combinar(df)

        # 4. Imputación y expansión de columnas
        df = self.__imputar_y_expandir(df)
        
        # 5. Pasos finales y filtros
        df = self.__pasos_finales(df)

        return df

    def __limpieza_inicial(self, df):
        """Realiza los primeros pasos de limpieza, formato y creación de ID."""
        self.__log("Iniciando limpieza y preparación de datos...")
        if "Km" in df.columns: df["Km"] = df["Km"].apply(self.__fix_km)
        if "Hora" in df.columns: df["Hora"] = df["Hora"].apply(self.__fix_time)

        df = df.dropna(subset=["Correlativo"])
        df = df[df["Correlativo"].astype(str).str.strip() != ""]
        
        cols_to_drop = [c for c in df.columns if 'Column' in str(c)] + [
            "ID Contrato", "ID Tramo", "Administrador", "Nombre Administrador"
        ]
        df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])

        if len(df) > 0 and pd.isna(df.iloc[-1].get("Correlativo")):
            df = df.iloc[:-1]

        # Conversión de tipos
        df["Correlativo"] = pd.to_numeric(df["Correlativo"], errors='coerce').astype('Int64')
        df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce").dt.date
        for col in ["P6", "P4", "P2", "P1", "P3", "P5", "Tramo"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

        # Creación de ID Accidente
        df["ID Accidente"] = df.apply(self.__make_id_acc, axis=1)
        self.__log("✅ Preparación inicial completada.")
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
        
        self.__log("✅ Merges completados.")
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
        
        self.__log("✅ Imputación y expansión completadas.")
        return df

    def __pasos_finales(self, df):
        """Aplica los filtros finales y crea columnas derivadas."""
        self.__log("Aplicando filtros y transformaciones finales...")
        if "Descripción del Accidente" in df.columns:
            cond_desc = df["Descripción del Accidente"].notnull() & (df["Descripción del Accidente"].astype(str).str.strip() != "")
            df = df[cond_desc].reset_index(drop=True)

        df["FECHA/HORA"] = df.apply(self.__combine_fecha_hora, axis=1)
        self.__log("✅ Proceso final completado.")
        return df
        
    # -----------------------------------------
    # MÉTODOS PRIVADOS - FUNCIONES AUXILIARES
    # -----------------------------------------

    def __log(self, msg):
        """Imprime un mensaje con marca de tiempo."""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{now}] [ETL Siniestralidad] {msg}")

    def __get_most_recent_excel(self, folder):
        """Busca el archivo Excel más reciente en una carpeta."""
        self.__log(f"Buscando archivo Excel más reciente en: {folder}")
        patterns = ["*.xlsx", "*.xlsm", "*.xls"]
        files = []
        for p in patterns:
            files.extend(glob.glob(os.path.join(folder, p)))
        if not files:
            return None
        files.sort(key=os.path.getmtime, reverse=True)
        return files[0]

    def __read_raw_sheet(self, path):
        """Lee la hoja de Excel y localiza la fila de encabezado."""
        self.__log(f"Cargando archivo: {path}")
        df_raw = pd.read_excel(path, header=None, engine="openpyxl", dtype=object)
        first_col = df_raw.iloc[:, 0].astype(str).fillna("")
        matches = first_col[first_col.str.strip().str.lower() == "correlativo"]
        
        if matches.empty: return None, None
        
        header_row_idx = matches.index[0]
        df_after = df_raw.iloc[header_row_idx + 1:].reset_index(drop=True)
        df_after.columns = [f"Column{i+1}" for i in range(df_after.shape[1])]
        df_after = df_after.dropna(how='all').copy()
        return df_after, header_row_idx

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
        # ... (código de la función fix_km) ...
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
        # ... (código de la función fix_time) ...
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

    def __split_and_explode(self, df, column, new_column):
        # ... (código de la función split_and_explode) ...
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
        # ... (código de la función make_id_acc) ...
        try:
            correl = int(row["Correlativo"])
            fecha = row["Fecha"]
            fecha_text = "000000" if pd.isna(fecha) else fecha.strftime("%Y%m")
            return f"ACC-{fecha_text}-{correl:03d}"
        except (ValueError, TypeError): return None

    def __combine_fecha_hora(self, row):
        # ... (código de la función combine_fecha_hora) ...
        f = row.get("Fecha")
        h = row.get("Hora")
        if pd.isna(f) or pd.isna(h): return pd.NaT
        try: return pd.to_datetime(f"{f} {h}")
        except Exception: return pd.NaT
        
    def __guardar_csv(self, df, ruta_csv):
        """Guarda el DataFrame final en un archivo CSV."""
        self.__log(f"Guardando CSV limpio en: {ruta_csv}")
        try:
            with open(ruta_csv, "w", encoding="utf-8-sig", newline='') as f:
                f.write(f"sep={self.__CSV_SEP}\n")
                df.to_csv(f, sep=self.__CSV_SEP, index=False, quoting=csv.QUOTE_ALL)
            self.__log(f"🎉 CSV guardado correctamente. Filas finales: {len(df)}")
        except Exception as e:
            self.__log(f"❌ Error al guardar CSV: {e}")
            raise