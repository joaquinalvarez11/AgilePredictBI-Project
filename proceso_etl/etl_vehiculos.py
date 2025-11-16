import pandas as pd
import numpy as np # Asegurar importación de numpy
import unicodedata
import os
import re
from pathlib import Path
from datetime import datetime
from config_manager import obtener_ruta

class ETLVehiculos():

    def __init__(self):
        """Inicializa las rutas usando el config_manager."""
        base_limpios = obtener_ruta('ruta_csv_limpio')
        # Define la ruta base para guardar archivos limpios
        self.__ruta_limpia_base = os.path.join(base_limpios, 'Siniestralidad', 'Ficha 1')
        os.makedirs(self.__ruta_limpia_base, exist_ok=True)
        self.__log("ETL de Vehículos (Ficha 1) inicializada.")

    def __log(self, msg):
        """Imprime un mensaje con marca de tiempo para consistencia."""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{now}] [ETL Vehiculos] {msg}")

    def __sin_tildes(self, txt: str):
        if pd.isna(txt):
            return txt
        # Normaliza y remueve diacríticos (tildes/¨/´)
        return unicodedata.normalize("NFKD", txt).encode("ASCII", "ignore").decode("ASCII")

    # --- Método público ---
    def procesar_archivo(self, ruta_archivo_excel):
        """
        Punto de entrada para el controlador. Procesa un único archivo de vehículos.
        """
        self.__log(f"Iniciando procesamiento para: {ruta_archivo_excel}")
        nombre_base = Path(ruta_archivo_excel).stem

        anio_str = self.__extraer_anio_de_ruta(ruta_archivo_excel) # Usa anio
        if not anio_str:
            self.__log(f"ADVERTENCIA: No se pudo determinar el anio para '{nombre_base}'. Se guardará en la raíz de 'Siniestralidad/Ficha 1'.") # Usa anio
            anio_str = ""

        # Construye la ruta de salida
        ruta_salida_anio = os.path.join(self.__ruta_limpia_base, anio_str) # Usa anio
        os.makedirs(ruta_salida_anio, exist_ok=True) # Usa anio
        ruta_csv_salida = os.path.join(ruta_salida_anio, f"{nombre_base}_Limpio.csv") # Usa anio

        if os.path.exists(ruta_csv_salida):
            self.__log(f"El archivo '{nombre_base}' ya ha sido procesado en '{anio_str}'. Saltando.") # Usa anio
            return True

        try:
            df_transformado = self.__transformar_excel(ruta_archivo_excel)

            # Chequear si la transformación devolvió None o un DataFrame vacío
            if df_transformado is None or df_transformado.empty:
                self.__log(f"ADVERTENCIA: La transformación de '{nombre_base}' no produjo datos.")
                return True # Considerar éxito parcial, no guardar

            # Guardado del archivo
            df_transformado.to_csv(ruta_csv_salida, index=False, encoding="utf-8-sig")
            self.__log(f"Éxito: '{nombre_base}' procesado ({len(df_transformado)} filas). Guardado en '{anio_str}'.") # Usa anio
            return True
        except Exception as e:
            self.__log(f"ERROR CRÍTICO procesando '{nombre_base}': {e}")
            raise e

    # --- Métodos privados ---
    def __extraer_anio_de_ruta(self, ruta_archivo): # Usa anio
        """Intenta extraer el anio (carpeta 'YYYY') del path del archivo."""
        try:
            parts = Path(ruta_archivo).parts
            for part in reversed(parts[:-1]):
                if len(part) == 4 and part.isdigit():
                    return part
        except Exception as e:
            self.__log(f"No se pudo extraer anio de la ruta {ruta_archivo}: {e}") # Usa anio
        return None

    def __transformar_excel(self, ruta_archivo):
        """
        ETL que replica las transformaciones de Power Query para datos de vehículos,
        incorporando las nuevas funcionalidades.
        """
        self.__log(f"Transformando '{Path(ruta_archivo).name}'...")

        # === 1. Extraer Año y Mes del nombre del archivo ===
        nombre_archivo = os.path.basename(ruta_archivo)
        match = re.search(r"(\d{2})\s+\w+\s+(\d{4})", nombre_archivo)
        if match:
            mes = match.group(1)
            anio = match.group(2) # Usa anio
            prefijo_fecha = f"{anio}{mes}" # Usa anio
        else:
            prefijo_fecha = "000000"
            self.__log(f"Advertencia: No se pudo extraer fecha MM YYYY de '{nombre_archivo}'.")

        # === 2. Leer el Excel saltando las primeras 6 filas ===
        try:
            engine = 'xlrd' if ruta_archivo.lower().endswith('.xls') else 'openpyxl'
            self.__log(f"Leyendo con motor {engine}...")
            df = pd.read_excel(ruta_archivo, skiprows=6, engine=engine)
        except Exception as e:
             self.__log(f"Error fatal al leer '{Path(ruta_archivo).name}' (motor {engine}): {e}")
             return None # Devolver None si falla

        # === 3. Renombrar columnas ===
        column_renames = {
            "Código Accidente": "Código Accidente", "Tipo Vehículo": "Tipo Vehículo",
            "Servicio": "Servicio", "Maniobra": "Maniobra", "Consecuencia": "Consecuencia",
            "Pista/Vía": "Pista/Vía", "Patente": "Patente", "Marca": "Marca"
        }
        # Renombrar solo las columnas existentes para evitar errores
        df = df.rename(columns={k: v for k, v in column_renames.items() if k in df.columns})


        # === NUEVO PASO: ELIMINAR FILAS FANTASMAS REALES ===
        # 1) Reemplazar valores vacíos o espacios
        df = df.replace(r'^\s*$', np.nan, regex=True)

        # 2) Eliminar filas donde TODAS las columnas excepto "Código Accidente" están vacías
        cols_datos = ["Tipo Vehículo", "Servicio", "Maniobra", "Consecuencia", "Pista/Vía", "Patente", "Marca"]
        cols_datos = [c for c in cols_datos if c in df.columns]  # Seguridad

        df = df.dropna(subset=cols_datos, how="all").reset_index(drop=True)


        # === 4. Eliminar filas completamente vacías ===
        self.__log("Eliminando filas basura (sin Patente Y sin Marca)...")
        df = df.dropna(subset=['Código Accidente', 'Patente', 'Marca'], how='all').reset_index(drop=True) # Resetear índice aquí

        # === 5. Convertir columnas a string y limpiar espacios ===
        text_cols = list(column_renames.values()) # Usar nombres nuevos
        for col in text_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace(['nan', 'None', ''], np.nan, regex=False)

        # === 6. Limpiar valores "SIN ANTECEDENTES" ===
        cols_to_clean = ["Tipo Vehículo", "Servicio", "Maniobra", "Consecuencia", "Pista/Vía"]
        for col in cols_to_clean:
            if col in df.columns:
                df[col] = df[col].str.strip().str.upper().replace("SIN ANTECEDENTES", None)

        # === 7. Rellenar "Código Accidente" ===
        if "Código Accidente" in df.columns:
            # Asegurarse de reemplazar 'nan' y 'None' strings antes de ffill
            df["Código Accidente"] = df["Código Accidente"].replace(['nan', 'None'], np.nan).ffill()

        # === 8. Normalizar "Pista/Vía" ===
        if "Pista/Vía" in df.columns:
            # Reemplazar ' y ' solo si existe, manejar NaN
            df["Pista/Vía"] = df["Pista/Vía"].str.lower().str.replace(" y ", "-", regex=False)
            # Separar y explotar de forma segura (maneja NaN)
            df["Pista/Vía_split"] = df["Pista/Vía"].str.split("-")
            df = df.explode("Pista/Vía_split")
            # Limpiar espacios antes de convertir a numérico
            df["Pista/Vía"] = pd.to_numeric(df["Pista/Vía_split"].str.strip(), errors="coerce")
            df = df.drop(columns=["Pista/Vía_split"]) # Eliminar columna temporal


        # === 9. Limpiar "Patente" ===
        if "Patente" in df.columns:
            # Quitar guiones y espacios
            df["Patente"] = df["Patente"].str.replace("-", "", regex=False).str.replace(" ", "", regex=False)
            # Reemplazar 'nan' y 'None' strings con NaN real
            df["Patente"] = df["Patente"].replace(['nan', 'None'], np.nan, regex=False)

        # === 10. Crear campo "ID Accidente" según cambios en Código Accidente ===
        self.__log("Generando 'ID Accidente' por cambios reales en 'Código Accidente'...")

        if "Código Accidente" in df.columns and not df["Código Accidente"].isna().all():

            # Normalizar Código Accidente:
            # - string
            # - mayúsculas
            # - sin espacios internos (ACC 19 -> ACC19)
            df["Código Accidente"] = df["Código Accidente"].astype(str).str.strip()
            df["__cod_norm"] = (
                df["Código Accidente"]
                .str.upper()
                .str.replace(r"\s+", "", regex=True)  # quita TODOS los espacios
            )

            # Marca TRUE cuando empieza un nuevo accidente:
            # - en la primera fila
            # - o cuando el código normalizado es distinto al de la fila anterior
            es_nuevo_accidente = df["__cod_norm"] != df["__cod_norm"].shift(1)

            # Correlativo ascendente: 1,1,1,...,2,2,2,...,3,3,...
            df["__correlativo"] = es_nuevo_accidente.cumsum()

            # Formatear correlativo a 3 dígitos
            df["__correlativo"] = df["__correlativo"].astype(int).astype(str).str.zfill(3)

            # Construir ID Accidente con el prefijo del archivo (AÑOMES)
            df["ID Accidente"] = "ACC-" + prefijo_fecha + "-" + df["__correlativo"]

            # Limpiar columnas auxiliares
            df = df.drop(columns=["__cod_norm", "__correlativo"])

        else:
            df["ID Accidente"] = None


        # === 11. Reemplazar vacíos/NaN/representaciones de nulo por 0 (Numérico) ===
        self.__log("Reemplazando valores nulos/vacíos representativos por 0...")
        valores_a_reemplazar_por_cero = [
            "SINPATENTE", "SIN PATENTE", "No registra", "NoRegistra", "Sin datos", "Sindatos","NO REGISTRA", "NOREGISTRA",
            "Sinantecedentes", "Sin antecedentes", "SINANTEDECENTES", "SIN ANTECEDENTES",
            "NAN", "nan", "None", "S/I", "-", "","N°0575902","NRO", None, np.nan
            # Añadir mas si es necesario
        ]
        # Aplicar a todo el DataFrame (Patente y Marca se corregirán después)
        df = df.replace(valores_a_reemplazar_por_cero, 0)

        # === 12. Reemplazar '0' en Patente/Marca por 'SIN-ANTECEDENTES' ===
        self.__log("Estandarizando 'SIN-ANTECEDENTES' en Patente y Marca...")
        for col in ["Patente", "Marca"]:
            if col in df.columns:
                # Lista de valores a reemplazar por SIN-ANTECEDENTES
                valores_a_sin_antecedentes = ["S/PPU", "S/I", "0", 0] # Incluir 0 numérico y "0" string
                df[col] = df[col].replace(valores_a_sin_antecedentes, "SIN-ANTECEDENTES")
                # Asegurar que sea string y mayúsculas al final
                df[col] = df[col].astype(str).str.upper()
                # Limpiar posibles espacios extra introducidos
                df[col] = df[col].str.strip()

        # === 13. Limpieza avanzada de la columna Marca ===
        if "Marca" in df.columns:
            self.__log("Aplicando limpieza avanzada a columna Marca...")
            # Quitar espacios internos ANTES de aplicar el diccionario
            df["Marca"] = df["Marca"].str.replace(" ", "", regex=False)
            
            # Quitar tildes/diacríticos en Marca (ej.: HYUNDAÍ -> HYUNDAI)
            df["Marca"] = df["Marca"].apply(self.__sin_tildes)

            # Diccionario de correcciones
            correcciones_marcas = {
                 "SINANTECEDENTES":"SIN-ANTECEDENTES", "SINMARCA" : "SIN-ANTECEDENTES",
                 "RANDON(REMOLQUE)":"RANDON", "MACK(CAMABAJA)":"MACK", # Corregido Mack
                 "MITSUBICHI":"MITSUBISHI", "MITSUVISHI":"MITSUBISHI", "MITZUBISHI":"MITSUBISHI",
                 "KIAMOTORS": "KIA", "KIAMOTOR": "KIA", "KÍAMOTORS": "KIA", "KIAFRONTIER" : "KIA",
                 "CHEBROLET": "CHEVROLET", "CARROHECHIZO": "REMOLQUE", "CARRODEREMOLQUE": "REMOLQUE",
                 "CHEROKEE": "JEEP", "INTER": "INTERNATIONAL", "MASDA": "MAZDA",
                 "DAFCL":"DAF", "MAC":"MACK", "FOR": "FORD", "BWW": "BMW",
                 "SAMGUN": "SAMSUNG", "TOYTA": "TOYOTA", "HYUNDAY":"HYUNDAI",
                 "SUSUKI.":"SUZUKI", "VW": "VOLKSWAGEN", "CAWASAKI": "KAWASAKI",
                 "WOLKSWAGEN": "VOLKSWAGEN", "GREALWALL": "GREAT-WALL", "GREATWALL": "GREAT-WALL",
                 "GREATWAL": "GREAT-WALL", "GREATWALT": "GREAT-WALL",
                 "THERMOKINGRAMPLA": "RAMPLA", "TERMOKINGRAMPLA": "RAMPLA",
                 "TERMOKINRAMPLA": "RAMPLA", "MERCEDEZ": "MERCEDES-BENZ",
                 "MERCEDES": "MERCEDES-BENZ", "MERCEDESBENZ": "MERCEDES-BENZ",
                 "MERCEDEZBENZ": "MERCEDES-BENZ", "PEUGEOTPARTNER":"PEUGEOT",
                 "HARLEYDAVIDSON": "HARLEY-DAVIDSON",
                 "MORRIS GARAGE": "MORRIS-GARAGE",
                 "MORRISGARAGE": "MORRIS-GARAGE",
                 "MITSUBICHIMONTERO":"MITSUBISHI", "HYUNDAI.": "HYUNDAI",
                 "CHEVROLET.": "CHEVROLET", "SUZUKI.": "SUZUKI", "KIA.": "KIA",
                 "PEUGEOT.": "PEUGEOT", "NISSAM":"NISSAN", "NISAN":"NISSAN",
                 "NISSNA":"NISSAN", "NISSSAN":"NISSAN", "TOYOTTA":"TOYOTA",
                 "TOYOTAYARIS":"TOYOTA", "HONDA.":"HONDA", "HODA":"HONDA",
                 "HYNDAI":"HYUNDAI", "HYUDAI":"HYUNDAI", "HYUDAHI":"HYUNDAI",
                 "HYUNDAIACCENT":"HYUNDAI", "ISUZU.":"ISUZU", "DAIHATSU.":"DAIHATSU",
                 "DAEWOO.":"DAEWOO", "DAEWU":"DAEWOO", "DODGE.":"DODGE", "JAC.":"JAC",
                 "JEEP.":"JEEP", "JPE":"JEEP", "JEEPCHEROKEE":"JEEP-CHEROKEE",
                 "RENAULT.":"RENAULT", "RENO":"RENAULT", "RENAUL":"RENAULT",
                 "REANULT":"RENAULT", "FIAT.":"FIAT", "FIA":"FIAT", "FOD":"FORD",
                 "FORDMOTOR":"FORD", "FORD.":"FORD", "CHEVROLE":"CHEVROLET",
                 "CHEVORLET":"CHEVROLET", "CHEVROLETE":"CHEVROLET",
                 "CHEVROLETSAIL":"CHEVROLET-SAIL", "SUZUK":"SUZUKI", "SUSUKI":"SUZUKI",
                 "SUZIKI":"SUZUKI", "MITSUBI":"MITSUBISHI", "PEUJEOT":"PEUGEOT",
                 "VOLSWAGEN":"VOLKSWAGEN", "VOLKS":"VOLKSWAGEN", "VOLV":"VOLVO",
                 "VOLV.":"VOLVO", "VOLVO.":"VOLVO", 
                 "FREIGTHLINER": "FREIGHTLINER", "FREIGTLINER": "FREIGHTLINER",
                 "FREIGTLINER.": "FREIGHTLINER", "FREIGHTLINER.": "FREIGHTLINER",
                 "FORDD":"FORD","OXFORD":"FORD", "VOLKWAGEN":"VOLKSWAGEN", "VOLKSAWAGEN":"VOLKSWAGEN",
                 "HYUNDAYGETZ":"HYUNDAI", "HYUNDAPORTER":"HYUNDAI",
                 "SUZUKY": "SUZUKI", "ZUZUKI": "SUZUKI", "SUSUKY": "SUZUKI",
                 "ZUBARU": "SUBARU", "SAMSUM": "SAMSUNG","SAMNSUNG": "SAMSUNG",
                 "GRANCHEROKEE": "JEEP", "GRANDCHEROKEE": "JEEP",
                 "NOREGISTRA":"SIN-ANTECEDENTES",
                 "INO":"HINO", "GACGONOW": "GAC-GONOW", "SSANYONG": "SSANGYONG","SSAINGYONG":"SSANGYONG",
                 "NISSANNAVARA":"NISSAN", "NISSANCOROLA":"NISSAN",
                 "SCANNIA": "SCANIA", "SECANIA": "SCANIA", "SCANIA.": "SCANIA", 
                 "DAEWO": "DAEWOO", "TOYOTAHILUX":"TOYOTA", "TOYOTARUNNER":"TOYOTA",
                 "TOYOTAURBAN": "TOYOTA", "MERCEDESACTRON":"MERCEDES-BENZ","MERCEDESACTROS":"MERCEDES-BENZ",
                 "DOGDE": "DODGE","BRILLANCE":"BRILLIANCE", "DUCATE": "DUCATI", "FREIGHLINER":"FREIGHTLINER",
                 "FORDCARGO":"FORD", "FORDFIESTA":"FORD", "FORDRANGER":"FORD",
                 "SINANTEDECENTES":"SIN-ANTECEDENTES", "HIARIO": "SIN-ANTECEDENTES", "HILUX":"TOYOTA",
                 "HIUNDAYELANTRA":"HYUNDAI", "HIUNDAI": "HYUNDAI","HYNDAY": "HYUNDAI",
                 "KEMBORT": "KENWORTH","KENWORK":"KENWORTH","KIACERATO":"KIA","KIAMORNING":"KIA",
                 "KIARIO":"KIA","KIASOLUTO": "KIA","KIASPORTAGE": "KIA","MAXUX":"MAXUS",
                 "MISUBISHI": "MITSUBISHI", "MITSHUBISHI": "MITSUBISHI","CHEVROLETAVEOLS1.4 ":"CHEVROLET",
                 "MINICOOPER": "MINI", "MINICOOOPER": "MINI", "MINICOOOPER.": "MINI",
                 "OPELCORSA":"OPEL", "OPELCORSAS":"OPEL", "OPELCORSAS.":"OPEL",
                 "PEUGOT": "PEUGEOT", "PEUGOT.":"PEUGEOT", "PEUGEOT.":"PEUGEOT",
                 "PORSHE":"PORSCHE", "RANDOMRAMPLA": "RANDON", "RANDONREMOLQUE": "RANDON", "RENEGATE": "JEEP",
                 "SANGYONG": "SSANGYONG", "SANSJONG": "SSANGYONG", "SORENTO": "KIA", "STATIONWAGON": "SIN-ANTECEDENTES",
                 "TOYOYA":"TOYOTA", "TSR": "ZENVO", "WOKSWAGEN": "VOLKSWAGEN", "WOLSVAGEN": "VOLKSWAGEN", 
                 "ZNADONGFENG": "DONGFENG", "CHEVY": "CHEVROLET", "FORESTER": "SUBARU",
                 "FRONTIER": "NISSAN", "FVR": "CHEVROLET", "GOREN": "GOREN", "GRAND-CHEROKEE": "JEEP",
                 "HIUNDAY": "HYUNDAI", "HOMAN": "SINOTRUK", "OMAN": "SINOTRUK", "KENWOOD": "KENWORTH",
                 "KONECT": "BRILLIANCE",  "MG": "MORRIS-GARAGE", "NAVARA": "NISSAN",
                 "OPELL": "OPEL", "OOPEL": "OPEL", "PULSAR": "BAJAJ", "RENEGADE": "JEEP", "SPRINTER": "MERCEDES-BENZ",
                 "KGM": "SSANGYONG", "STATION-WAGON": "SIN-ANTECEDENTES", "TROOPERS": "ISUZU", "TROOPER": "ISUZU",
                 "ZENVOTSR": "ZENVO", "ZUMI": "SIN-ANTECEDENTES", "PUGEOT": "PEUGEOT", "BENZ": "MERCEDES-BENZ",
                 "SINOTRUKHOMAN": "SINOTRUK", "SINOTRUKOMAN": "SINOTRUK", "NISSANMP300": "NISSAN",
                 "HYUNDAITUCSON":"HYUNDAI", "ZX": "ZX-AUTO", "ZXAUTO": "ZX-AUTO",
                 #"PÈUGEOT": "PEUGEOT",
            }
            # Aplicar reemplazos (solo si la columna existe)
            if "Marca" in df.columns:
                df["Marca"] = df["Marca"].replace(correcciones_marcas)
                # Asegurar mayúsculas al final
                df["Marca"] = df["Marca"].astype(str).str.upper()

        # === 14. ELIMINAR FILAS FANTASMAS ===
        df = df.dropna(how="all")
        df = df[~((df["Patente"].isna()) & (df["Marca"].isna()))]

        # === 15. CONVERTIR NUMÉRICOS DE 5.0 → 5 ===
        cols_numericas = ["Tipo Vehículo", "Servicio", "Maniobra", "Consecuencia", "Pista/Vía"]
        for col in cols_numericas:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)


        # === 16. REFORZANDO LA UNIFICACIÓN FINAL DE PATENTE Y MARCA ===
        self.__log("Aplicando unificación final en Patente y Marca...")

        # Listado de valores que deben convertirse a SIN-ANTECEDENTES
        valores_invalidos_global = [
            "-", "SINANTECEDENTES", "SINANTECEDENTE", "SINPPU", "SINPATENTE", "SINANTEDECENTES",
            "SINDATOS", "DESCONOCIDA", "N°0575902", "N°", "NRO", "NONE",
            "0", 0, "", None, np.nan
        ]

        for col in ["Patente", "Marca"]:
            if col in df.columns:
                df[col] = df[col].astype(str).str.upper().str.strip()

                # Remove punctuation and weird characters
                df[col] = df[col].str.replace(r"[^A-Z0-9\-]", "", regex=True)

                # Convert patterns like SINPATENTE, SINPU, etc → SIN-ANTECEDENTES
                df[col] = df[col].replace(valores_invalidos_global, "SIN-ANTECEDENTES")

                # Cualquier valor con longitud menor a 2 → lo consideramos inválido y lo pasamos a SIN-ANTECEDENTES
                df.loc[df[col].str.len() < 2, col] = "SIN-ANTECEDENTES"

                # Si la marca contiene números → probablemente es error → SIN-ANTECEDENTES
                df.loc[df["Marca"].str.contains(r"\d", regex=True), "Marca"] = "SIN-ANTECEDENTES"

        # === 17. Correcciones de marcas muy largas como CHEVROLETAVEOLS1.4 -> CHEVROLET ===
        df["Marca"] = df["Marca"].str.replace(r"^(CHEVROLET).*", "CHEVROLET", regex=True)
        df["Marca"] = df["Marca"].str.replace(r"^(MERCEDESBENZ).*", "MERCEDES-BENZ", regex=True)
        df["Marca"] = df["Marca"].str.replace(r"^(HYUNDAI).*", "HYUNDAI", regex=True)
        df["Marca"] = df["Marca"].str.replace(r"^(SUZUKI).*", "SUZUKI", regex=True)


        return df