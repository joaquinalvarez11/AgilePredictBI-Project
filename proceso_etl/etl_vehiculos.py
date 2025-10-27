import pandas as pd
import os
import re
from pathlib import Path
from datetime import datetime
from config_manager import obtener_ruta

class ETLVehiculos():

    def __init__(self):
        """Inicializa las rutas usando el config_manager."""
        base_limpios = obtener_ruta('ruta_csv_limpio')
        self.__ruta_limpia_base = os.path.join(base_limpios, 'Siniestralidad', 'Ficha 1')
        os.makedirs(self.__ruta_limpia_base, exist_ok=True)
        self.__log("ETL de Vehículos (Ficha 1) inicializada.")

    def __log(self, msg):
        """Imprime un mensaje con marca de tiempo para consistencia."""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{now}] [ETL Vehiculos] {msg}")

    # Métodos públicos
    def procesar_archivo(self, ruta_archivo_excel):
        """
        Punto de entrada para el controlador. Procesa un único archivo de vehículos.
        """
        self.__log(f"Iniciando procesamiento para: {ruta_archivo_excel}")
        nombre_base = Path(ruta_archivo_excel).stem
        
        anio_str = self.__extraer_anio_de_ruta(ruta_archivo_excel)
        if not anio_str:
            self.__log(f"ADVERTENCIA: No se pudo determinar el año para '{nombre_base}'. Se guardará en la raíz de 'Siniestralidad/Ficha 1'.")
            anio_str = ""
        ruta_salida_anio = os.path.join(self.__ruta_limpia_base, anio_str)
        os.makedirs(ruta_salida_anio, exist_ok=True)
        ruta_csv_salida = os.path.join(ruta_salida_anio, f"{nombre_base}_Limpio.csv")

        if os.path.exists(ruta_csv_salida):
            self.__log(f"El archivo '{nombre_base}' ya ha sido procesado. Saltando.")
            return True

        try:
            # Lógica de transformación en un método privado
            df_transformado = self.__transformar_excel(ruta_archivo_excel)
            
            if df_transformado.empty:
                self.__log(f"La transformación de '{nombre_base}' no produjo datos. Saltando guardado.")
                return True

            # Guardado del archivo
            df_transformado.to_csv(ruta_csv_salida, index=False, encoding="utf-8-sig")
            self.__log(f"Éxito: '{nombre_base}' procesado ({len(df_transformado)} filas). Guardado en '{anio_str}'.") # Ya usa __log
            return True
        except Exception as e:
            self.__log(f"ERROR CRÍTICO procesando '{nombre_base}': {e}") # Ya usa __log
            raise e

    # Métodos privados
    def __extraer_anio_de_ruta(self, ruta_archivo):
        """Intenta extraer el año (carpeta 'YYYY') del path del archivo."""
        try:
            parts = Path(ruta_archivo).parts
            for part in reversed(parts[:-1]):
                if len(part) == 4 and part.isdigit():
                    return part
        except Exception as e:
            self.__log(f"No se pudo extraer año de la ruta {ruta_archivo}: {e}")
        return None
    
    def __transformar_excel(self, ruta_archivo):
        """
        ETL que replica las transformaciones de Power Query para datos de vehículos.
        """
        self.__log(f"Transformando '{Path(ruta_archivo).name}'...")

        # === 1. Extraer Año y Mes del nombre del archivo ===
        nombre_archivo = os.path.basename(ruta_archivo)
        match = re.search(r"(\d{2})\s+\w+\s+(\d{4})", nombre_archivo) # Se busca un patrón como "01 Enero 2024"
        if match:
            mes = match.group(1)
            año = match.group(2)
            prefijo_fecha = f"{año}{mes}"
        else:
            prefijo_fecha = "000000"
            self.__log(f"Advertencia: No se pudo extraer fecha MM YYYY de '{nombre_archivo}'.")

        # === 2. Leer el Excel saltando las primeras 6 filas ===
        try:
            engine = 'xlrd' if ruta_archivo.lower().endswith('.xls') else 'openpyxl'
            self.__log(f"Leyendo con motor {engine}...")
            df = pd.read_excel(ruta_archivo, skiprows=6, engine=engine)
        except Exception as e:
             self.__log(f"Error fatal al leer '{Path(ruta_archivo).name}' (motor {engine}): {e}") # Usar __log
             return None

        # === 3. Renombrar columnas ===
        column_renames = {
            "Código Accidente": "Código Accidente", "Tipo Vehículo": "Tipo Vehículo",
            "Servicio": "Servicio", "Maniobra": "Maniobra", "Consecuencia": "Consecuencia",
            "Pista/Vía": "Pista/Vía", "Patente": "Patente", "Marca": "Marca"
        }
        df = df.rename(columns=column_renames)

        # === 4. Convertir columnas a string ===
        text_cols = [
            "Código Accidente", "Tipo Vehículo", "Servicio", "Maniobra", 
            "Consecuencia", "Pista/Vía", "Patente", "Marca"
        ]
        for col in text_cols:
            if col in df.columns:
                df[col] = df[col].astype(str)

        # === 5. Eliminar filas completamente vacías ===
        df = df.dropna(how="all").reset_index(drop=True)

        # === 6. Limpiar valores "SIN ANTECEDENTES" ===
        cols_to_clean = ["Tipo Vehículo", "Servicio", "Maniobra", "Consecuencia", "Pista/Vía"]
        for col in cols_to_clean:
            if col in df.columns:
                df[col] = df[col].str.strip().str.upper().replace("SIN ANTECEDENTES", None)

        # === 7. Normalizar "Pista/Vía" ===
        if "Pista/Vía" in df.columns:
            # Reemplazar ' y ' solo si existe, manejar NaN
            df["Pista/Vía"] = df["Pista/Vía"].str.lower().str.replace(" y ", "-", regex=False)
            # Separar y explotar de forma segura
            df["Pista/Vía_split"] = df["Pista/Vía"].str.split("-")
            df = df.explode("Pista/Vía_split")
            # Limpiar espacios antes de convertir a numérico
            df["Pista/Vía"] = pd.to_numeric(df["Pista/Vía_split"].str.strip(), errors="coerce")
            df = df.drop(columns=["Pista/Vía_split"]) # Eliminar columna temporal

        # === 8. Rellenar "Código Accidente" ===
        if "Código Accidente" in df.columns:
            df["Código Accidente"] = df["Código Accidente"].replace("nan", None).ffill()

        # === 9. Limpiar "Patente" ===
        if "Patente" in df.columns:
            df["Patente"] = df["Patente"].str.replace("-", "", regex=False).str.replace(" ", "", regex=False)

        # === 10. Crear campo "ID Accidente" ===
        df = df.reset_index(drop=True)
        codigos_unicos = df["Código Accidente"].dropna().unique()
        mapa_ids = {}
        for i, codigo in enumerate(codigos_unicos, start=1):
            correlativo = str(i).zfill(3)
            mapa_ids[codigo] = f"ACC-{prefijo_fecha}-{correlativo}"
        df["ID Accidente"] = df["Código Accidente"].map(mapa_ids)

        # === 11. Reemplazar vacíos/NaN/representaciones de nulo por 0 (Numérico) ===
        self.__log("Reemplazando valores nulos/vacíos representativos por 0...")
        valores_a_reemplazar_por_cero = [
            "SINPATENTE", "SIN PATENTE", "No registra", "NoRegistra", "Sin datos", "Sindatos",
            "Sinantecedentes", "Sin antecedentes", # Ya deberían ser NaN, pero por si acaso
            "NAN", "nan", "None", "S/I", "", None, np.nan
        ]
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
            # Se hace antes del replace para que coincidan las claves del diccionario
            df["Marca"] = df["Marca"].str.replace(" ", "", regex=False)

            # Diccionario de correcciones
            correcciones_marcas = {
                "SINANTECEDENTES":"SIN-ANTECEDENTES", "SINMARCA" : "SIN-ANTECEDENTES",
                "RANDON(REMOLQUE)":"REMOLQUE", "Mack(CAMABAJA)":"MACK",
                "MITSUBICHI":"MITSUBISHI", "MITSUVISHI":"MITSUBISHI", "MITZUBISHI":"MITSUBISHI",
                "KIAMOTORS": "KIA", "KIAMOTOR": "KIA", "KÍAMOTORS": "KIA", "KIAFRONTIER" : "KIA-FRONTIER",
                "CHEBROLET": "CHEVROLET", "CARROHECHIZO": "REMOLQUE", "CARRODEREMOLQUE": "REMOLQUE",
                "CHEROKEE": "JEEP", "INTER": "INTERNATIONAL", "MASDA": "MAZDA",
                "DAFCL":"DAF", "MAC":"MACK", "FOR": "FORD", "BWW": "BMW",
                "SAMGUN": "SAMSUNG", "TOYTA": "TOYOTA", "HYUNDAY":"HYUNDAI",
                "SUSUKI.":"SUZUKI", "VW": "VOLKSWAGEN", "CAWASAKI": "KAWASAKI",
                "WOLKSWAGEN": "VOLKSWAGEN", "GREALWALL": "GREAT-WALL", "GREATWALL": "GREAT-WALL",
                "GREATWAL": "GREAT-WALL", "GREATWALT": "GREAT-WALL",
                "THERMOKINGRAMPLA": "THERMO-KING-RAMPLA", "TERMOKINGRAMPLA": "THERMO-KING-RAMPLA",
                "TERMOKINRAMPLA": "THERMO-KING-RAMPLA", "MERCEDEZ": "MERCEDES-BENZ",
                "MERCEDES": "MERCEDES-BENZ", "MERCEDESBENZ": "MERCEDES-BENZ",
                "MERCEDEZBENZ": "MERCEDES-BENZ", "PEUGEOTPARTNER":"PEUGEOT-PARTNER",
                "HARLEYDAVIDSON": "HARLEY-DAVIDSON",
                "MORRIS GARAGE": "MORRIS-GARAGE",
                "MORRISGARAGE": "MORRIS-GARAGE",
                "MITSUBICHIMONTERO":"MITSUBISHI-MONTERO", "HYUNDAI.": "HYUNDAI",
                "CHEVROLET.": "CHEVROLET", "SUZUKI.": "SUZUKI", "KIA.": "KIA",
                "PEUGEOT.": "PEUGEOT", "NISSAM":"NISSAN", "NISAN":"NISSAN",
                "NISSNA":"NISSAN", "NISSSAN":"NISSAN", "TOYOTTA":"TOYOTA",
                "TOYOTAYARIS":"TOYOTA-YARIS", "HONDA.":"HONDA", "HODA":"HONDA",
                "HYNDAI":"HYUNDAI", "HYUDAI":"HYUNDAI", "HYUDAHI":"HYUNDAI",
                "HYUNDAIACCENT":"HYUNDAI-ACCENT", "ISUZU.":"ISUZU", "DAIHATSU.":"DAIHATSU",
                "DAEWOO.":"DAEWOO", "DAEWU":"DAEWOO", "DODGE.":"DODGE", "JAC.":"JAC",
                "JEEP.":"JEEP", "JPE":"JEEP", "JEEPCHEROKEE":"JEEP-CHEROKEE",
                "RENAULT.":"RENAULT", "RENO":"RENAULT", "RENAUL":"RENAULT",
                "REANULT":"RENAULT", "FIAT.":"FIAT", "FIA":"FIAT", "FOD":"FORD",
                "FORDMOTOR":"FORD", "FORD.":"FORD", "CHEVROLE":"CHEVROLET",
                "CHEVORLET":"CHEVROLET", "CHEVROLETE":"CHEVROLET",
                "CHEVROLETSAIL":"CHEVROLET-SAIL", "SUZUK":"SUZUKI", "SUSUKI":"SUZUKI",
                "SUZIKI":"SUZUKI", "MITSUBI":"MITSUBISHI", "PEUJEOT":"PEUGEOT",
                "VOLSWAGEN":"VOLKSWAGEN", "VOLKS":"VOLKSWAGEN", "VOLV":"VOLVO",
                "VOLV.":"VOLVO", "VOLVO.":"VOLVO"
            }
            # Aplicar reemplazos
            df["Marca"] = df["Marca"].replace(correcciones_marcas)
            # Asegurar mayúsculas al final por si acaso
            df["Marca"] = df["Marca"].astype(str).str.upper()

        return df
