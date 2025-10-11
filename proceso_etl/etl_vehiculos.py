import pandas as pd
import os
import re
import numpy as np

def transformar_excel(ruta_archivo, salida_csv=None, salida_excel=None):
    """
    ETL en Python que replica las transformaciones hechas en Power Query (Power BI),
    con creación del campo 'ID Accidente', integración de archivos .xls/.xlsx,
    reemplazo de vacíos por 0 y limpieza avanzada de Patente y Marca.
    """

    # === 1. Extraer Año y Mes del nombre del archivo ===
    nombre_archivo = os.path.basename(ruta_archivo)
    match = re.match(r"(\d{2})\s+\w+\s+(\d{4})", nombre_archivo)
    if match:
        mes = match.group(1)
        año = match.group(2)
        prefijo_fecha = f"{año}{mes}"
    else:
        prefijo_fecha = "000000"

    # === 2. Detectar extensión y motor ===
    if ruta_archivo.endswith(".xlsx"):
        engine = "openpyxl"
    elif ruta_archivo.endswith(".xls"):
        engine = "xlrd"
    else:
        raise ValueError("Formato de archivo no soportado")

    # === 3. Leer Excel ===
    df = pd.read_excel(ruta_archivo, skiprows=6, engine=engine)

    # === 4. Renombrar columnas ===
    column_renames = {
        "Código Accidente": "Código Accidente",
        "Tipo Vehículo": "Tipo Vehículo",
        "Servicio": "Servicio",
        "Maniobra": "Maniobra",
        "Consecuencia": "Consecuencia",
        "Pista/Vía": "Pista/Vía",
        "Patente": "Patente",
        "Marca": "Marca"
    }
    df = df.rename(columns=column_renames)

    # === 5. Eliminar filas vacías ===
    df = df.dropna(how="all")

    # === 6. Convertir a string para limpieza ===
    text_cols = ["Código Accidente", "Tipo Vehículo", "Servicio",
                 "Maniobra", "Consecuencia", "Pista/Vía", "Patente", "Marca"]
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # === 7. Limpiar "SIN ANTECEDENTES" ===
    cols_to_clean = ["Tipo Vehículo", "Servicio", "Maniobra", "Consecuencia", "Pista/Vía"]
    for col in cols_to_clean:
        if col in df.columns:
            df[col] = df[col].str.upper().replace("SIN ANTECEDENTES", None)

    # === 8. Normalizar "Pista/Vía" ===
    if "Pista/Vía" in df.columns:
        df["Pista/Vía"] = df["Pista/Vía"].str.lower().str.strip()
        df["Pista/Vía"] = df["Pista/Vía"].str.replace(" y ", "-", regex=False)
        df = df.assign(**{"Pista/Vía": df["Pista/Vía"].str.split("-")}).explode("Pista/Vía")
        df["Pista/Vía"] = pd.to_numeric(df["Pista/Vía"], errors="coerce")

    # === 9. Rellenar "Código Accidente" ===
    if "Código Accidente" in df.columns:
        df["Código Accidente"] = df["Código Accidente"].replace(["nan", "None"], None)
        df["Código Accidente"] = df["Código Accidente"].ffill()

    # === 10. Limpiar "Patente" ===
    if "Patente" in df.columns:
        df["Patente"] = df["Patente"].str.replace("-", "", regex=False).str.replace(" ", "", regex=False)

    # === 11. Crear campo "ID Accidente" ===
    df = df.reset_index(drop=True)
    codigos_unicos = df["Código Accidente"].dropna().unique()
    mapa_ids = {}
    for i, codigo in enumerate(codigos_unicos, start=1):
        correlativo = str(i).zfill(3)
        mapa_ids[codigo] = f"ACC-{prefijo_fecha}-{correlativo}"
    df["ID Accidente"] = df["Código Accidente"].map(mapa_ids)

    # === 12. Reemplazar vacíos o NaN y más por 0 ===
    df = df.replace([
        "SINPATENTE", "SIN PATENTE", "Sin Patente", "NO REGISTRA", "NOREGISTRA",
        "No registra", "NoRegistra", "Sin datos", "Sindatos", "Sinantecedentes",
        "Sin antecedentes", "NAN", "nan", "None","S/I", None, np.nan
    ], 0)

    # === 13. Reemplazar '0' en Patente/Marca por 'SIN-ANTECEDENTES' ===
    for col in ["Patente", "Marca"]:
        if col in df.columns:
            df[col] = df[col].replace(["SINPATENTE", "S/PPU", "S/I", "0", 0, "nan", "None", ""], "SIN-ANTECEDENTES")
            df[col] = df[col].astype(str).str.upper()

    # === 14. Limpieza avanzada de la columna Marca ===
    if "Marca" in df.columns:
        df["Marca"] = df["Marca"].str.replace(" ", "", regex=False)

        correcciones_marcas = {
            # ---- Nuevas correcciones comunes ----
            "SINANTECEDENTES":"SIN-ANTECEDENTES",
            "SINMARCA" : "SIN-ANTECEDENTES",
            "RANDON(REMOLQUE)":"REMOLQUE",
            "Mack(CAMABAJA)":"MACK",
            "MITSUBICHI":"MITSUBISHI",
            "MITSUVISHI":"MITSUBISHI",
            "MITZUBISHI":"MITSUBISHI",
            "KIAMOTORS": "KIA",
            "KIAMOTOR": "KIA",
            "KÍAMOTORS": "KIA",
            "KIAFRONTIER" : "KIA-FRONTIER",
            "CHEBROLET": "CHEVROLET",
            "CARROHECHIZO": "REMOLQUE",
            "CARRODEREMOLQUE": "REMOLQUE",
            "CHEROKEE": "JEEP",
            "INTER": "INTERNATIONAL",
            "MASDA": "MAZDA",
            "DAFCL":"DAF",
            "MAC":"MACK",
            "FOR": "FORD",
            "BWW": "BMW",
            "SAMGUN": "SAMSUNG",
            "TOYTA": "TOYOTA",
            "HYUNDAY":"HYUNDAI",
            "SUSUKI.":"SUZUKI",
            "VW": "VOLKSWAGEN",
            "CAWASAKI": "KAWASAKI",
            "WOLKSWAGEN": "VOLKSWAGEN",
            "GREALWALL": "GREAT-WALL",
            "GREATWALL": "GREAT-WALL",
            "GREATWAL": "GREAT-WALL",
            "GREATWALT": "GREAT-WALL",
            "THERMOKINGRAMPLA": "THERMO-KING-RAMPLA",
            "TERMOKINGRAMPLA": "THERMO-KING-RAMPLA",
            "TERMOKINRAMPLA": "THERMO-KING-RAMPLA",
            "MERCEDEZ": "MERCEDES-BENZ",
            "MERCEDES": "MERCEDES-BENZ",
            "MERCEDESBENZ": "MERCEDES-BENZ",
            "MERCEDEZBENZ": "MERCEDES-BENZ",
            "PEUGEOTPARTNER":"PEUGEOT-PARTNER",
            "HARLEYDAVIDSON": "HARLEY-DAVIDSON",
            "MORRIS GARAGE": "MORRIS-GARAGE",
            "MITSUBICHIMONTERO":"MITSUBISHI-MONTERO",
            "HYUNDAI.": "HYUNDAI",
            "CHEVROLET.": "CHEVROLET",
            "SUZUKI.": "SUZUKI",
            "KIA.": "KIA",
            "PEUGEOT.": "PEUGEOT",

            "NISSAM":"NISSAN",
            "NISAN":"NISSAN",
            "NISSNA":"NISSAN",
            "NISSSAN":"NISSAN",
            "TOYOTTA":"TOYOTA",
            "TOYOTAYARIS":"TOYOTA-YARIS",
            "HONDA.":"HONDA",
            "HODA":"HONDA",
            "HYNDAI":"HYUNDAI",
            "HYUDAI":"HYUNDAI",
            "HYUDAHI":"HYUNDAI",
            "HYUNDAIACCENT":"HYUNDAI-ACCENT",
            "ISUZU.":"ISUZU",
            "DAIHATSU.":"DAIHATSU",
            "DAEWOO.":"DAEWOO",
            "DAEWU":"DAEWOO",
            "DODGE.":"DODGE",
            "JAC.":"JAC",
            "JEEP.":"JEEP",
            "JPE":"JEEP",
            "JEEPCHEROKEE":"JEEP-CHEROKEE",
            "RENAULT.":"RENAULT",
            "RENO":"RENAULT",
            "RENAUL":"RENAULT",
            "REANULT":"RENAULT",
            "FIAT.":"FIAT",
            "FIA":"FIAT",
            "FOD":"FORD",
            "FORDMOTOR":"FORD",
            "FORD.":"FORD",
            "CHEVROLE":"CHEVROLET",
            "CHEVORLET":"CHEVROLET",
            "CHEVROLETE":"CHEVROLET",
            "CHEVROLETSAIL":"CHEVROLET-SAIL",
            "SUZUK":"SUZUKI",
            "SUSUKI":"SUZUKI",
            "SUZIKI":"SUZUKI",
            "MITSUBI":"MITSUBISHI",
            "PEUJEOT":"PEUGEOT",
            "VOLSWAGEN":"VOLKSWAGEN",
            "VOLKS":"VOLKSWAGEN",
            "VOLV":"VOLVO",
            "VOLV.":"VOLVO",
            "VOLVO.":"VOLVO"
        }

        df["Marca"] = df["Marca"].replace(correcciones_marcas)
        df["Marca"] = df["Marca"].astype(str).str.upper()

    # === 15. Exportar ===
    if salida_csv:
        df.to_csv(salida_csv, index=False, encoding="utf-8-sig")
    if salida_excel:
        df.to_excel(salida_excel, index=False)

    return df


# ==========================================
# Procesar TODOS los años (2016 a 2024)
# ==========================================
if __name__ == "__main__":
    carpeta_base_brutos = r"C:\Users\crist\Desktop\Proyecto De Título\Prototipados de la tesis\Prototipo de Powe BI\ETL prueba\ExcelETLVehiculoBrutos"
    carpeta_base_limpios = r"C:\Users\crist\Desktop\Proyecto De Título\Prototipados de la tesis\Prototipo de Powe BI\ETL prueba\ExcelETLVehiculoLimpios"

    for año in range(2016, 2025):
        carpeta_brutos = os.path.join(carpeta_base_brutos, str(año))
        carpeta_limpios = os.path.join(carpeta_base_limpios, f"Limpio_{año}")
        os.makedirs(carpeta_limpios, exist_ok=True)

        if not os.path.exists(carpeta_brutos):
            print(f"⚠️ Carpeta no encontrada: {carpeta_brutos}")
            continue

        print(f"\n🚀 Procesando año {año}...")

        for archivo in os.listdir(carpeta_brutos):
            if archivo.endswith((".xlsx", ".xls")):
                nombre_sin_ext = os.path.splitext(archivo)[0]
                ruta_entrada = os.path.join(carpeta_brutos, archivo)

                ruta_excel_salida = os.path.join(carpeta_limpios, f"Limpio_{nombre_sin_ext}.xlsx")
                ruta_csv_salida = os.path.join(carpeta_limpios, f"Limpio_{nombre_sin_ext}.csv")

                print(f"🔄 Procesando: {archivo}...")
                try:
                    transformar_excel(ruta_entrada, ruta_csv_salida, ruta_excel_salida)
                    print(f"✅ Guardado en: {carpeta_limpios}")
                except Exception as e:
                    print(f"❌ Error procesando {archivo}: {e}")

        print(f"🎯 Año {año} completado.\n")
