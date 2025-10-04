import pandas as pd
import os
import re

def transformar_excel(ruta_archivo, salida_csv=None, salida_excel=None):
    """
    ETL en Python que replica las transformaciones hechas en Power Query (Power BI),
    con creación del campo 'ID Accidente' (mismo ID para filas con el mismo Código Accidente) y
    con la integracion de mas archivos.
    """

    # === 1. Extraer Año y Mes del nombre del archivo ===
    nombre_archivo = os.path.basename(ruta_archivo)
    match = re.match(r"(\d{2})\s+\w+\s+(\d{4})", nombre_archivo)
    if match:
        mes = match.group(1)   # "01"
        año = match.group(2)   # "2016"
        prefijo_fecha = f"{año}{mes}"  # "201601"
    else:
        prefijo_fecha = "000000"  # fallback si no se encuentra

    # === 2. Leer el Excel saltando las primeras 6 filas ===
    df = pd.read_excel(ruta_archivo, skiprows=6, engine="openpyxl")

    # === 3. Renombrar columnas según la consulta en Power Query ===
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

    # === 4. Convertir columnas a string ===
    text_cols = ["Código Accidente", "Tipo Vehículo", "Servicio", 
                "Maniobra", "Consecuencia", "Pista/Vía", 
                "Patente", "Marca"]
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].astype(str)

    # === 5. Eliminar filas completamente vacías ===
    df = df.dropna(how="all")

    # === 6. Limpiar valores "SIN ANTECEDENTES" ===
    cols_to_clean = ["Tipo Vehículo", "Servicio", "Maniobra", "Consecuencia", "Pista/Vía"]
    for col in cols_to_clean:
        if col in df.columns:
            df[col] = df[col].str.strip().str.upper().replace("SIN ANTECEDENTES", None)

    # === 7. Normalizar "Pista/Vía" ===
    if "Pista/Vía" in df.columns:
        df["Pista/Vía"] = df["Pista/Vía"].str.lower().str.strip()
        df["Pista/Vía"] = df["Pista/Vía"].str.replace(" y ", "-", regex=False)
        df = df.assign(**{"Pista/Vía": df["Pista/Vía"].str.split("-")}).explode("Pista/Vía")
        df["Pista/Vía"] = pd.to_numeric(df["Pista/Vía"], errors="coerce")

    # === 8. Rellenar "Código Accidente" ===
    if "Código Accidente" in df.columns:
        df["Código Accidente"] = df["Código Accidente"].replace("nan", None)
        df["Código Accidente"] = df["Código Accidente"].fillna(method="ffill")

    # === 9. Limpiar "Patente" ===
    if "Patente" in df.columns:
        df["Patente"] = df["Patente"].str.replace("-", "", regex=False).str.replace(" ", "", regex=False)

    # === 10. Crear campo "ID Accidente" (agrupado por Código) ===
    df = df.reset_index(drop=True)

    # Mapeo de Código Accidente -> ID único
    codigos_unicos = df["Código Accidente"].dropna().unique()
    mapa_ids = {}

    for i, codigo in enumerate(codigos_unicos, start=1):
        correlativo = str(i).zfill(3)  # 001, 002, ...
        mapa_ids[codigo] = f"ACC-{prefijo_fecha}-{correlativo}"

    # Asignar el mismo ID a todas las filas con el mismo Código Accidente
    df["ID Accidente"] = df["Código Accidente"].map(mapa_ids)

    # === 11. Exportar ===
    if salida_csv:
        df.to_csv(salida_csv, index=False, encoding="utf-8-sig")
    if salida_excel:
        df.to_excel(salida_excel, index=False)

    return df


# ================================
# Procesar SOLO nuevos Excel en carpeta
# ================================
if __name__ == "__main__":
    carpeta_brutos = r"C:\Users\crist\Desktop\Proyecto De Título\Prototipados de la tesis\Prototipo de Powe BI\ETL prueba\ExcelETLVehiculoBrutos"
    carpeta_limpios = r"C:\Users\crist\Desktop\Proyecto De Título\Prototipados de la tesis\Prototipo de Powe BI\ETL prueba\ExcelETLVehiculoLimpios"

    # Crear carpeta de salida si no existe
    os.makedirs(carpeta_limpios, exist_ok=True)

    # Recorrer todos los Excel de la carpeta de brutos
    for archivo in os.listdir(carpeta_brutos):
        if archivo.endswith(".xlsx"):
            nombre_sin_ext = os.path.splitext(archivo)[0]
            ruta_entrada = os.path.join(carpeta_brutos, archivo)

            # Archivos de salida
            ruta_excel_salida = os.path.join(carpeta_limpios, f"Limpio_{nombre_sin_ext}.xlsx")
            ruta_csv_salida = os.path.join(carpeta_limpios, f"Limpio_{nombre_sin_ext}.csv")

            # --- Verificar si YA existe ---
            if os.path.exists(ruta_excel_salida) and os.path.exists(ruta_csv_salida):
                print(f"⏭️ Saltado (ya procesado): {archivo}")
                continue

            print(f"🔄 Procesando: {archivo}...")
            try:
                transformar_excel(
                    ruta_archivo=ruta_entrada,
                    salida_excel=ruta_excel_salida,
                    salida_csv=ruta_csv_salida
                )
                print(f"✅ Guardado: {ruta_excel_salida} y {ruta_csv_salida}")
            except Exception as e:
                print(f"❌ Error procesando {archivo}: {e}")
