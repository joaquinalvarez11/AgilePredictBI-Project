import sqlite3
import pandas as pd
import numpy as np
import time
import os
import sys

# 1. Añadir el directorio raíz del proyecto al path
current_dir = os.path.dirname(__file__)
# Subimos TRES niveles (desde /proceso_db/scripts/dim/ hasta el raíz)
project_root = os.path.abspath(os.path.join(current_dir, '..', '..', '..'))
sys.path.append(project_root)

# 2. Importar el gestor de configuración
from config_manager import obtener_ruta

try:
    # 3. Obtener la ruta desde config.json
    ruta_db = obtener_ruta("ruta_database")
except Exception as e:
    print(f"Error al cargar ruta_database desde config_manager: {e}")
    sys.exit(1)

# --- Configuración ---
start_date = '2016-01-01 00:00:00'
end_date = '2036-12-31 23:00:00'

# --- Mapas para español ---
meses_map = {
    1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril', 5: 'Mayo', 6: 'Junio',
    7: 'Julio', 8: 'Agosto', 9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
}
dias_map = {
    0: 'Lunes', 1: 'Martes', 2: 'Miércoles', 3: 'Jueves', 4: 'Viernes', 5: 'Sábado', 6: 'Domingo'
}

# --- 1. Conectar y verificar antes de generar ---
print("Conectando a la base de datos...")
conn = sqlite3.connect(ruta_db)
cursor = conn.cursor()
count = 0

try:
    cursor.execute("SELECT COUNT(*) FROM dim_DateTime")
    count = cursor.fetchone()[0]
    
    if count > 0:
        print(f"dim_DateTime ya está poblada con {count} registros. No se necesita carga.")
        conn.close()
        exit()

    # --- 2. Si count es 0, proceder con la generación ---
    print(f"Tabla dim_DateTime vacía. Generando {end_date} registros (por minuto)...")
    print("Esto puede tardar varios minutos.")
    start_gen = time.time()
    
    try:
        minute_dates = pd.date_range(start=start_date, end=end_date, freq='min')
    except ImportError:
        minute_dates = pd.date_range(start=start_date, end=end_date, freq='T') # Fallback para versiones antiguas

    df = pd.DataFrame(minute_dates, columns=['DateTime_dt'])
    end_gen = time.time()
    print(f"Se generaron {len(df)} filas en {end_gen - start_gen:.2f}s. Creando columnas...")

    # 3. Extraer todas las columnas necesarias
    df['idDateTime'] = range(1, len(df) + 1)
    df['DateTime'] = df['DateTime_dt'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df['Date'] = df['DateTime_dt'].dt.strftime('%Y-%m-%d')
    df['Year'] = df['DateTime_dt'].dt.year
    df['Month'] = df['DateTime_dt'].dt.month
    df['Day'] = df['DateTime_dt'].dt.day
    df['Hour'] = df['DateTime_dt'].dt.hour
    df['Minute'] = df['DateTime_dt'].dt.minute
    df['MonthName'] = df['Month'].map(meses_map)
    df['WeekDay'] = df['DateTime_dt'].dt.dayofweek.map(dias_map)
    df['WeekNumber'] = df['DateTime_dt'].dt.isocalendar().week.astype(int)

    df['date_int'] = df['Month'] * 100 + df['Day']
    conditions = [
        (df['date_int'] >= 321) & (df['date_int'] <= 620), # Otoño
        (df['date_int'] >= 621) & (df['date_int'] <= 920), # Invierno
        (df['date_int'] >= 921) & (df['date_int'] <= 1220) # Primavera
    ]
    choices = ['Otoño', 'Invierno', 'Primavera']
    df['Period'] = np.select(conditions, choices, default='Verano')

    columnas_finales = [
        'idDateTime', 'DateTime', 'Date', 'Year', 'Month', 'Day', 'Hour', 'Minute',
        'MonthName', 'WeekDay', 'WeekNumber', 'Period'
    ]
    df_final = df[columnas_finales]

    # 4. Cargar a SQLite
    print(f"Cargando {len(df_final)} registros en la base de datos...")
    start_load = time.time()
    df_final.to_sql("dim_DateTime", conn, if_exists="append", index=False)
    conn.commit()
    end_load = time.time()
    print(f"Éxito: Se cargaron {len(df_final)} registros en {end_load - start_load:.2f}s.")

except Exception as e:
    print(f"Error al cargar dim_DateTime: {e}")
    conn.rollback()
finally:
    if conn:
        conn.close()