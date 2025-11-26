import sqlite3
import pandas as pd
import numpy as np
from config_manager import obtener_ruta

# --- Configuración ---
km_start = 473.000
km_end = 665.000
precision = 0.001 # Granularidad de 1 metro

# Mapa de Puntos de Interés
map_places = [
    (473.600, "Inicio Tramo", "Inicio Tramo"),
    (473.900, "Puente", "Paso Superior FFCC"),
    (474.000, "Puente", "Puente Fiscal"),
    (474.640, "Enlace", "Enlace Compañías"),
    (475.920, "Enlace", "Enlace San Pedro"),
    (482.270, "Enlace", "Enlace Jardín"),
    (481.300, "Carabineros", "Plaza Pesaje"),
    (482.780, "Enlace", "Enlace El Romeral"),
    (503.000, "Puente", "Puente Juan Soldado"),
    (508.400, "Enlace", "Enlace Caleta Hornos"),
    (515.000, "Cuesta", "Cuesta Buenos Aires"),
    (529.900, "Enlace", "Enlace La Higuera"),
    (540.000, "Carabineros", "Área de Control Poniente"),
    (545.000, "Variente", "Variente Global Hunter"),
    (547.400, "Enlace", "Enlace Punta Choros"),
    (549.160, "Enlace", "Enlace Trapiche Sur"),
    (551.740, "Enlace", "Enlace Trapiche Norte"),
    (554.000, "Peaje", "Peaje IV Región"),
    (555.000, "Acceso", "Acceso Barrick/Punta Colorada Sur"),
    (559.600, "Servicios", "Área de Servicios y Descanso"),
    (572.000, "Variente", "Variente Incahuasi"),
    (572.940, "Enlace", "Enlace Incahuasi"),
    (583.000, "Cuesta", "Cuesta Pajonales"),
    (595.300, "Peaje", "Peaje III Región"),
    (604.520, "Enlace", "Enlace Cachiyuyo"),
    (613.680, "Enlace", "Enlace Domeyko"),
    (652.000, "Puente", "Paso Superior FFCC"),
    (656.000, "Carabineros", "Área de Control Oriente")
]
# Ordenar el mapa por Km para un procesamiento correcto
map_places.sort(key=lambda x: x[0])

def run(callback):
    """
    Carga la dimensión Km en la base de datos.
    Recibe un callback para enviar mensajes de log.
    """

    try:
        ruta_db = obtener_ruta("ruta_database")
        callback("Generando rango de Kms (por metro)...")

        # 1. Generar todos los Kms
        total_points = int((km_end - km_start) / precision) + 1
        kms = np.linspace(km_start, km_end, total_points)
        kms_rounded = np.round(kms, 3) # Redondear para evitar errores de punto flotante

        df = pd.DataFrame({'Km': kms_rounded})
        df['idKm'] = range(1, len(df) + 1)
        df['Element'] = None
        df['Place'] = None

        callback(f"Se generaron {len(df)} filas (metros). Aplicando lógica de lugares...")

        # 2. Aplicar lógica de "15 metros"
        for km_place, element, place in map_places:
            # Definir el rango de 15 metros (0.015 km)
            km_inicio_rango = km_place
            km_fin_rango = np.round(km_place + 0.015, 3)
            
            # Encontrar todas las filas del DataFrame que caen en este rango
            mask = (df['Km'] >= km_inicio_rango) & (df['Km'] <= km_fin_rango)
            
            df.loc[mask, 'Element'] = element
            df.loc[mask, 'Place'] = place

        callback("Lógica de lugares aplicada.")

        # 3. Seleccionar y ordenar las columnas finales
        columnas_finales = ['idKm', 'Km', 'Element', 'Place']
        df_final = df[columnas_finales]

        # 4. Conectar y cargar a SQLite
        print("Conectando a la base de datos...")
        conn = sqlite3.connect(ruta_db)
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM dim_Km")
        count = cursor.fetchone()[0]
        
        if count == 0:
            callback(f"Tabla dim_Km vacía. Cargando {len(df_final)} registros...")
            df_final.to_sql("dim_Km", conn, if_exists="append", index=False)
            conn.commit()
            callback(f"Éxito: Se cargaron {len(df_final)} registros en dim_Km.")
        else:
            callback(f"dim_Km ya está poblada con {count} registros. No se necesita carga.")

    except Exception as e:
        callback(f"Error al cargar dim_Km: {e}")
        if 'conn' in locals():
            conn.rollback()
        raise
    finally:
        if 'conn' in locals():
            conn.close()
