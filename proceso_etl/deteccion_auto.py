import os
import re
import pandas as pd
from pathlib import Path
from config_manager import obtener_ruta
from proceso_etl.etl_siniestros import ETLSiniestralidad
from proceso_etl.etl_trafico import ETLTrafico
from proceso_etl.etl_vehiculos import ETLVehiculos

def _extraer_fecha_del_nombre(nombre_archivo):
    match = re.search(r'(\d{4}).*?(\d{2})|(\d{2}).*?(\d{4})|(\d{2})\s+\w+\s+(\d{4})', nombre_archivo)
    if match:
        groups = [g for g in match.groups() if g is not None]
        año = next((g for g in groups if len(g) == 4), None)
        mes = next((g for g in groups if len(g) == 2), None)
        if año and mes:
            return (int(año), int(mes))
    return (0, 0)

def encontrar_archivos_a_procesar():
    """
    Busca todos los archivos Excel en las carpetas de brutos estructuradas
    y devuelve una lista de aquellos que necesitan ser procesados.
    """
    ruta_brutos_base = obtener_ruta('ruta_excel_bruto')
    ruta_limpios_base = obtener_ruta('ruta_csv_limpio')
    archivos_pendientes = []
    log_mensajes = []

    # Definir las subcarpetas a explorar y sus tipos asociados
    carpetas_etl = {
        'Tráfico Mensual': ('trafico', os.path.join(ruta_limpios_base, 'Trafico')),
        'Siniestralidad/Ficha 0': ('siniestralidad', os.path.join(ruta_limpios_base, 'Siniestralidad')),
        'Siniestralidad/Ficha 1': ('vehiculos', os.path.join(ruta_limpios_base, 'Vehiculos'))
    }

    log_mensajes.append("Iniciando búsqueda de archivos a procesar...")

    for subcarpeta_bruta, (tipo_etl, carpeta_limpia_destino) in carpetas_etl.items():
        ruta_subcarpeta_actual = os.path.join(ruta_brutos_base, subcarpeta_bruta)
        log_mensajes.append(f"Explorando: {ruta_subcarpeta_actual}")

        if not os.path.isdir(ruta_subcarpeta_actual):
            log_mensajes.append(f"  Advertencia: La carpeta '{subcarpeta_bruta}' no existe. Saltando.")
            continue

        # Iterar por años dentro de cada subcarpeta
        for anio_carpeta in sorted(os.listdir(ruta_subcarpeta_actual)):
            ruta_año = os.path.join(ruta_subcarpeta_actual, anio_carpeta)
            if os.path.isdir(ruta_año): # Asegurarse que es una carpeta (ej: '2016', '2017')
                log_mensajes.append(f"  Dentro de año: {anio_carpeta}")
                # Iterar por archivos dentro de la carpeta del año
                for archivo in sorted(os.listdir(ruta_año)):
                    if archivo.endswith(('.xlsx', '.xls')) and not archivo.startswith('~'):
                        ruta_archivo_bruto = os.path.join(ruta_año, archivo)
                        nombre_base = Path(archivo).stem
                        # Construir la ruta donde debería estar el archivo limpio
                        ruta_carpeta_anio_limpia = os.path.join(carpeta_limpia_destino, anio_carpeta) # usa "anio"
                        ruta_archivo_limpio = os.path.join(ruta_carpeta_anio_limpia, f"{nombre_base}_Limpio.csv")

                        # Si el archivo limpio NO existe, añadir el bruto a la lista de pendientes
                        if not os.path.exists(ruta_archivo_limpio):
                            archivos_pendientes.append({'ruta': ruta_archivo_bruto, 'tipo': tipo_etl})
                            log_mensajes.append(f"    -> PENDIENTE: {archivo}")
                        # else:
                        #     log_mensajes.append(f"    -> Ya procesado: {archivo}") # Opcional: loggear los ya procesados

    if not archivos_pendientes:
        log_mensajes.append("No se encontraron archivos nuevos o pendientes de procesar.")
    else:
        log_mensajes.append(f"Se encontraron {len(archivos_pendientes)} archivos pendientes.")

    # Devolver la lista de diccionarios y los mensajes de log
    return archivos_pendientes, "\n".join(log_mensajes)

# Opción alternativa del archivo más reciente
# def encontrar_archivo_mas_reciente():
#    ruta_brutos = obtener_ruta('ruta_excel_bruto')
#    archivos_excel = [f for f in os.listdir(ruta_brutos) if f.endswith(('.xlsx', '.xls'))]
#    if not archivos_excel:
#        return None, "No se encontraron archivos Excel en la carpeta de entrada."
#    archivos_ordenados = sorted(archivos_excel, key=_extraer_fecha_del_nombre, reverse=True)
#    ruta_completa = os.path.join(ruta_brutos, archivos_ordenados[0])
#    return ruta_completa, f"Archivo más reciente encontrado: {archivos_ordenados[0]}"

def identificar_tipo_por_contenido(ruta_excel):
    try:
        df_preview_cols = pd.read_excel(ruta_excel, nrows=1).columns
        if all(col.startswith('Column') for col in df_preview_cols[:5]):
            return 'siniestralidad'
        
        df_preview_skip = pd.read_excel(ruta_excel, skiprows=6, nrows=1).columns
        columnas_vehiculo = {"código accidente", "tipo vehículo", "servicio"}
        if columnas_vehiculo.issubset({str(c).lower() for c in df_preview_skip}):
            return 'vehiculos'

        xls = pd.ExcelFile(ruta_excel)
        if any(str(hoja).strip().startswith(tuple('123456789')) for hoja in xls.sheet_names):
            return 'trafico'
    except Exception as e:
        print(f"Error al analizar el archivo {ruta_excel}: {e}")
        return 'error'
    return 'desconocido'


def ejecutar_proceso_etl_completo():
    """
    Función orquestadora: Encuentra TODOS los archivos pendientes, los identifica
    (usando la estructura de carpetas) y delega su procesamiento.
    """
    archivos_a_procesar, mensaje_busqueda = encontrar_archivos_a_procesar()

    print(mensaje_busqueda) # Mostrar el log de la búsqueda

    if not archivos_a_procesar:
        return "No hay archivos nuevos para procesar.", None # Mensaje para la GUI

    resultados_procesamiento = []
    archivos_procesados_ruta = [] # Para devolver la lista de archivos que se intentaron procesar

    # Mapa de clases ETL
    etl_map = {
        'siniestralidad': ETLSiniestralidad,
        'trafico': ETLTrafico,
        'vehiculos': ETLVehiculos
    }

    print(f"\nIniciando procesamiento de {len(archivos_a_procesar)} archivos...")

    for archivo_info in archivos_a_procesar:
        ruta_archivo = archivo_info['ruta']
        tipo_archivo = archivo_info['tipo'] # Usamos el tipo determinado por la carpeta
        nombre_archivo = Path(ruta_archivo).name
        archivos_procesados_ruta.append(ruta_archivo) # Añadir a la lista de procesados

        print(f"\nProcesando: {nombre_archivo} (Tipo detectado: {tipo_archivo})")

        ETLClass = etl_map.get(tipo_archivo)

        if not ETLClass:
            mensaje = f"Error: No hay clase ETL para el tipo '{tipo_archivo}' de {nombre_archivo}."
            print(mensaje)
            resultados_procesamiento.append(mensaje)
            continue # Saltar al siguiente archivo

        try:
            etl_instance = ETLClass()
            # Llamar a procesar_archivo (maneja internamente si ya existe, aunque la búsqueda ya lo filtró)
            resultado_exitoso = etl_instance.procesar_archivo(ruta_archivo)

            if resultado_exitoso:
                 mensaje = f"Éxito: {nombre_archivo} procesado."
                 print(mensaje)
            else:
                 # Si devuelve False, fue un fallo controlado (ej. no produjo datos)
                 mensaje = f"Advertencia: {nombre_archivo} procesado pero no generó resultado (ver logs detallados)."
                 print(mensaje)
            resultados_procesamiento.append(mensaje)

        except Exception as e:
            # Capturar errores críticos durante la transformación individual
            mensaje = f"ERROR CRÍTICO al procesar {nombre_archivo}: {e}"
            print(mensaje)
            resultados_procesamiento.append(mensaje)
            # Continuar con el siguiente archivo a pesar del error

    # Mensaje final para la GUI
    resumen_errores = sum(1 for msg in resultados_procesamiento if "ERROR" in msg)
    resumen_advertencias = sum(1 for msg in resultados_procesamiento if "Advertencia" in msg)
    resumen_exitosos = len(resultados_procesamiento) - resumen_errores - resumen_advertencias

    mensaje_final = (
        f"Proceso ETL completado.\n"
        f"- Archivos procesados con éxito: {resumen_exitosos}\n"
        f"- Archivos con advertencias (sin datos generados): {resumen_advertencias}\n"
        f"- Archivos con errores críticos: {resumen_errores}\n\n"
        f"Detalles:\n" + "\n".join([f"  - {msg}" for msg in resultados_procesamiento])
    )

    # Devolvemos el resumen y la lista de archivos procesados (o None si no se procesó ninguno)
    return mensaje_final, archivos_procesados_ruta if archivos_procesados_ruta else None