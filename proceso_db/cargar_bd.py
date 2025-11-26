import time

# --- 0. Importar directamente los módulos ---
from proceso_db.scripts import crear_tablas
from proceso_db.scripts.dim import cargar_dimDateTime, cargar_dimensiones, cargar_dimKm
from proceso_db.scripts.fact import cargar_factAccident, cargar_factAccident_conteo, cargar_factTraffic, cargar_factVehicleAccident

# --- 1. Definir el orden de ejecución ---
scripts_creacion = [
    ("Verificando estructura de base de datos...", crear_tablas),
    ("Actualizando dimensión de Tiempo...", cargar_dimDateTime),
    ("Actualizando dimensión de Kilometraje...", cargar_dimKm),
    ("Actualizando catálogos maestros...", cargar_dimensiones)
]

scripts_carga_hechos = [
    ("Cargando datos de Tráfico...", cargar_factTraffic),
    ("Cargando datos de Siniestralidad (Accidentes)...", cargar_factAccident),
    ("Cargando datos de Vehículos...", cargar_factVehicleAccident)
]

scripts_post_carga = [
    ("Calculando totales y estadísticas...", cargar_factAccident_conteo)
]

# --- 2. Lógica Principal como función ---
def ejecutar_carga_db_completa(callback_progreso, cancel_event):
    """
    Función principal llamada desde la GUI.
    Acepta un callback para enviar logs.
    """
    try:
        callback_progreso("Iniciando proceso de carga a la Base de Datos...")
        
        all_scripts = [
            ("--- FASE 1: CREANDO ESTRUCTURA Y DIMENSIONES ---", scripts_creacion),
            ("--- FASE 2: CARGANDO DATOS DE HECHOS ---", scripts_carga_hechos),
            ("--- FASE 3: ENRIQUECIENDO DATOS (POST-CARGA) ---", scripts_post_carga)
        ]
        
        # --- Calcular total de scripts ---
        total_scripts = sum(len(fase[1]) for fase in all_scripts)
        scripts_completados = 0

        for fase_titulo, scripts_en_fase in all_scripts:
            callback_progreso(f"\n{fase_titulo}")
            for titulo, modulo in scripts_en_fase:
                if cancel_event.is_set():
                    callback_progreso("Carga a DB cancelada por el usuario.")
                    return "Carga de Base de Datos cancelada."
                
                callback_progreso(f"\n>>> {titulo}")
                start_time = time.time()
                modulo.run(callback_progreso)
                end_time = time.time()
                callback_progreso(f"--- Finalizado: {titulo} (Duración: {end_time - start_time:.2f}s) ---")

                # --- Actualizar progreso ---
                scripts_completados += 1
                # Enviamos mensaje vacío para no ensuciar el log, pero pasamos los números
                callback_progreso("", scripts_completados, total_scripts)
        
        resumen = "Carga a la Base de Datos completada con éxito."
        callback_progreso(f"\n\n--- {resumen} ---")
        return resumen

    except Exception as e:
        error_msg = f"El proceso de carga a DB se interrumpió: {e}"
        callback_progreso(error_msg)
        # Re-lanzar la excepción para que el hilo de la GUI la capture
        raise e

# --- Esto permite probar el script de forma independiente ---
if __name__ == "__main__":
    
    # Un 'callback' falso que solo imprime a consola
    def simple_print_callback(mensaje, actual=0, total=0):
        print(mensaje)
    
    # Un 'evento' falso que nunca se activa
    class FakeEvent:
        def is_set(self): return False
            
    print("Ejecutando carga_bd.py en modo de prueba...")
    ejecutar_carga_db_completa(simple_print_callback, FakeEvent())
