import os
import time
import sys
import subprocess

# --- 0. Definir rutas basadas en la ubicación de este script ---
base_path = os.path.dirname(__file__)
scripts_path = os.path.join(base_path, "scripts")
dim_path = os.path.join(scripts_path, "dim")
fact_path = os.path.join(scripts_path, "fact")

# --- 1. Definir el orden de ejecución ---
scripts_creacion = [
    os.path.join(scripts_path, "crear_tablas.py"),
    os.path.join(dim_path, "cargar_dimDateTime.py"),
    os.path.join(dim_path, "cargar_dimKm.py"),
    os.path.join(dim_path, "cargar_dimensiones.py")
]

scripts_carga_hechos = [
    os.path.join(fact_path, "cargar_factTraffic.py"),
    os.path.join(fact_path, "cargar_factAccident.py"),
    os.path.join(fact_path, "cargar_factVehicleAccident.py")
]

scripts_post_carga = [
    os.path.join(fact_path, "cargar_factAccident_conteo.py")
]

# --- Función para ejecutar y reenviar logs ---
def ejecutar_script(script_path, callback):
    """
    Ejecuta un script usando subprocess y reenvía su salida
    en tiempo real al callback de la GUI.
    """
    script_name = os.path.basename(script_path)
    script_dir = os.path.dirname(script_path)
    
    # Nombres amigables para los scripts
    nombres_amigables = {
        "crear_tablas.py": "Verificando estructura de base de datos...",
        "cargar_dimDateTime.py": "Actualizando dimensión de Tiempo...",
        "cargar_dimKm.py": "Actualizando dimensión de Kilometraje...",
        "cargar_dimensiones.py": "Actualizando catálogos maestros...",
        "cargar_factTraffic.py": "Cargando datos de Tráfico...",
        "cargar_factAccident.py": "Cargando datos de Siniestralidad (Accidentes)...",
        "cargar_factVehicleAccident.py": "Cargando datos de Vehículos...",
        "cargar_factAccident_conteo.py": "Calculando totales y estadísticas..."
    }
    
    titulo = nombres_amigables.get(script_name, f"Ejecutando {script_name}...")
    callback(f"\n>>> {titulo}") # Mensaje limpio de inicio

    start_time = time.time()
    
    # Usamos subprocess.Popen para capturar stdout línea por línea
    # 'python -u' es para salida "sin búfer", crucial para logs en vivo
    my_env = os.environ.copy()
    my_env["PYTHONIOENCODING"] = "utf-8"

    process = subprocess.Popen(
        ['python', '-u', script_name],
        cwd=script_dir,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT, # Captura stdout y stderr en el mismo pipe
        text=True,
        encoding='utf-8',
        errors='replace',
        env=my_env
    )

    # --- LISTA NEGRA DE LOGS (RUIDO TÉCNICO) ---
    ignorar_si_contiene = [
        "Conectando a la base de datos",
        "Creando mapas",
        "Mapas de puentes",
        "Mapas de validación",
        "Consultando log",
        "Buscando archivos CSV",
        "Insertando datos en tablas",
        "Commit",
        "Tablas creadas correctamente",
        "PRAGMA foreign_keys",
        "sys.path",
        "config_manager",
        "DEBUG:",
        "Duración:",
        "Finalizado:",
        "Conexión cerrada",
        "Iniciando script de actualización",
        "Se encontraron 0 archivos",
        "Optimizando",
        "Carga de CSVs completada"
    ]

    # Leer la salida del subproceso línea por línea
    while True:
        linea = process.stdout.readline()
        if linea == '' and process.poll() is not None:
            break
        
        if linea:
            texto = linea.strip()
            if not texto: continue # Ignorar líneas vacías
            
            # 1. Filtro de Ruido
            if any(frase in texto for frase in ignorar_si_contiene):
                continue
                
            # 2. Formato Ejecutivo (Reemplazos visuales)
            if "Leyendo:" in texto:
                # Simplificar: "Leyendo: archivo.csv" -> "-> Procesando: archivo.csv"
                archivo = texto.split("Leyendo:")[-1].strip()
                callback(f"  -> Procesando: {archivo}")
            
            elif "Procesando Archivo:" in texto:
                 archivo = texto.split("Procesando Archivo:")[-1].strip()
                 callback(f"  -> Procesando: {archivo}")

            elif "ADVERTENCIA" in texto or "WARNING" in texto:
                callback(f"  ⚠️  {texto}") # Icono de alerta
            
            elif "ERROR" in texto or "Error:" in texto:
                callback(f"   {texto}") # Icono de error
                
            elif "archivos 100% limpios" in texto:
                callback(f"   {texto}") # Éxito
            
            elif "Se encontraron" in texto and "NUEVOS" in texto:
                # "Se encontraron 105 archivos CSV NUEVOS..." -> "Se detectaron 105 archivos nuevos."
                nums = [int(s) for s in texto.split() if s.isdigit()]
                if nums:
                    callback(f"    Se detectaron {nums[0]} archivos nuevos pendientes.")
            
            elif "Se omitieron" in texto: # Reporte final de errores
                callback(f"\n   RESUMEN DE ERRORES:\n  {texto}")

            elif "- ID Accidente:" in texto or "- Archivo:" in texto: # Detalles del error
                callback(f"    {texto}")

            elif "Se actualizaron" in texto: # Conteo final
                 callback(f"   {texto}")

            else:
                pass 
                callback(f"    {texto}")

    # Esperar a que termine y obtener el código de error
    codigo_error = process.poll()
    
    end_time = time.time()
    callback(f"--- Finalizado: {script_name} (Duración: {end_time - start_time:.2f}s) ---")
    
    if codigo_error != 0:
        error_msg = f"El script '{script_name}' falló con el código de error: {codigo_error}"
        callback(f"\n--- ¡ERROR CRÍTICO! ---")
        callback(error_msg)
        raise Exception(error_msg)

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
            for script in scripts_en_fase:
                if cancel_event.is_set():
                    callback_progreso("Carga a DB cancelada por el usuario.")
                    return "Carga de Base de Datos cancelada."
                
                ejecutar_script(script, callback_progreso)

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
    def simple_print_callback(mensaje):
        print(mensaje)
    
    # Un 'evento' falso que nunca se activa
    class FakeEvent:
        def is_set(self): return False
            
    print("Ejecutando carga_bd.py en modo de prueba...")
    ejecutar_carga_db_completa(simple_print_callback, FakeEvent())