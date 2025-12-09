import time
import os
import re
from config_manager import obtener_ruta
from utils.ocultar_bd import set_file_hidden

# --- Importar módulos ---
from proceso_db.scripts import crear_tablas
from proceso_db.scripts.dim import cargar_dimDateTime, cargar_dimensiones, cargar_dimKm
from proceso_db.scripts.fact import cargar_factAccident, cargar_factAccident_conteo, cargar_factTraffic, cargar_factVehicleAccident

class GestorLogEjecutivo:
    def __init__(self, callback_real):
        self.callback_real = callback_real
        self.archivos_con_error_total = set()
        self.buffer_reportes_parciales = []
        self.capturando_resumen = False
        self.start_time = time.time()
    
    def log_filtrado(self, mensaje, actual=None, total=None):
        """
        Gestor inteligente de logs:
        - Muestra progreso general.
        - Oculta errores técnicos en vivo.
        - Graba los reportes detallados de los sub-scripts para mostrarlos al final.
        """
        # 1. Log Interno
        if mensaje and mensaje.strip():
            print(f"[INTERNAL LOG] {mensaje.strip()}")

        if not mensaje: 
            self.callback_real("", actual, total)
            return

        msg_clean = mensaje.strip()

        # --- A. DETECCIÓN DE INICIO/FIN DE REPORTES ---
        # Detectamos el inicio del reporte
        if "Reporte Detallado" in mensaje and "Errores" in mensaje:
            self.capturando_resumen = True
            
            tipo_reporte = "Ficha 1 (Vehículos)"
            if "Tráfico" in mensaje: tipo_reporte = "Tráfico Mensual"
            elif "Ficha 0" in mensaje: tipo_reporte = "Ficha 0 (Accidentes)"
            
            # Solo guardamos un título limpio, no mostramos nada en vivo
            self.buffer_reportes_parciales.append(f"\n🔸 DETALLE: {tipo_reporte}")
            return 

        if "Proceso ETL para" in mensaje and "completado" in mensaje:
            self.capturando_resumen = False
            return 

        # --- B. MODO GRABACIÓN ---
        if self.capturando_resumen:
            condiciones_captura = [
                msg_clean.startswith("- Archivo:"),
                msg_clean.startswith("- ID Accidente:"),
                msg_clean.startswith("Error:"),
                msg_clean.startswith("Se omitieron"),
                "Datos:" in msg_clean
            ]
            if any(condiciones_captura):
                self.buffer_reportes_parciales.append(f"   {msg_clean}")
            return # Grabamos y silenciamos

        # --- C. FILTROS EN TIEMPO REAL ---
        if "ADVERTENCIA: Se encontraron errores en" in mensaje:
            match = re.search(r"en (.*)\. Se revirtió", mensaje)
            if match:
                self.archivos_con_error_total.add(match.group(1))
            return 

        filtros_basura = [
            "Error de Integridad",
            "FOREIGN KEY",
            "Error fatal",
            "Valores FK:",
            "Se omitieron" 
        ]
        if any(x in mensaje for x in filtros_basura):
            return 

        # --- D. PASO A TRAVÉS ---
        self.callback_real(mensaje, actual, total)


    def obtener_resumen_final(self):
        msgs = []
        if self.archivos_con_error_total:
            msgs.append("\nARCHIVOS RECHAZADOS (Rollback completo):")
            for arch in sorted(self.archivos_con_error_total):
                msgs.append(f" • {arch}")
        
        if self.buffer_reportes_parciales:
            # Agregamos un salto de línea antes de los detalles
            msgs.append("") 
            msgs.extend(self.buffer_reportes_parciales)
        
        if not msgs:
            return None
        return "\n".join(msgs)

# --- Configuración ---
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

def ejecutar_carga_db_completa(callback_progreso, cancel_event):
    gestor = GestorLogEjecutivo(callback_progreso)
    try:
        gestor.log_filtrado("Iniciando proceso de carga a la Base de Datos...")
        
        all_scripts = [
            ("--- FASE 1: ESTRUCTURA Y DIMENSIONES ---", scripts_creacion),
            ("--- FASE 2: CARGA DE HECHOS ---", scripts_carga_hechos),
            ("--- FASE 3: POST-PROCESAMIENTO ---", scripts_post_carga)
        ]
        
        total_scripts = sum(len(fase[1]) for fase in all_scripts)
        scripts_completados = 0

        for fase_titulo, scripts_en_fase in all_scripts:
            gestor.log_filtrado(f"\n{fase_titulo}")
            for titulo, modulo in scripts_en_fase:
                if cancel_event.is_set():
                    gestor.log_filtrado("! Carga cancelada por el usuario.")
                    return "Carga cancelada."
                
                gestor.log_filtrado(f">>> Ejecutando: {titulo}")
                modulo.run(gestor.log_filtrado)
                scripts_completados += 1
                gestor.log_filtrado("", scripts_completados, total_scripts)
        
        try:
            ruta_db = obtener_ruta("ruta_database")
            if ruta_db and os.path.exists(ruta_db): set_file_hidden(ruta_db)
        except Exception: pass

        resumen = gestor.obtener_resumen_final()
        
        if resumen:
            # Imprimir el encabezado usando el gestor
            gestor.log_filtrado("\n" + "="*50)
            gestor.log_filtrado(" RESUMEN EJECUTIVO DE CARGA")
            gestor.log_filtrado("="*50)
            
            # Imprimir el resumen usando el callback original.
            # Así evitamos que el gestor se "auto-censure" al ver las palabras "Error", "FK", etc.
            callback_progreso(resumen) 
            
            gestor.log_filtrado("\nEl resto de la información se cargó correctamente.")
            return "Carga con Observaciones"
        else:
            gestor.log_filtrado("\nCarga a Base de Datos completada PERFECTAMENTE.")
            return "Éxito Total"

    except Exception as e:
        gestor.log_filtrado(f"Error crítico DB: {e}")
        raise e