import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import os
import time
import shutil
import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from PyPDF2 import PdfReader, PdfWriter

# Ruta temporal donde Power BI crea las carpetas "print-job"
USER_HOME = os.path.expanduser("~")
CARPETA_ORIGEN_PBI = os.path.join(USER_HOME, r"AppData\Local\Temp\Power BI Desktop")

class VistaExportar(ttk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller
        
        # --- Estilos ---
        style = ttk.Style()
        style.configure("White.TFrame", background="white")
        
        # Aplicamos el estilo (En lugar de bg="white")
        self.configure(style="White.TFrame") 

        # Estilos adicionales
        style.configure("Export.TLabelframe", background="white")
        style.configure("Export.TLabelframe.Label", background="white", foreground="#004c8c", font=("Helvetica", 11, "bold"))
        style.configure("Export.TLabel", background="white", font=("Helvetica", 10))
        style.configure("Status.TLabel", background="#f8f9fa", font=("Consolas", 9))

        # --- Variables de Estado ---
        self.monitor_activo = False
        self.observer = None
        
        path_escritorio = os.path.join(USER_HOME, "Desktop")
        self.ruta_destino = tk.StringVar(value=path_escritorio)
        self.tipo_reporte = tk.StringVar(value="Tráfico") 
        self.paginas_seleccion = tk.StringVar(value="1-3")
        self.estado_texto = tk.StringVar(value="Sistema en espera. Configure y active el monitor.")

        # --- Interfaz Gráfica ---
        self.columnconfigure(0, weight=1)
        self.columnconfigure(1, weight=1)
        
        # Header Azul
        header_frame = tk.Frame(self, bg="#f0f4f8", height=60)
        header_frame.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 20))
        tk.Label(header_frame, text="Gestor de Exportación Power BI", font=("Helvetica", 16, "bold"), bg="#f0f4f8", fg="#004c8c").pack(pady=15)

        # --- Panel de Configuración (Izquierda) ---
        frame_config = ttk.LabelFrame(self, text="Parámetros de Exportación", padding=20, style="Export.TLabelframe")
        frame_config.grid(row=1, column=0, sticky="nsew", padx=20, pady=10)

        # 1. Carpeta Destino
        ttk.Label(frame_config, text="Directorio de salida:", style="Export.TLabel").pack(anchor="w", pady=5)
        frame_ruta = ttk.Frame(frame_config, style="Export.TLabelframe") # Hereda bg blanco
        frame_ruta.pack(fill="x", pady=5)
        
        entry_ruta = ttk.Entry(frame_ruta, textvariable=self.ruta_destino, state="readonly")
        entry_ruta.pack(side="left", fill="x", expand=True)
        ttk.Button(frame_ruta, text="📂", width=3, command=self.seleccionar_carpeta).pack(side="right", padx=5)

        # 2. Selector de Tipo
        ttk.Label(frame_config, text="Categoría del Informe:", style="Export.TLabel").pack(anchor="w", pady=(20, 5))
        opciones_reporte = ["Tráfico", "Siniestro", "Predicción", "Ejecutivo_General"]
        combo_tipo = ttk.Combobox(frame_config, textvariable=self.tipo_reporte, values=opciones_reporte, state="readonly", font=("Helvetica", 10))
        combo_tipo.pack(fill="x", pady=5)
        ttk.Label(frame_config, text="Nomenclatura: Categoría_Fecha.pdf", font=("Helvetica", 8), foreground="#888888", background="white").pack(anchor="w")

        # 3. Páginas
        ttk.Label(frame_config, text="Páginas a procesar (Ej: 1, 3-5):", style="Export.TLabel").pack(anchor="w", pady=(20, 5))
        ttk.Entry(frame_config, textvariable=self.paginas_seleccion).pack(fill="x")
        ttk.Label(frame_config, text="Deje vacío para archivo completo.", font=("Helvetica", 8), foreground="#888888", background="white").pack(anchor="w")

        # --- Panel de Control (Derecha) ---
        frame_control = ttk.Frame(self, padding=20, style="Export.TLabelframe") # Usamos el estilo para bg blanco
        frame_control.grid(row=1, column=1, sticky="nsew", padx=20, pady=10)

        # Botón Monitor
        self.btn_monitor = tk.Button(
            frame_control, 
            text="INICIAR MONITOR", 
            bg="#004c8c", fg="white", font=("Helvetica", 12, "bold"),
            command=self.toggle_monitor,
            height=2, relief="flat", cursor="hand2",
            activebackground="#003366", activeforeground="white"
        )
        self.btn_monitor.pack(fill="x", pady=(0, 20))

        # Consola de Estado
        ttk.Label(frame_control, text="Bitácora de Actividad:", style="Export.TLabel", font=("Helvetica", 10, "bold")).pack(anchor="w")
        
        self.lbl_consola = ttk.Label(
            frame_control, 
            textvariable=self.estado_texto, 
            style="Status.TLabel",
            relief="groove", 
            anchor="nw",
            padding=10,
            wraplength=350
        )
        self.lbl_consola.pack(fill="both", expand=True)

        # Botón Volver
        ttk.Button(self, text="Volver al Menú Principal", command=lambda: controller.show_menu_principal()).grid(row=2, column=0, columnspan=2, pady=30)

    def seleccionar_carpeta(self):
        folder = filedialog.askdirectory()
        if folder:
            self.ruta_destino.set(folder)

    def toggle_monitor(self):
        if not self.monitor_activo:
            self.iniciar_monitor()
        else:
            self.detener_monitor()

    def iniciar_monitor(self):
        if not os.path.exists(CARPETA_ORIGEN_PBI):
            try:
                os.makedirs(CARPETA_ORIGEN_PBI, exist_ok=True)
            except:
                messagebox.showerror("Error Crítico", f"No se encuentra la ruta de Power BI:\n{CARPETA_ORIGEN_PBI}")
                return

        # Usamos la lógica de carpetas (FolderHandler)
        self.event_handler = FolderHandler(self.procesar_carpeta_detectada)
        self.observer = Observer()
        
        # Solo miramos si aparece una carpeta "print-job" en la raíz. No miramos dentro todavía.
        self.observer.schedule(self.event_handler, path=CARPETA_ORIGEN_PBI, recursive=False)
        
        try:
            self.observer.start()
            self.monitor_activo = True
            self.btn_monitor.config(text="DETENER MONITOR", bg="#d9534f") # Rojo al estar activo
            self.estado_texto.set("MONITOR ACTIVO\nEsperando que Power BI genere una carpeta de exportación...")
        except Exception as e:
            messagebox.showerror("Error", f"Fallo al iniciar Watchdog: {e}")

    def detener_monitor(self):
        if self.observer:
            self.observer.stop()
            self.observer.join()
        self.monitor_activo = False
        self.btn_monitor.config(text="INICIAR MONITOR", bg="#337ab7")
        self.estado_texto.set("Monitor detenido.")

    def procesar_carpeta_detectada(self, ruta_carpeta):
        """
        Se ejecuta cuando aparece una carpeta (print-job).
        Esta función corre en un hilo secundario (Watchdog).
        """
        self.after(0, lambda: self.estado_texto.set(f"Detectada exportación en curso...\nEsperando finalización de Power BI (15s)..."))
        
        # 1. Espera de Seguridad para Power BI
        # Esperamos a que Power BI termine de escribir el archivo dentro de la carpeta
        time.sleep(15) 

        # 2. Buscar el PDF dentro de esa carpeta
        archivos = []
        try:
            archivos = [f for f in os.listdir(ruta_carpeta) if f.lower().endswith(".pdf")]
        except FileNotFoundError:
            self.after(0, lambda: self.estado_texto.set("Error: La carpeta temporal desapareció antes de tiempo."))
            return

        if not archivos:
            self.after(0, lambda: self.estado_texto.set("No se encontró PDF dentro de la carpeta detectada."))
            return

        ruta_pdf_origen = os.path.join(ruta_carpeta, archivos[0])
        
        # 3. Procesar
        self.ejecutar_transformacion(ruta_pdf_origen)

    def ejecutar_transformacion(self, ruta_origen):
        try:
            # Construir nombre final
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S")
            tipo = self.tipo_reporte.get() # Obtener valor del Combobox
            nombre_final = f"{tipo}_{timestamp}.pdf"
            ruta_final = os.path.join(self.ruta_destino.get(), nombre_final)

            # Mover (Intentar varias veces por si acaso sigue bloqueado)
            shutil.move(ruta_origen, ruta_final)

            # Limpiar páginas (Cortar)
            paginas_str = self.paginas_seleccion.get()
            if paginas_str:
                ruta_limpia = self.limpiar_paginas(ruta_final, paginas_str)
                os.remove(ruta_final)
                os.rename(ruta_limpia, ruta_final)

            # Notificar éxito en el hilo principal
            self.after(0, lambda: self.exito_ui(nombre_final))

        except Exception as e:
            self.after(0, lambda: messagebox.showerror("Error", f"Error procesando PDF: {e}"))
            self.after(0, lambda: self.estado_texto.set(f"Error: {str(e)}"))

    def limpiar_paginas(self, ruta_pdf, paginas_str):
        """Misma lógica de limpieza, robusta ante errores de input"""
        reader = PdfReader(ruta_pdf)
        writer = PdfWriter()
        total_paginas = len(reader.pages)
        
        indices = set()
        try:
            for parte in paginas_str.split(','):
                parte = parte.strip()
                if '-' in parte:
                    ini, fin = map(int, parte.split('-'))
                    indices.update(range(ini, fin + 1))
                elif parte.isdigit():
                    indices.add(int(parte))
        except:
            return ruta_pdf # Si falla el parseo, devolver original

        paginas_agregadas = 0
        for i in sorted(indices):
            if 1 <= i <= total_paginas:
                writer.add_page(reader.pages[i-1])
                paginas_agregadas += 1
        
        if paginas_agregadas == 0: return ruta_pdf

        ruta_temp = ruta_pdf.replace(".pdf", "_temp.pdf")
        with open(ruta_temp, "wb") as f:
            writer.write(f)
        return ruta_temp

    def exito_ui(self, nombre):
        self.estado_texto.set(f"Finalizado.\nArchivo generado: {nombre}\nUbicación: {self.ruta_destino.get()}")
        # Opcional: Abrir carpeta o mostrar popup. Para ejecutivo, el texto verde en consola suele bastar.
        # messagebox.showinfo("Listo", f"Reporte generado: {nombre}") 

# --- Handler Modificado para Carpetas ---
class FolderHandler(FileSystemEventHandler):
    def __init__(self, callback):
        self.callback = callback
        self.ultimo_tiempo = 0

    def on_created(self, event):
        # AHORA SOLO NOS IMPORTAN LAS CARPETAS
        if not event.is_directory: 
            return
        
        # Verificar que sea una carpeta temporal de Power BI (opcional, pero recomendado)
        # Power BI suele crear carpetas "print-job-xxxx" o GUIDs. 
        # Aceptamos cualquier carpeta nueva en Temp/Power BI Desktop
        
        ahora = time.time()
        if ahora - self.ultimo_tiempo < 5: # Debounce de 5s
            return
        self.ultimo_tiempo = ahora

        # Llamamos al callback que tiene el sleep interno
        self.callback(event.src_path)