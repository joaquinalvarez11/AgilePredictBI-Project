import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import os
from config_manager import cargar_ruta_base, guardar_ruta_base
# from utils.backup_manager import BackupManager

class VistaMenuPrincipal(tk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller
        # self.backup_manager = BackupManager()

        # --- Estilos ---
        self.configure(bg="white") # Fondo blanco base

        # Estilos personalizados para este menú
        style = ttk.Style()
        style.configure("MenuTitle.TLabel", background="#004c8c", foreground="white", font=("Helvetica", 22, "bold"))
        style.configure("Card.TButton", font=("Helvetica", 12, "bold"), background="white")
        style.configure("Desc.TLabel", background="white", foreground="#666666", font=("Helvetica", 10))

        # Grid principal
        self.grid_rowconfigure(0, weight=0) # Header Azul
        self.grid_rowconfigure(1, weight=1) # Espacio
        self.grid_rowconfigure(2, weight=0) # Botones
        self.grid_rowconfigure(3, weight=0) # Descripciones
        self.grid_rowconfigure(4, weight=2) # Espacio inferior
        
        self.grid_columnconfigure((0, 1, 2), weight=1)

        # --- Header Superior ---
        # Frame azul arriba para dar identidad corporativa
        header_frame = tk.Frame(self, bg="#004c8c", height=80)
        header_frame.grid(row=0, column=0, columnspan=3, sticky="ew")
        header_frame.grid_propagate(False) # Mantiene altura fija
        
        lbl_title = tk.Label(header_frame, text="Menú Principal", font=("Helvetica", 18, "bold"), bg="#004c8c", fg="white")
        lbl_title.place(relx=0.5, rely=0.5, anchor="center")

        # --- Campo para carpeta base y respaldo ---
        top_frame = ttk.Frame(self)
        top_frame.grid(row=1, column=0, columnspan=3, pady=(20, 20), padx=20, sticky="ew")

        top_frame.columnconfigure(0, weight=1)
        top_frame.columnconfigure(1, weight=1)

        # --- Frame de carpeta base ---
        ruta_frame = ttk.Frame(top_frame)
        ruta_frame.grid(row=0, column=0, sticky="w", padx=10)

        lbl_ruta = ttk.Label(ruta_frame, text="Carpeta principal para resultados:")
        lbl_ruta.pack(anchor="w")
        
        self.ruta_var = tk.StringVar()
        self.entry_ruta = ttk.Entry(ruta_frame, width=50, textvariable=self.ruta_var, state="readonly")
        self.entry_ruta.pack(side="left")

        btn_ruta = ttk.Button(ruta_frame, text="📂", width=3, command=self.seleccionar_ruta)
        btn_ruta.pack(side="left", padx=(10, 0))

        # Cargar ruta guardada
        ultima_ruta = cargar_ruta_base()
        if ultima_ruta:
            self.ruta_var.set(ultima_ruta)
            self.controller.ruta_base = ultima_ruta
        
        # --- Frame de OneDrive ---
        onedrive_frame = ttk.Frame(top_frame)
        onedrive_frame.grid(row=0, column=1, sticky="e", padx=10)

        lbl_onedrive = ttk.Label(onedrive_frame, text="Gestión en OneDrive:")
        lbl_onedrive.pack(anchor="w")

        btn_backup = ttk.Button(onedrive_frame, text="☁️ Respaldar", width=16, command=self.respaldar_archivos)
        btn_backup.pack(side="left", padx=5)

        btn_recuperar = ttk.Button(onedrive_frame, text="📥 Recuperar", width=16, command=self.recuperar_archivos)
        btn_recuperar.pack(side="left", padx=5)

        # --- Contenido ---
        self.ESPACIO_RESERVADO = "\n\n"

        # Definimos los botones (ETL, ML, Export)
        # Uso de ipady/ipadx para que se sientan como "tarjetas"
        
        # 1. ETL
        btn_etl = ttk.Button(self, text="Gestión de Datos (ETL)", command=lambda: self.validar_y_navegar(controller.show_etl_view), style="Card.TButton", width=25)
        btn_etl.grid(row=2, column=0, padx=20, pady=(50, 15), ipady=10)

        self.lbl_etl_desc = ttk.Label(self, text=self.ESPACIO_RESERVADO, style="Desc.TLabel", justify="center", wraplength=250)
        self.lbl_etl_desc.grid(row=3, column=0, padx=20, sticky="n")

        # 2. ML
        btn_ml = ttk.Button(self, text="Análisis Predictivo", command=lambda: self.validar_y_navegar(controller.show_ml_view), style="Card.TButton", width=25)
        btn_ml.grid(row=2, column=1, padx=20, pady=(50, 15), ipady=10)

        self.lbl_ml_desc = ttk.Label(self, text=self.ESPACIO_RESERVADO, style="Desc.TLabel", justify="center", wraplength=250)
        self.lbl_ml_desc.grid(row=3, column=1, padx=20, sticky="n")

        # 3. Export
        btn_export = ttk.Button(self, text="Exportación de Informes", command=lambda: self.validar_y_navegar(controller.show_export_view), style="Card.TButton", width=25)
        btn_export.grid(row=2, column=2, padx=20, pady=(50, 15), ipady=10)

        self.lbl_export_desc = ttk.Label(self, text=self.ESPACIO_RESERVADO, style="Desc.TLabel", justify="center", wraplength=250)
        self.lbl_export_desc.grid(row=3, column=2, padx=20, sticky="n")

        # --- Eventos Hover ---
        btn_etl.bind("<Enter>", lambda e: self.lbl_etl_desc.config(text="Ciclo completo de ingesta:\nLimpieza de Excel y Carga a BD."))
        btn_etl.bind("<Leave>", lambda e: self.lbl_etl_desc.config(text=self.ESPACIO_RESERVADO))
        
        btn_ml.bind("<Enter>", lambda e: self.lbl_ml_desc.config(text="Proyecciones inteligentes:\nDetección de patrones en siniestralidad."))
        btn_ml.bind("<Leave>", lambda e: self.lbl_ml_desc.config(text=self.ESPACIO_RESERVADO))
        
        btn_export.bind("<Enter>", lambda e: self.lbl_export_desc.config(text="Centro de Reportes:\nCaptura automatizada desde Power BI."))
        btn_export.bind("<Leave>", lambda e: self.lbl_export_desc.config(text=self.ESPACIO_RESERVADO))

    # Selección de ruta principal
    def seleccionar_ruta(self):
        ruta = filedialog.askdirectory(title="Seleccionar Carpeta Principal")
        if ruta:
            # Actualizar el campo
            self.ruta_var.set(ruta)
            self.controller.ruta_base = ruta
            guardar_ruta_base(ruta)
    
    # Validar si la carpeta principal está definida y si existe
    def validar_y_navegar(self, funcion_navegar):
        ruta_base = getattr(self.controller, "ruta_base", "")
        if not ruta_base or not os.path.exists(ruta_base):
            messagebox.showerror("Error", "Carpeta principal no definida o no existe.\nDebe definir una carpeta principal válida antes de continuar.")
            return
        
        funcion_navegar()

    # Respaldo de archivos (base de datos, CSV limpios, gráficos y CSV de las predicciones)
    def respaldar_archivos(self):
        ruta_base = getattr(self.controller, "ruta_base", "")
        if not ruta_base or not os.path.exists(ruta_base):
            messagebox.showerror("Error", "Carpeta principal no definida o no existe.\nDebe definir una carpeta principal válida antes de respaldar.")
            return
        
        try:
            # TODO: Registrar la aplicación para el acceso a OneDrive
            # self.backup_manager.respaldar()
            messagebox.showinfo("Respaldar", "Respaldo completado en OneDrive.")
        except Exception as e:
            messagebox.showerror("Error", f"No se pudo realizar el respaldo:\n{e}")
    
    # Recuperación de archivos (mencionado del método anterior)
    def recuperar_archivos(self):
        ruta_base = getattr(self.controller, "ruta_base", "")
        if not ruta_base or not os.path.exists(ruta_base):
            messagebox.showerror("Error", "Carpeta principal no definida o no existe.\nDebe definir una carpeta principal válida antes de recuperar.")
            return
        
        try:
            # TODO: Registrar la aplicación para el acceso a OneDrive
            # self.backup_manager.recuperar("backup.zip.enc", destino=ruta_base)
            messagebox.showinfo("Recuperar", f"Respaldo recuperado y extraído en la carpeta '{ruta_base}'.")
        except Exception as e:
            messagebox.showerror("Error", f"No se pudo recuperar el respaldo:\n{e}")
