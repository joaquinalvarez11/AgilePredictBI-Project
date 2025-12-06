import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import os
from config_manager import obtener_ruta, cargar_ruta_base, guardar_ruta_base
from utils.backup_manager import BackupManager
from utils.ocultar_bd import remove_file_force

class VistaMenuPrincipal(tk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller
        self.backup_manager = BackupManager()

        # --- Estilos Globales del Frame ---
        self.configure(bg="white") 

        # --- Definición de Estilos ---
        style = ttk.Style()
        
        # Estilo para tarjetas principales
        style.configure("Card.TButton", font=("Segoe UI", 11, "bold"), background="white")
        
        # Estilo para etiquetas descriptivas
        style.configure("Desc.TLabel", background="white", foreground="#666666", font=("Segoe UI", 10))
        
        # Estilo para botones de acción secundarios (OneDrive)
        style.configure("Outline.TButton", font=("Segoe UI", 9))

        # --- Configuración del Grid Principal ---
        self.grid_rowconfigure(0, weight=0) # Header
        self.grid_rowconfigure(1, weight=0) # Panel de Configuración (Auto-ajustable)
        self.grid_rowconfigure(2, weight=1) # Espacio flexible
        self.grid_rowconfigure(3, weight=0) # Botones Principales
        self.grid_rowconfigure(4, weight=0) # Descripciones
        self.grid_rowconfigure(5, weight=2) # Espacio inferior
        
        self.grid_columnconfigure((0, 1, 2), weight=1)

        # 1. HEADER
        header_frame = tk.Frame(self, bg="#004c8c", height=70) # Un poco más delgado se ve más elegante
        header_frame.grid(row=0, column=0, columnspan=3, sticky="ew")
        header_frame.grid_propagate(False) 
        
        lbl_title = tk.Label(header_frame, text="Menú Principal", font=("Segoe UI", 18, "bold"), bg="#004c8c", fg="white")
        lbl_title.place(relx=0.03, rely=0.5, anchor="w") # Alineado a la izquierda se ve más moderno

        # Botón Cerrar Sesión
        btn_logout = tk.Button(header_frame, text="Cerrar Sesión", 
                               bg="#003d73", fg="white", font=("Segoe UI", 9),
                               bd=0, activebackground="#005599", activeforeground="white",
                               cursor="hand2", padx=15,
                               command=self.cerrar_sesion)
        btn_logout.place(relx=0.97, rely=0.5, anchor="e")

        # 2. CONFIGURACIÓN
        # Usamos un LabelFrame para agrupar visualmente la configuración
        config_frame = tk.LabelFrame(self, text=" Configuración de Entorno de Trabajo ", 
                                     font=("Segoe UI", 10, "bold"), 
                                     bg="white", fg="#004c8c", bd=1, relief="solid")
        config_frame.grid(row=1, column=0, columnspan=3, padx=40, pady=(30, 10), sticky="ew")

        # Layout interno del config_frame
        config_frame.columnconfigure(1, weight=1) # El entry se expande
        
        # --- A. Selección de Ruta ---
        lbl_ruta = tk.Label(config_frame, text="Carpeta de Resultados:", bg="white", font=("Segoe UI", 9, "bold"), fg="#444")
        lbl_ruta.grid(row=0, column=0, padx=(20, 10), pady=15, sticky="w")

        self.ruta_var = tk.StringVar()
        self.entry_ruta = ttk.Entry(config_frame, textvariable=self.ruta_var, state="readonly")
        self.entry_ruta.grid(row=0, column=1, padx=5, pady=15, sticky="ew")

        lbl_ejemplo = tk.Label(config_frame, text=".../SCRDA Excel/", bg="white", fg="#888888", font=("Segoe UI", 9, "italic"))
        lbl_ejemplo.grid(row=0, column=2, padx=(0, 10), pady=15)

        btn_ruta = ttk.Button(config_frame, text="Examinar...", command=self.seleccionar_ruta, style="Outline.TButton")
        btn_ruta.grid(row=0, column=3, padx=(0, 20), pady=15)

        # Cargar ruta previa
        ultima_ruta = cargar_ruta_base()
        if ultima_ruta:
            self.ruta_var.set(ultima_ruta)
            self.controller.ruta_base = ultima_ruta
        else:
            self.ruta_var.set(" No definida...")

        # --- Separador Vertical Visual ---
        sep = ttk.Separator(config_frame, orient="vertical")
        sep.grid(row=0, column=4, sticky="ns", padx=10, pady=10)

        # --- B. Gestión OneDrive ---
        lbl_cloud = tk.Label(config_frame, text="Sincronización Cloud:", bg="white", font=("Segoe UI", 9, "bold"), fg="#444")
        lbl_cloud.grid(row=0, column=5, padx=(10, 10), pady=15, sticky="w")

        # Frame para agrupar los botones de la nube
        cloud_btns_frame = tk.Frame(config_frame, bg="white")
        cloud_btns_frame.grid(row=0, column=6, padx=(0, 20), pady=15)

        # Botones limpios
        btn_backup = ttk.Button(cloud_btns_frame, text="Respaldar", command=self.respaldar_archivos, style="Outline.TButton")
        btn_backup.pack(side="left", padx=5)

        btn_restore = ttk.Button(cloud_btns_frame, text="Restaurar", command=self.recuperar_archivos, style="Outline.TButton")
        btn_restore.pack(side="left", padx=5)

        # --- Mantenimiento de BD ---
        maint_frame = tk.Frame(config_frame, bg="#f8f9fa")
        maint_frame.grid(row=1, column=0, columnspan=7, sticky="ew", padx=1, pady=(0,1))
        
        lbl_maint = tk.Label(maint_frame, text="Zona de Mantenimiento:", bg="#f8f9fa", fg="#666", font=("Segoe UI", 8))
        lbl_maint.pack(side="left", padx=20, pady=5)
        
        btn_delete_db = ttk.Button(maint_frame, text="⚠️ Eliminar Base de Datos Local", 
                                   command=self.borrar_base_datos, style="Danger.TButton")
        btn_delete_db.pack(side="right", padx=20, pady=5)

        # 3. TARJETAS PRINCIPALES
        self.ESPACIO_RESERVADO = "\n\n"

        # 1. ETL
        btn_etl = ttk.Button(self, text="Gestión de Datos (ETL)", command=lambda: self.validar_y_navegar(controller.show_etl_view), style="Card.TButton")
        btn_etl.grid(row=3, column=0, padx=30, pady=(20, 10), ipady=15, sticky="ew")

        self.lbl_etl_desc = ttk.Label(self, text=self.ESPACIO_RESERVADO, style="Desc.TLabel", justify="center")
        self.lbl_etl_desc.grid(row=4, column=0, padx=30, sticky="n")

        # 2. ML
        btn_ml = ttk.Button(self, text="Análisis Predictivo", command=lambda: self.validar_y_navegar(controller.show_ml_view), style="Card.TButton")
        btn_ml.grid(row=3, column=1, padx=30, pady=(20, 10), ipady=15, sticky="ew")

        self.lbl_ml_desc = ttk.Label(self, text=self.ESPACIO_RESERVADO, style="Desc.TLabel", justify="center")
        self.lbl_ml_desc.grid(row=4, column=1, padx=30, sticky="n")

        # 3. Export
        btn_export = ttk.Button(self, text="Informes y Reportes", command=lambda: self.validar_y_navegar(controller.show_export_view), style="Card.TButton")
        btn_export.grid(row=3, column=2, padx=30, pady=(20, 10), ipady=15, sticky="ew")

        self.lbl_export_desc = ttk.Label(self, text=self.ESPACIO_RESERVADO, style="Desc.TLabel", justify="center")
        self.lbl_export_desc.grid(row=4, column=2, padx=30, sticky="n")

        # --- Tooltips/Descripciones Hover ---
        btn_etl.bind("<Enter>", lambda e: self.lbl_etl_desc.config(text="Procesamiento y limpieza de datos\nde origen Excel a Base de Datos."))
        btn_etl.bind("<Leave>", lambda e: self.lbl_etl_desc.config(text=self.ESPACIO_RESERVADO))
        
        btn_ml.bind("<Enter>", lambda e: self.lbl_ml_desc.config(text="Modelos de Machine Learning para\nproyección de siniestralidad."))
        btn_ml.bind("<Leave>", lambda e: self.lbl_ml_desc.config(text=self.ESPACIO_RESERVADO))
        
        btn_export.bind("<Enter>", lambda e: self.lbl_export_desc.config(text="Captura automatizada desde Power BI."))
        btn_export.bind("<Leave>", lambda e: self.lbl_export_desc.config(text=self.ESPACIO_RESERVADO))


    # --- Métodos de Lógica ---
    def seleccionar_ruta(self):
        ruta = filedialog.askdirectory(title="Seleccionar Carpeta Principal")
        if ruta:
            self.ruta_var.set(ruta)
            self.controller.ruta_base = ruta
            guardar_ruta_base(ruta)
    
    def validar_y_navegar(self, funcion_navegar):
        ruta_base = getattr(self.controller, "ruta_base", "")
        if not ruta_base or not os.path.exists(ruta_base):
            messagebox.showerror("Configuración Requerida", "Por favor defina la 'Carpeta de Resultados' antes de continuar.")
            return
        funcion_navegar()

    def respaldar_archivos(self):
        ruta_base = getattr(self.controller, "ruta_base", "")
        if not ruta_base or not os.path.exists(ruta_base):
            messagebox.showerror("Error", "Carpeta principal no definida.")
            return
        
        try:
            rutas = {
                "Excel Brutos": obtener_ruta("ruta_excel_bruto"),
                "Excel Limpios": obtener_ruta("ruta_csv_limpio"),
                "Predicciones": obtener_ruta("ruta_predicciones"),
                "database": obtener_ruta("ruta_database")
            }
            fuentes_validas = {nombre: ruta for nombre, ruta in rutas.items() if ruta}
            if not fuentes_validas:
                messagebox.showerror("Error", "No hay datos procesados para respaldar.")
                return
            
            carpeta_backup, respaldadas = self.backup_manager.subir_a_onedrive_local(fuentes_validas)
            detalle = "\n".join([f"- {nombre}: {cantidad}" for nombre, cantidad in respaldadas.items()])
            msg = f"Sincronización con OneDrive exitosa.\n\nUbicación: {carpeta_backup}\n\nArchivos:\n{detalle}"
            messagebox.showinfo("Respaldo Cloud", msg)
        except Exception as e:
            messagebox.showerror("Error de Sincronización", f"Detalle del error:\n{e}")

    def recuperar_archivos(self):
        ruta_base = getattr(self.controller, "ruta_base", "")
        if not ruta_base:
            messagebox.showerror("Error", "Defina carpeta local primero.")
            return

        carpeta_backup = filedialog.askdirectory(
            initialdir=self.backup_manager.CARPETA_ONEDRIVE,
            title="Seleccionar versión en OneDrive"
        )
        if carpeta_backup:
            self.backup_manager.descargar_de_onedrive_local(os.path.basename(carpeta_backup), ruta_base)
            messagebox.showinfo("Restauración", "Datos recuperados exitosamente.")

    def cerrar_sesion(self):
        respuesta = messagebox.askyesno("Confirmación", "¿Desea cerrar la sesión actual?")
        if respuesta:
            self.controller.show_frame_by_name("VistaBienvenida")

    def borrar_base_datos(self):
        ruta_base = getattr(self.controller, "ruta_base", "")
        if not ruta_base:
            messagebox.showwarning("Atención", "No se ha definido la ruta base.")
            return

        # Obtenemos la ruta de la DB usando tu config_manager
        try:
            ruta_db = obtener_ruta("ruta_database")
            if not ruta_db:
                messagebox.showinfo("Información", "No se encontró configuración de base de datos.")
                return
                
            if not os.path.exists(ruta_db):
                messagebox.showinfo("Información", "No existe ninguna base de datos creada actualmente.")
                return

            # Confirmación de seguridad
            confirm = messagebox.askyesno(
                "ELIMINAR BASE DE DATOS", 
                "¿Estás seguro de que deseas eliminar la Base de Datos local?\n\n"
                "Esto borrará todos los datos procesados.Tendrás que ejecutar el proceso ETL nuevamente.",
                icon='warning'
            )
            
            if confirm:
                # Usamos nuestra utilidad que sabe borrar archivos ocultos
                exito, mensaje = remove_file_force(ruta_db)
                if exito:
                    messagebox.showinfo("Éxito", "Base de datos eliminada correctamente.\nEl sistema está limpio.")
                else:
                    messagebox.showerror("Error", f"No se pudo eliminar:\n{mensaje}")
                    
        except Exception as e:
            messagebox.showerror("Error Crítico", f"Falló el proceso de limpieza: {e}")