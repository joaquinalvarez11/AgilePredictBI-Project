import tkinter as tk
from tkinter import ttk

class VistaMenuPrincipal(tk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller

        # Fila 0: Espaciador superior (CON peso, empuja hacia abajo)
        # Fila 1: Título
        # Fila 2: Botones
        # Fila 3: Descripciones
        # Fila 4: Espaciador inferior (CON peso, empuja hacia arriba)
        self.grid_rowconfigure((0, 4), weight=1) # Filas 0 y 4 son espaciadores
        self.grid_rowconfigure((1, 2, 3), weight=0) # Filas de contenido no expanden
        self.grid_columnconfigure((0, 1, 2), weight=1) # Columnas centran horizontalmente

        # Título (Ahora en Fila 1)
        lbl_title = ttk.Label(self, text="Menú Principal", font=("Arial", 24, "bold"))
        # Aumentar el padding inferior (pady) para dar más espacio
        lbl_title.grid(row=1, column=0, columnspan=3, pady=(0, 100), sticky="n") # Fila 1

        # --- Constante para reservar espacio ---
        self.ESPACIO_RESERVADO = "\n\n"

        # --- Bloque 1: Transformación ETL (en Fila 2) ---
        btn_etl = ttk.Button(
            self, 
            text="Transformación ETL", 
            command=lambda: controller.show_etl_view(),
            width=30,
            style="Menu.TButton"
        )
        btn_etl.grid(row=2, column=0, padx=20, pady=20) # Fila 2
        
        # Etiqueta de descripción (en Fila 3)
        self.lbl_etl_desc = ttk.Label(
            self,
            text=self.ESPACIO_RESERVADO,
            font=("Arial", 10),
            justify="center",
            wraplength=250
        )
        self.lbl_etl_desc.grid(row=3, column=0, padx=20, pady=(0, 20), sticky="n") # Fila 3

        # --- Bloque 2: Análisis Predictivo (en Fila 2) ---
        btn_ml = ttk.Button(
            self, 
            text="Análisis Predictivo", 
            command=lambda: controller.show_ml_view(),
            width=30,
            style="Menu.TButton"
        )
        btn_ml.grid(row=2, column=1, padx=20, pady=20) # Fila 2

        # Etiqueta de descripción (en Fila 3)
        self.lbl_ml_desc = ttk.Label(
            self,
            text=self.ESPACIO_RESERVADO,
            font=("Arial", 10),
            justify="center",
            wraplength=250
        )
        self.lbl_ml_desc.grid(row=3, column=1, padx=20, pady=(0, 20), sticky="n") # Fila 3
        
        # --- Bloque 3: Exportación de Informes (en Fila 2) ---
        btn_export = ttk.Button(
            self, 
            text="Exportación de Informes", 
            command=lambda: controller.show_export_view(),
            width=30,
            style="Menu.TButton"
        )
        btn_export.grid(row=2, column=2, padx=20, pady=20) # Fila 2

        # Etiqueta de descripción (en Fila 3)
        self.lbl_export_desc = ttk.Label(
            self,
            text=self.ESPACIO_RESERVADO,
            font=("Arial", 10),
            justify="center",
            wraplength=250
        )
        self.lbl_export_desc.grid(row=3, column=2, padx=20, pady=(0, 20), sticky="n") # Fila 3

        # --- Vinculación de eventos ---
        btn_etl.bind("<Enter>", self.on_etl_enter)
        btn_etl.bind("<Leave>", self.on_etl_leave)
        btn_ml.bind("<Enter>", self.on_ml_enter)
        btn_ml.bind("<Leave>", self.on_ml_leave)
        btn_export.bind("<Enter>", self.on_export_enter)
        btn_export.bind("<Leave>", self.on_export_leave)
        
        # Estilo
        style = ttk.Style(self)
        style.configure("Menu.TButton", font=("Arial", 14), padding=40)

    # --- Funciones de Eventos ---
    def on_etl_enter(self, event):
        self.lbl_etl_desc.config(text="Procesa archivos Excel brutos (Tráfico, Siniestros, Vehículos) y los guarda como CSV limpios.")
    def on_etl_leave(self, event):
        self.lbl_etl_desc.config(text=self.ESPACIO_RESERVADO) 
    def on_ml_enter(self, event):
        self.lbl_ml_desc.config(text="Ejecuta un modelo predictivo para encontrar correlaciones y tendencias en los datos.")
    def on_ml_leave(self, event):
        self.lbl_ml_desc.config(text=self.ESPACIO_RESERVADO) 
    def on_export_enter(self, event):
        self.lbl_export_desc.config(text="Captura informes PDF desde Power BI.")
    def on_export_leave(self, event):
        self.lbl_export_desc.config(text=self.ESPACIO_RESERVADO)