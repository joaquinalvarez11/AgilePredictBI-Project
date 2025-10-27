import tkinter as tk
from tkinter import ttk

class VistaMenuPrincipal(tk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller

        # Configuración del Grid para centrar los bloques
        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure((0, 1, 2), weight=1)

        # Título
        lbl_title = ttk.Label(self, text="Menú Principal", font=("Arial", 24, "bold"))
        lbl_title.grid(row=0, column=0, columnspan=3, pady=(40, 60), sticky="n")

        # Bloque 1: Transformación ETL
        btn_etl = ttk.Button(
            self, 
            text="Transformación ETL", 
            command=lambda: controller.show_etl_view(), # Llama al método del controlador
            width=30,
            style="Menu.TButton" # Estilo personalizado opcional
        )
        btn_etl.grid(row=0, column=0, padx=20, pady=20)
        
        # Bloque 2: Análisis Predictivo
        btn_ml = ttk.Button(
            self, 
            text="Análisis Predictivo", 
            command=lambda: controller.show_ml_view(),
            width=30,
            style="Menu.TButton"
        )
        btn_ml.grid(row=0, column=1, padx=20, pady=20)
        
        # Bloque 3: Exportación de Informes
        btn_export = ttk.Button(
            self, 
            text="Exportación de Informes", 
            command=lambda: controller.show_export_view(),
            width=30,
            style="Menu.TButton"
        )
        btn_export.grid(row=0, column=2, padx=20, pady=20)

        # Estilo opcional para que los botones parezcan más "bloques"
        style = ttk.Style(self)
        style.configure("Menu.TButton", font=("Arial", 14), padding=40)