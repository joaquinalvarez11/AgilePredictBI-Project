import tkinter as tk
from tkinter import ttk

class VistaMenuPrincipal(tk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller

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

        # --- Contenido ---
        self.ESPACIO_RESERVADO = "\n\n"

        # Definimos los botones (ETL, ML, Export)
        # Uso de ipady/ipadx para que se sientan como "tarjetas"
        
        # 1. ETL
        btn_etl = ttk.Button(self, text="Gestión de Datos (ETL)", command=lambda: controller.show_etl_view(), style="Card.TButton", width=25)
        btn_etl.grid(row=2, column=0, padx=20, pady=(50, 15), ipady=10)

        self.lbl_etl_desc = ttk.Label(self, text=self.ESPACIO_RESERVADO, style="Desc.TLabel", justify="center", wraplength=250)
        self.lbl_etl_desc.grid(row=3, column=0, padx=20, sticky="n")

        # 2. ML
        btn_ml = ttk.Button(self, text="Análisis Predictivo", command=lambda: controller.show_ml_view(), style="Card.TButton", width=25)
        btn_ml.grid(row=2, column=1, padx=20, pady=(50, 15), ipady=10)

        self.lbl_ml_desc = ttk.Label(self, text=self.ESPACIO_RESERVADO, style="Desc.TLabel", justify="center", wraplength=250)
        self.lbl_ml_desc.grid(row=3, column=1, padx=20, sticky="n")

        # 3. Export
        btn_export = ttk.Button(self, text="Exportación de Informes", command=lambda: controller.show_export_view(), style="Card.TButton", width=25)
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