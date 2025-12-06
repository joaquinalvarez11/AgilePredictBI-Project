import tkinter as tk
from tkinter import ttk
from PIL import Image, ImageTk
import os

class VistaBienvenida(ttk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller 
        
        # --- Estilos ---
        style = ttk.Style()
        style.configure("White.TFrame", background="white")
        style.configure("Title.TLabel", background="white", foreground="#004c8c", font=("Helvetica", 20, "bold"))
        style.configure("Body.TLabel", background="white", foreground="#555555", font=("Helvetica", 11))
        style.configure("Action.TButton", font=("Helvetica", 11, "bold"))

        self.configure(style="White.TFrame")

        # --- Grid Config ---
        self.grid_rowconfigure(0, weight=1) 
        self.grid_rowconfigure(1, weight=2) 
        self.grid_rowconfigure(2, weight=0) # Barra decorativa inferior
        self.grid_columnconfigure(0, weight=1) 

        # --- Sección Logos ---
        logos_frame = tk.Frame(self, bg="white")
        logos_frame.grid(row=0, column=0, sticky="s", pady=(60, 0))
        
        logos_frame.grid_columnconfigure(0, weight=1)
        logos_frame.grid_columnconfigure(1, weight=0)
        logos_frame.grid_columnconfigure(2, weight=0)
        logos_frame.grid_columnconfigure(3, weight=0)
        logos_frame.grid_columnconfigure(4, weight=1)

        script_dir = os.path.dirname(__file__)
        assets_dir = os.path.join(script_dir, '..', 'assets')

        # Cargar Logo RDA
        try:
            left_img_path = os.path.join(assets_dir, "RDA_Logo.png")
            left_img = Image.open(left_img_path).resize((250, 100), Image.LANCZOS)
            self.rda_logo = ImageTk.PhotoImage(left_img)
            tk.Label(logos_frame, image=self.rda_logo, bg="white", bd=0).grid(row=0, column=1, padx=15)
        except FileNotFoundError:
            tk.Label(logos_frame, text="[RDA Logo]", bg="white", fg="gray").grid(row=0, column=1)

        # Línea vertical separadora sutil
        sep = tk.Frame(logos_frame, bg="#cccccc", width=2, height=60)
        sep.grid(row=0, column=2, padx=30)

        # Cargar Logo Predict
        try:
            right_img_path = os.path.join(assets_dir, "agilepredictbi_Logo.png")
            right_img = Image.open(right_img_path).resize((200, 100), Image.LANCZOS)
            self.predict_logo = ImageTk.PhotoImage(right_img)
            tk.Label(logos_frame, image=self.predict_logo, bg="white", bd=0).grid(row=0, column=3, padx=15)
        except FileNotFoundError:
            tk.Label(logos_frame, text="[Predict Logo]", bg="white", fg="gray").grid(row=0, column=3)

        # --- Sección TExto y Acción ---
        text_button_frame = ttk.Frame(self, style="White.TFrame")
        text_button_frame.grid(row=1, column=0, sticky="n", padx=40, pady=(30, 50))
        
        ttk.Label(text_button_frame, text="Bienvenido a AgilePredictBI", style="Title.TLabel").pack(pady=(0, 15))
        
        texto_desc = (
            "Su asistente ejecutivo para la transformación de datos y análisis predictivo.\n"
            "Gestionamos el cruce inteligente entre tráfico y siniestralidad para la toma de decisiones."
        )
        ttk.Label(text_button_frame, text=texto_desc, style="Body.TLabel", justify="center", wraplength=700).pack(pady=5)

        # Botón con un poco más de presencia (Padding interno)
        btn = ttk.Button(text_button_frame, text="Comenzar Sesión", style="Action.TButton", command=self.controller.show_login_view)
        btn.pack(pady=30, ipadx=20, ipady=5)

        # --- Barra Inferior ---
        # Una franja de color institucional al pie
        footer_strip = tk.Frame(self, bg="#00a2e8", height=10)
        footer_strip.grid(row=2, column=0, sticky="ew")
