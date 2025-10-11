import tkinter as tk
from tkinter import ttk
from PIL import Image, ImageTk
import os

class VistaBienvenida(ttk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller 

        # Configurar la cuadrícula del frame principal para que el contenido se centre y expanda
        self.grid_rowconfigure(0, weight=1) # Fila superior para los logos
        self.grid_rowconfigure(1, weight=2) # Fila inferior para el texto y el botón
        self.grid_columnconfigure(0, weight=1) # Columna central para todo

        # Frame para los Logos en la parte superior
        logos_frame = ttk.Frame(self)
        logos_frame.grid(row=0, column=0, sticky="s", pady=(50, 0)) # Sticky="s" para pegarlo a la parte inferior de su celda
        
        # Configurar las columnas de logos_frame para que los logos se centren
        logos_frame.grid_columnconfigure(0, weight=1) # Espaciador izquierdo
        logos_frame.grid_columnconfigure(1, weight=0) # Logo RDA (no expandir)
        logos_frame.grid_columnconfigure(2, weight=0) # Espacio entre logos
        logos_frame.grid_columnconfigure(3, weight=0) # Logo Predict (no expandir)
        logos_frame.grid_columnconfigure(4, weight=1) # Espaciador derecho

        script_dir = os.path.dirname(__file__)
        assets_dir = os.path.join(script_dir, '..', 'assets')

        # Cargar y posicionar logo RDA
        try:
            left_img_path = os.path.join(assets_dir, "RDA_Logo.png")
            left_img = Image.open(left_img_path).resize((220, 100), Image.LANCZOS) # Ajustar tamaño para que quepan juntos
            self.rda_logo = ImageTk.PhotoImage(left_img)
            ttk.Label(logos_frame, image=self.rda_logo).grid(row=0, column=1, padx=10, pady=10) # Columna 1
        except FileNotFoundError:
            ttk.Label(logos_frame, text="[RDA Logo Placeholder]\n(No encontrado)").grid(row=0, column=1, padx=10, pady=10)
            print(f"RDA_Logo.png no encontrado en {left_img_path}")

        # Separador pequeño entre logos (opcional, para visualización)
        # Puedes ajustar el padx para controlar el espacio entre ellos
        ttk.Frame(logos_frame, width=30).grid(row=0, column=2) 

        # Cargar y posicionar logo Predict
        try:
            right_img_path = os.path.join(assets_dir, "agilepredictbi_Logo.png")
            right_img = Image.open(right_img_path).resize((160, 100), Image.LANCZOS) # Mismo tamaño para ambos
            self.predict_logo = ImageTk.PhotoImage(right_img)
            ttk.Label(logos_frame, image=self.predict_logo).grid(row=0, column=3, padx=10, pady=10) # Columna 3
        except FileNotFoundError:
            ttk.Label(logos_frame, text="[Predict Logo Placeholder]\n(No encontrado)").grid(row=0, column=3, padx=10, pady=10)
            print(f"agilepredictbi_Logo.png no encontrado en {right_img_path}")

        # Frame para el Texto y Botón en la parte inferior
        text_button_frame = ttk.Frame(self)
        text_button_frame.grid(row=1, column=0, sticky="n", padx=30, pady=(20, 50)) # Sticky="n" para pegarlo arriba de su celda
        
        # Contenido centrado
        text_button_frame.grid_columnconfigure(0, weight=1)

        ttk.Label(text_button_frame, text="Bienvenido al Asistente Predictivo", font=("Arial", 18, "bold")).pack(pady=10)
        ttk.Label(text_button_frame,
                  text="Este asistente le ayudará a simular un análisis predictivo cruzando datos de tráfico y siniestralidad. Haga clic en Siguiente para comenzar.",
                  wraplength=600, justify="center").pack(pady=10) # Línea más ancha

        # Botón Siguiente
        ttk.Button(text_button_frame, text="Siguiente", command=self.controller.show_menu_principal).pack(pady=20)
