import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import os

class VistaExportar(ttk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller
        self.parent_container = parent

        self.export_path = None

        self.grid_rowconfigure(0, weight=0) # Título
        self.grid_rowconfigure(1, weight=1) # Botones de exportación
        self.grid_rowconfigure(2, weight=0) # Navegación
        self.grid_columnconfigure(0, weight=1)

        ttk.Label(self, text="Exportar Resultados", font=("Arial", 16, "bold")).grid(row=0, column=0, pady=15)

        export_buttons_frame = ttk.Frame(self)
        export_buttons_frame.grid(row=1, column=0, padx=50, pady=20, sticky="nsew")
        export_buttons_frame.grid_columnconfigure(0, weight=1) 

        ttk.Button(export_buttons_frame, text="Seleccionar carpeta de exportación", command=self.choose_folder).pack(pady=10)
        ttk.Button(export_buttons_frame, text="Exportar gráfico como imagen", command=self.export_plot).pack(pady=10)
        ttk.Button(export_buttons_frame, text="Exportar predicción como CSV", command=self.export_csv).pack(pady=10)
        
        nav = ttk.Frame(self)
        nav.grid(row=2, column=0, pady=20)
        ttk.Button(nav, text="Volver al Menú", command=lambda: self.controller.show_menu_principal()).pack(side="left", padx=10)
        ttk.Button(nav, text="Cerrar Asistente", command=self.controller.destroy).pack(side="right", padx=10)

    def choose_folder(self):
        folder = filedialog.askdirectory()
        if folder:
            self.parent.export_path = folder
            messagebox.showinfo("Ruta Designada", f"Las exportaciones se guardarán en:\n{folder}")

    def export_plot(self):
        if not self.parent.export_path:
            messagebox.showwarning("Ruta no seleccionada", "Primero seleccione una carpeta de exportación.")
            return
        
        # Ahora el gráfico se guarda directamente aquí
        fig_to_export = self.parent.predicted_plot_fig

        if fig_to_export:
            img_path = f"{self.parent.export_path}/grafico_prediccion.png"
            try:
                fig_to_export.savefig(img_path)
                messagebox.showinfo("Exportado", f"Gráfico exportado como imagen en:\n{img_path}")
            except Exception as e:
                messagebox.showerror("Error de Exportación", f"No se pudo exportar el gráfico: {e}")
        else:
            messagebox.showwarning("Advertencia", "No se ha generado ningún gráfico para exportar. Navegue a la página de resultados y genere la simulación.")

    def export_csv(self):
        if not self.parent.export_path:
            messagebox.showwarning("Ruta no seleccionada", "Primero seleccione una carpeta de exportación.")
            return

        # Ahora los datos se guardan directamente aquí
        df_to_export = self.parent.prediction_results_df

        if df_to_export is not None and not df_to_export.empty:
            csv_path = f"{self.parent.export_path}/prediccion_simulada.csv"
            try:
                df_to_export.to_csv(csv_path, index=False)
                messagebox.showinfo("Exportado", f"CSV con predicción simulada exportado en:\n{csv_path}")
            except Exception as e:
                messagebox.showerror("Error de Exportación", f"No se pudo exportar el CSV: {e}")
        else:
            messagebox.showwarning("Advertencia", "No hay datos de predicción simulados para exportar. Navegue a la página de resultados y genere la simulación.")