import tkinter as tk
from tkinter import ttk, messagebox
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from proceso_ml.ml_regresion_lineal import MLRegressionLineal
import traceback

class VistaML(ttk.Frame):
	def __init__(self, parent, controller):
		super().__init__(parent)
		self.parent = parent
		self.controller = controller
		self.modelo = MLRegressionLineal()

		self.grid_rowconfigure(0, weight=0)
		self.grid_rowconfigure(1, weight=1)
		self.grid_rowconfigure(2, weight=0)
		self.grid_columnconfigure(0, weight=1)

		ttk.Label(self, text="Resultados del Análisis Predictivo", font=("Arial", 16, "bold")).grid(row=0, column=0, pady=15)

		self.results_content_frame = ttk.Frame(self)
		self.results_content_frame.grid(row=1, column=0, padx=10, pady=10, sticky="nsew")
		self.results_content_frame.grid_rowconfigure(0, weight=1)
		self.results_content_frame.grid_columnconfigure(0, weight=1)

		# Botón para la ejecución del análisis predictivo
		self.analyze_button = ttk.Button(self.results_content_frame, text="Realizar Predicción", command=self.ejecutar_analisis)
		self.analyze_button.grid(row=0, column=0, pady=20)

		# Botón para la exportación (luego de la predicción)
		self.export_button = ttk.Button(self.results_content_frame, text="Exportar Resultado", command=self.exportar_resultado)
		self.export_button.grid(row=1, column=0, pady=10)
		self.export_button["state"] = "disabled"

		# Navegación
		nav = ttk.Frame(self)
		nav.grid(row=2, column=0, pady=20)
		ttk.Button(nav, text="Volver al Menú", command=lambda: controller.show_menu_principal()).pack(side="left", padx=10)
	
	def ejecutar_analisis(self):
		try:
			# Ejecutar análisis completo
			self.modelo.realizar_prediccion()

			# Mostrar gráfico en la interfaz
			fig = self.modelo.fig

			if fig:
				canvas = FigureCanvasTkAgg(fig, master=self.results_content_frame)
				canvas_widget = canvas.get_tk_widget()
				canvas_widget.grid(row=2, column=0, sticky="nsew", padx=5, pady=5)
				canvas.draw()
			
			# Activar botón de exportar
			self.export_button["state"] = "normal"
			messagebox.showinfo("Análisis Completado", "Predicción realizada exitosamente.")
		except Exception as e:
			traceback.print_exc()
			messagebox.showerror("Erorr en Análisis", f"Ocurrió un error durante el análisis:\n{e}")
	
	def exportar_resultado(self):
		try:
			self.modelo.exportar_csv_resultado()
			messagebox.showinfo("Exportación Exitosa", "El resultado fue exportado correctamente.")
		except Exception as e:
			messagebox.showerror("Error en Exportación", f"No se pudo exportar el resultado:\n{e}")
