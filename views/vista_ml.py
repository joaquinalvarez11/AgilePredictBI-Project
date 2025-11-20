import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from proceso_ml.ml_regresion_lineal import MLRegressionLineal
import traceback
import threading

class VistaML(ttk.Frame):
	def __init__(self, parent, controller):
		super().__init__(parent)
		self.parent = parent
		self.controller = controller
		self.modelo = MLRegressionLineal()
		self.ml_thread = None
		self.canvas_widget = None

		self.grid_rowconfigure(0, weight=1)
		self.grid_rowconfigure(1, weight=0)
		self.grid_rowconfigure(2, weight=0)
		self.grid_columnconfigure(0, weight=1)

		# Frame principal del contenido
		frame_contenido = ttk.Frame(self)
		frame_contenido.grid(row=0, column=0, sticky="nsew")
		frame_contenido.grid_rowconfigure(2, weight=1)
		frame_contenido.grid_columnconfigure(0, weight=1)

		lbl_title = ttk.Label(frame_contenido, text="Resultados del Análisis Predictivo", font=("Arial", 18, "bold"))
		lbl_title.grid(row=0, column=0, pady=(30, 5))

		info_text = (
			"Este análisis generará una predicción futura basada en los datos de\n"
			"tráfico y siniestralidad disponibles.\n"
			"Haga clic en Realizar Predicción (puede tardar)."
		)

		lbl_info = ttk.Label(frame_contenido, text=info_text, justify="center")
		lbl_info.grid(row=1, column=0, pady=(0, 20), padx=40)

		self.frame_resultados = ttk.Frame(frame_contenido)
		self.frame_resultados.grid(row=2, column=0, padx=10, pady=10, sticky="nsew")
		self.frame_resultados.grid_rowconfigure(2, weight=1)
		self.frame_resultados.grid_columnconfigure(0, weight=1)

		# Frame para botones
		frame_button = ttk.Frame(self)
		frame_button.grid(row=1, column=0, pady=(10, 5))

		# Botón para la ejecución del análisis predictivo
		self.btn_prediccion = ttk.Button(frame_button, text="Realizar Predicción", command=self.iniciar_proceso_en_thread)
		self.btn_prediccion.pack(side=tk.LEFT, padx=10, ipady=5, ipadx=10)

		# Botón para alternar los logs y el gráfico
		self.btn_toggle_vista = ttk.Button(frame_button, text="Ver Gráfico", command=self.__alternar_vista)
		self.btn_toggle_vista.pack(side=tk.LEFT, padx=10, ipady=5, ipadx=10)
		self.btn_toggle_vista.config(state="disabled")

		# Frame para progreso y logs
		self.frame_logs = ttk.Frame(self.frame_resultados)
		self.frame_logs.grid(row=1, column=0, sticky="nsew")

		# Label de progreso
		self.lbl_progreso = ttk.Label(self.frame_logs, text="Progreso: 0%", font=("Courier New", 10))
		self.lbl_progreso.grid(row=0, column=0, pady=(5, 5), sticky="ew")

		# Área de texto para logs
		self.txt_status = scrolledtext.ScrolledText(self.frame_logs, wrap=tk.WORD, height=12, state='disabled', font=("Courier New", 10))
		self.txt_status.grid(row=1, column=0, sticky="nsew", padx=5, pady=5)

		self.frame_logs.grid_rowconfigure(1, weight=1)
		self.frame_logs.grid_columnconfigure(0, weight=1)

		# Navegación
		nav = ttk.Frame(self)
		nav.grid(row=2, column=0, pady=(0, 20))

		# Botón para volver al menú
		btn_back = ttk.Button(nav, text="Volver al Menú", command=lambda: controller.show_menu_principal())
		btn_back.pack(side=tk.LEFT, padx=10, ipady=5, ipadx=10)

	def iniciar_proceso_en_thread(self):
		if self.canvas_widget:
			self.canvas_widget.destroy()
			self.canvas_widget = None
		
		self.__resetear_vista()
		self.btn_prediccion.config(state="disabled", text="Procesando...")

		# Mostrar los logs
		self.frame_logs.grid()

		self.ml_thread = threading.Thread(target=self.ejecutar_analisis)
		self.ml_thread.start()
	
	def progreso_callback(self, mensaje, porcentaje=None):
		if not self.winfo_exists():
			return
		self.after(0, self.__actualizar_gui_callback, mensaje, porcentaje)
	
	def __actualizar_gui_callback(self, mensaje, porcentaje=None):
		self.txt_status.config(state="normal")
		self.txt_status.insert(tk.END, mensaje + "\n")
		self.txt_status.see(tk.END)
		self.txt_status.config(state="disabled")

		if porcentaje is not None:
			self.lbl_progreso.config(text=f"Progreso: {porcentaje:.1f}%")
		
		self.update_idletasks()
	
	def __resetear_vista(self):
		self.lbl_progreso.config(text="Progreso: 0%")
		self.txt_status.config(state="normal")
		self.txt_status.delete("1.0", tk.END)
		self.txt_status.config(state="disabled")
		self.frame_logs.grid()
		self.btn_toggle_vista.config(state="disabled")

	def __alternar_vista(self):
		if self.canvas_widget is None or self.ml_thread and self.ml_thread.is_alive():
			return

		if self.canvas_widget and self.frame_logs.winfo_ismapped():
			self.frame_logs.grid_remove()
			self.canvas_widget.grid()
			self.btn_toggle_vista.config(text="Ver Logs")
		elif self.canvas_widget:
			self.canvas_widget.grid_remove()
			self.frame_logs.grid()
			self.btn_toggle_vista.config(text="Ver Gráfico")
	
	def __crear_y_mostrar_grafico(self, fig):
		def crear_canvas():
			if self.canvas_widget:
				self.canvas_widget.destroy()
			
			canvas = FigureCanvasTkAgg(fig, master=self.frame_resultados)
			canvas.draw()
			
			self.canvas_widget = canvas.get_tk_widget()
			self.canvas_widget.grid(row=2, column=0, sticky="nsew", padx=5, pady=5)

			self.frame_logs.grid_remove()
			self.btn_toggle_vista.config(state="normal", text="Ver Logs")
		
		self.after(0, crear_canvas)
	
	def ejecutar_analisis(self):
		try:
			# Ejecutar análisis completo
			self.progreso_callback("Iniciando análisis ML ...", 5)
			self.modelo.realizar_prediccion(callback=self.progreso_callback)
			self.progreso_callback("Generando gráfico ...", 90)

			# Mostrar gráfico en la interfaz
			fig = self.modelo.fig

			if fig:
				self.after(0, lambda: self.__crear_y_mostrar_grafico(fig))

			self.progreso_callback("Proceso completado.", 100)
			
			self.after(0, lambda: messagebox.showinfo("Análisis Completado", "Predicción realizada exitosamente."))
			
		except Exception as e:
			traceback.print_exc()
			self.progreso_callback(f"Error: {type(e).__name__}: {e}")
			self.after(0, lambda: messagebox.showerror("Error en Análisis", f"Ocurrió un error durante el análisis:\n{e}"))
		finally:
			self.after(0, lambda: self.btn_prediccion.config(state="normal", text="Realizar Predicción"))
