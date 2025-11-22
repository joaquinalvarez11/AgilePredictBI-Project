import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from proceso_ml.ml_regresion_lineal import MLRegressionLineal
import traceback
import threading
import os

class VistaResultados(ttk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.parent = parent
        self.controller = controller
        self.modelo = MLRegressionLineal()
        self.ml_thread = None
        self.canvas_widget = None

        # --- Estilos ---
        style = ttk.Style()
        style.configure("White.TFrame", background="white")
        self.configure(style="White.TFrame")
        style.configure("MLInfo.TLabel", background="white", foreground="#555555", font=("Helvetica", 10))

        # --- Grid Config ---
        self.grid_rowconfigure(0, weight=0) # Header
        self.grid_rowconfigure(1, weight=0) # Info
        self.grid_rowconfigure(2, weight=1) # Resultados
        self.grid_rowconfigure(3, weight=0) # Botones
        self.grid_rowconfigure(4, weight=0) # Navegación
        self.grid_columnconfigure(0, weight=1)

        # 1. Header
        header_frame = tk.Frame(self, bg="#f0f4f8", height=60)
        header_frame.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        tk.Label(header_frame, text="Motor de Análisis Predictivo", 
                 font=("Helvetica", 16, "bold"), bg="#f0f4f8", fg="#004c8c").pack(pady=15)

        # 2. Info
        frame_info = tk.Frame(self, bg="white")
        frame_info.grid(row=1, column=0, sticky="ew", padx=20)

        info_text = (
            "Este módulo ejecuta el motor de Machine Learning sobre la Base de Datos.\n"
            "Generará los datos históricos y las proyecciones futuras para su consumo en Power BI."
        )
        lbl_info = ttk.Label(frame_info, text=info_text, justify="center", style="MLInfo.TLabel")
        lbl_info.pack(pady=(0, 10))

        # 3. Área de Resultados
        self.frame_resultados = tk.Frame(self, bg="white")
        self.frame_resultados.grid(row=2, column=0, padx=40, pady=5, sticky="nsew")
        self.frame_resultados.grid_rowconfigure(0, weight=1)
        self.frame_resultados.grid_columnconfigure(0, weight=1)

        # Frame de Logs
        self.frame_logs = tk.Frame(self.frame_resultados, bg="white")
        self.frame_logs.grid(row=0, column=0, sticky="nsew")
        
        self.lbl_progreso = tk.Label(self.frame_logs, text="Estado: Sistema en espera.", 
                                     font=("Helvetica", 10, "bold"), bg="white", fg="#004c8c")
        self.lbl_progreso.pack(anchor="w", pady=(0, 5))

        frame_consola_borde = tk.Frame(self.frame_logs, bg="#cccccc", bd=1)
        frame_consola_borde.pack(fill="both", expand=True)
        
        self.txt_status = scrolledtext.ScrolledText(
            frame_consola_borde, wrap=tk.WORD, height=10, state='disabled', 
            font=("Consolas", 9), bg="#f9f9f9", fg="#333333", relief="flat"
        )
        self.txt_status.pack(fill="both", expand=True, padx=1, pady=1)

        # 4. Botones de Acción
        frame_acciones = tk.Frame(self, bg="white")
        frame_acciones.grid(row=3, column=0, pady=15)

        self.btn_prediccion = tk.Button(
            frame_acciones, text="▶ Ejecutar Motor ML", 
            bg="#004c8c", fg="white", font=("Helvetica", 10, "bold"),
            relief="flat", padx=20, pady=8, cursor="hand2",
            command=self.iniciar_proceso_en_thread
        )
        self.btn_prediccion.pack(side=tk.LEFT, padx=10)

        # Botón para abrir la carpeta donde se guardan los archivos para Power BI
        self.btn_abrir_carpeta = tk.Button(
            frame_acciones, text="📂 Abrir Carpeta Resultados", 
            bg="#00a2e8", fg="white", font=("Helvetica", 10, "bold"),
            relief="flat", padx=15, pady=8, cursor="hand2",
            state="disabled", command=self.abrir_carpeta_resultados
        )
        self.btn_abrir_carpeta.pack(side=tk.LEFT, padx=10)
        
        # Botón Toggle Gráfico (Opcional, para vista rápida)
        self.btn_toggle_vista = tk.Button(
            frame_acciones, text="👁 Vista Previa Gráfica", 
            bg="gray", fg="white", font=("Helvetica", 10),
            relief="flat", padx=15, pady=8, cursor="hand2",
            state="disabled", command=self.__alternar_vista
        )
        self.btn_toggle_vista.pack(side=tk.LEFT, padx=10)

        # 5. Navegación
        frame_nav = tk.Frame(self, bg="white")
        frame_nav.grid(row=4, column=0, pady=(0, 30))
        ttk.Button(frame_nav, text="Volver al Menú", command=lambda: controller.show_menu_principal()).pack()

    # --- Lógica ---

    def iniciar_proceso_en_thread(self):
        if self.canvas_widget:
            self.canvas_widget.destroy()
            self.canvas_widget = None
        
        self.__resetear_vista()
        self.btn_prediccion.config(state="disabled", text="Procesando...", bg="#cccccc")
        self.frame_logs.grid()

        self.ml_thread = threading.Thread(target=self.ejecutar_analisis)
        self.ml_thread.start()
    
    def ejecutar_analisis(self):
        try:
            self.progreso_callback("Conectando a Base de Datos...", 5)
            # Sin filtros, procesamos todo para generar el dataset maestro
            self.modelo.realizar_prediccion(callback=self.progreso_callback)
            
            self.progreso_callback("Renderizando vista previa...", 90)
            fig = self.modelo.fig

            if fig:
                self.after(0, lambda: self.__crear_y_mostrar_grafico(fig))
            
            self.progreso_callback("Exportando datos para Power BI...", 95)
            self.progreso_callback("¡Proceso Completado Exitosamente!", 100)
            
            self.after(0, lambda: messagebox.showinfo("Éxito", "Datos generados para Power BI."))
            self.after(0, lambda: self.btn_abrir_carpeta.config(state="normal", bg="#00a2e8"))
            
        except Exception as e:
            traceback.print_exc()
            self.progreso_callback(f"Error crítico: {e}")
            self.after(0, lambda: messagebox.showerror("Error", str(e)))
        finally:
            self.after(0, lambda: self.btn_prediccion.config(state="normal", text="▶ Ejecutar Motor ML", bg="#004c8c"))

    def abrir_carpeta_resultados(self):
        try:
            # Asumiendo que el modelo guarda la ruta en self.modelo.ruta_salida
            # O usamos la configuración
            from config_manager import obtener_ruta
            ruta = obtener_ruta("ruta_predicciones")
            os.startfile(ruta)
        except Exception as e:
            messagebox.showerror("Error", f"No se pudo abrir la carpeta: {e}")

    # Funciones auxiliares estándar...
    def progreso_callback(self, mensaje, porcentaje=None):
        if not self.winfo_exists(): return
        self.after(0, self.__actualizar_gui_callback, mensaje, porcentaje)
    
    def __actualizar_gui_callback(self, mensaje, porcentaje=None):
        self.txt_status.config(state="normal")
        self.txt_status.insert(tk.END, mensaje + "\n")
        self.txt_status.see(tk.END)
        self.txt_status.config(state="disabled")
        if porcentaje is not None: self.lbl_progreso.config(text=f"Progreso: {porcentaje:.1f}%")
        self.update_idletasks()
    
    def __resetear_vista(self):
        self.lbl_progreso.config(text="Estado: Iniciando...")
        self.txt_status.config(state="normal")
        self.txt_status.delete("1.0", tk.END)
        self.txt_status.config(state="disabled")
        self.frame_logs.grid()
        self.btn_toggle_vista.config(state="disabled", bg="gray")
        self.btn_abrir_carpeta.config(state="disabled", bg="gray")

    def __alternar_vista(self):
        if self.canvas_widget is None or (self.ml_thread and self.ml_thread.is_alive()): return
        if self.canvas_widget and self.frame_logs.winfo_ismapped():
            self.frame_logs.grid_remove()
            self.canvas_widget.grid()
            self.btn_toggle_vista.config(text="📋 Ver Logs")
        elif self.canvas_widget:
            self.canvas_widget.grid_remove()
            self.frame_logs.grid()
            self.btn_toggle_vista.config(text="👁 Vista Previa")
    
    def __crear_y_mostrar_grafico(self, fig):
        def crear_canvas():
            if self.canvas_widget: self.canvas_widget.destroy()
            canvas = FigureCanvasTkAgg(fig, master=self.frame_resultados)
            canvas.draw()
            self.canvas_widget = canvas.get_tk_widget()
            self.canvas_widget.configure(bg="white")
            self.canvas_widget.grid(row=0, column=0, sticky="nsew")
            self.frame_logs.grid_remove()
            self.btn_toggle_vista.config(state="normal", text="📋 Ver Logs", bg="gray")
        self.after(0, crear_canvas)