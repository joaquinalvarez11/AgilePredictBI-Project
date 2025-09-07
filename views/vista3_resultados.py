import tkinter as tk
from tkinter import ttk, messagebox
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression # Importamos para una simulación más cercana a ML

class VistaResultados(ttk.Frame):
    def __init__(self, parent):
        super().__init__(parent)
        self.parent = parent
        self.fig = None

        self.grid_rowconfigure(0, weight=0) # Título
        self.grid_rowconfigure(1, weight=1) # Contenido principal de gráfico y predicción
        self.grid_rowconfigure(2, weight=0) # Navegación
        self.grid_columnconfigure(0, weight=1)

        ttk.Label(self, text="Resultados del Análisis Predictivo", font=("Arial", 16, "bold")).grid(row=0, column=0, pady=15)

        self.results_content_frame = ttk.Frame(self)
        self.results_content_frame.grid(row=1, column=0, padx=10, pady=10, sticky="nsew")
        self.results_content_frame.grid_rowconfigure(0, weight=1)
        self.results_content_frame.grid_columnconfigure(0, weight=1)

        # Botón para generar el análisis/simulación
        self.analyze_button = ttk.Button(self.results_content_frame, text="Realizar Análisis Predictivo (Simulado)", command=self.perform_predictive_analysis)
        self.analyze_button.grid(row=0, column=0, pady=20) # Inicialmente en el centro

        nav = ttk.Frame(self)
        nav.grid(row=2, column=0, pady=20)
        ttk.Button(nav, text="Atrás", command=parent.prev_page).pack(side="left", padx=10)
        ttk.Button(nav, text="Siguiente", command=parent.next_page).pack(side="right", padx=10)

    def perform_predictive_analysis(self):
        # Limpiar el frame de contenido si ya hay algo
        for widget in self.results_content_frame.winfo_children():
            if widget != self.analyze_button: # No borrar el botón de análisis aún
                widget.destroy()

        # Quitar el botón después de la primera vez que se presiona
        if self.analyze_button.winfo_exists():
            self.analyze_button.destroy()

        traffic_df = self.parent.traffic_df
        accidents_df = self.parent.accidents_df

        if traffic_df is None or accidents_df is None:
            messagebox.showwarning("Datos Faltantes", "Por favor, cargue ambos CSV (Tráfico y Siniestralidad) en la página anterior.")
            self.parent.prev_page() # Volver a la página anterior
            return
        
        # Simulación de Unión y Preprocesamiento
        # Asumir que ambos tienen una columna 'Mes' o simplemente usar los índices para simplificar
        try:
            # Intentar cruzar por una columna común si existe (ej. 'Mes', 'Fecha', 'Periodo')
            # Esto es un placeholder, en un caso real se necesitaría una lógica de unión robusta y manejo de fechas.
            
            # Para este prototipo, vamos a usar los índices como si fueran periodos de tiempo
            # Y forzar que tengan la misma longitud para el ejemplo
            min_rows = min(len(traffic_df), len(accidents_df))
            
            # Simular que el tráfico influye en los accidentes
            # Seleccionar una columna numérica de cada DF para la simulación
            # Usar la primera columna numérica si no hay una obvia, o pedir al usuario que la elija
            traffic_col = None
            for col in traffic_df.columns:
                if pd.api.types.is_numeric_dtype(traffic_df[col]):
                    traffic_col = col
                    break
            
            accidents_col = None
            for col in accidents_df.columns:
                if pd.api.types.is_numeric_dtype(accidents_df[col]):
                    accidents_col = col
                    break

            if traffic_col is None or accidents_col is None:
                messagebox.showerror("Error de Datos", "Asegúrese de que ambos CSV contengan al menos una columna numérica para el análisis simulado.")
                return

            X = traffic_df[traffic_col].head(min_rows).values.reshape(-1, 1) # Variable independiente de Tráfico
            y = accidents_df[accidents_col].head(min_rows).values # Variable dependiente de Siniestralidad

            # Simulación de Modelo Predictivo de Regresión Lineal Simple
            model = LinearRegression()
            model.fit(X, y)

            # Generar Predicción
            # Para el prototipo, se predice la siniestralidad para valores futuros de tráfico.
            # Podemos simular que el tráfico aumentará ligeramente o se mantendrá estable.
            # Se toman los últimos 5 valores de tráfico y "extrapolamos"
            last_traffic_value = X[-1][0]
            future_traffic_values = np.array([last_traffic_value + 10 * i for i in range(1, 6)]).reshape(-1, 1)
            
            # Predicciones de siniestralidad para los valores de tráfico existentes y futuros
            predicted_accidents_existing = model.predict(X)
            predicted_accidents_future = model.predict(future_traffic_values)

            # Preparar datos para el gráfico y la exportación
            # Crear un DataFrame combinado para visualización y exportación
            combined_data = pd.DataFrame({
                'Tráfico_Observado': X.flatten(),
                'Siniestralidad_Observada': y.flatten(),
                'Siniestralidad_Estimada': predicted_accidents_existing.flatten()
            })
            
            future_data = pd.DataFrame({
                'Tráfico_Predicho': future_traffic_values.flatten(),
                'Siniestralidad_Predicha': predicted_accidents_future.flatten()
            })

            # Almacenar los resultados combinados en self.parent.prediction_results_df
            # Esto es lo que se exportará como CSV
            self.parent.prediction_results_df = pd.concat([combined_data, future_data], axis=1)
            messagebox.showinfo("Análisis Completado", "Análisis predictivo simulado realizado exitosamente.")

            # Generar Gráfico
            self.fig, ax = plt.subplots(figsize=(8, 5))
            ax.scatter(X, y, color='blue', label='Datos Observados (Tráfico vs Siniestralidad)')
            ax.plot(X, predicted_accidents_existing, color='red', label='Línea de Tendencia (Regresión Simulada)')
            ax.scatter(future_traffic_values, predicted_accidents_future, color='green', marker='X', s=100, label='Predicciones Futuras')

            ax.set_title("Tráfico vs Siniestralidad con Predicción Simulada")
            ax.set_xlabel(f"Tráfico Mensual ({traffic_col})")
            ax.set_ylabel(f"Siniestralidad ({accidents_col})")
            ax.legend()
            ax.grid(True)

            # Almacenar la figura para exportación
            self.parent.predicted_plot_fig = self.fig

            canvas = FigureCanvasTkAgg(self.fig, master=self.results_content_frame)
            canvas_widget = canvas.get_tk_widget()
            canvas_widget.grid(row=0, column=0, sticky="nsew", padx=5, pady=5) # El gráfico ahora toma la posición 0,0

            canvas.draw()

        except Exception as e:
            messagebox.showerror("Error en Análisis", f"Ocurrió un error durante el análisis simulado:\n{e}\n\nAsegúrese de que los CSVs contengan columnas numéricas relevantes para 'tráfico' y 'siniestralidad'.")
            # Volver a mostrar el botón si hay un error
            self.analyze_button = ttk.Button(self.results_content_frame, text="Realizar Análisis Predictivo (Simulado)", command=self.perform_predictive_analysis)
            self.analyze_button.grid(row=0, column=0, pady=20)


    def get_plot_figure(self):
        # Ahora devuelve la figura almacenada
        return self.parent.predicted_plot_fig