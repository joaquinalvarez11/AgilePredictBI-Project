import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import pandas as pd
import numpy as np

class VistaCargarCSV(ttk.Frame):
    def __init__(self, parent):
        super().__init__(parent)
        self.parent = parent
        
        self.grid_rowconfigure(0, weight=0)
        self.grid_rowconfigure(1, weight=1)
        self.grid_rowconfigure(2, weight=0)
        self.grid_columnconfigure(0, weight=1)

        ttk.Label(self, text="Carga de Archivos CSV", font=("Arial", 16, "bold")).grid(row=0, column=0, pady=15)

        self.csv_loading_frame = ttk.Frame(self)
        self.csv_loading_frame.grid(row=1, column=0, sticky="nsew", padx=10, pady=10)
        self.csv_loading_frame.grid_columnconfigure(0, weight=1)
        self.csv_loading_frame.grid_columnconfigure(1, weight=1)
        self.csv_loading_frame.grid_rowconfigure(0, weight=1)

        # Panel para Tráfico Mensual
        self.traffic_panel = ttk.LabelFrame(self.csv_loading_frame, text="Tráfico Mensual")
        self.traffic_panel.grid(row=0, column=0, sticky="nsew", padx=5, pady=5)
        self.traffic_panel.grid_rowconfigure(0, weight=0)
        self.traffic_panel.grid_rowconfigure(1, weight=1)
        self.traffic_panel.grid_columnconfigure(0, weight=1)

        ttk.Button(self.traffic_panel, text="Seleccionar CSV Tráfico", command=lambda: self.load_csv('traffic')).grid(row=0, column=0, pady=5)
        
        self.traffic_tree_frame = ttk.Frame(self.traffic_panel)
        self.traffic_tree_frame.grid(row=1, column=0, sticky="nsew", padx=5, pady=5)
        self.traffic_tree_frame.grid_rowconfigure(0, weight=1)
        self.traffic_tree_frame.grid_columnconfigure(0, weight=1)

        self.traffic_scrollbar_y = ttk.Scrollbar(self.traffic_tree_frame, orient="vertical")
        self.traffic_scrollbar_y.grid(row=0, column=1, sticky="ns")
        self.traffic_scrollbar_x = ttk.Scrollbar(self.traffic_tree_frame, orient="horizontal")
        self.traffic_scrollbar_x.grid(row=1, column=0, sticky="ew")

        self.traffic_tree = ttk.Treeview(self.traffic_tree_frame, columns=[], show="headings",
                                         yscrollcommand=self.traffic_scrollbar_y.set,
                                         xscrollcommand=self.traffic_scrollbar_x.set)
        self.traffic_tree.grid(row=0, column=0, sticky="nsew")
        self.traffic_scrollbar_y.config(command=self.traffic_tree.yview)
        self.traffic_scrollbar_x.config(command=self.traffic_tree.xview)


        # Panel para Siniestralidad
        self.accidents_panel = ttk.LabelFrame(self.csv_loading_frame, text="Siniestralidad")
        self.accidents_panel.grid(row=0, column=1, sticky="nsew", padx=5, pady=5)
        self.accidents_panel.grid_rowconfigure(0, weight=0)
        self.accidents_panel.grid_rowconfigure(1, weight=1)
        self.accidents_panel.grid_columnconfigure(0, weight=1)

        ttk.Button(self.accidents_panel, text="Seleccionar CSV Siniestralidad", command=lambda: self.load_csv('accidents')).grid(row=0, column=0, pady=5)

        self.accidents_tree_frame = ttk.Frame(self.accidents_panel)
        self.accidents_tree_frame.grid(row=1, column=0, sticky="nsew", padx=5, pady=5)
        self.accidents_tree_frame.grid_rowconfigure(0, weight=1)
        self.accidents_tree_frame.grid_columnconfigure(0, weight=1)
        
        self.accidents_scrollbar_y = ttk.Scrollbar(self.accidents_tree_frame, orient="vertical")
        self.accidents_scrollbar_y.grid(row=0, column=1, sticky="ns")
        self.accidents_scrollbar_x = ttk.Scrollbar(self.accidents_tree_frame, orient="horizontal")
        self.accidents_scrollbar_x.grid(row=1, column=0, sticky="ew")

        self.accidents_tree = ttk.Treeview(self.accidents_tree_frame, columns=[], show="headings",
                                           yscrollcommand=self.accidents_scrollbar_y.set,
                                           xscrollcommand=self.accidents_scrollbar_x.set)
        self.accidents_tree.grid(row=0, column=0, sticky="nsew")
        self.accidents_scrollbar_y.config(command=self.accidents_tree.yview)
        self.accidents_scrollbar_x.config(command=self.accidents_tree.xview)

        nav = ttk.Frame(self)
        nav.grid(row=2, column=0, pady=20)
        ttk.Button(nav, text="Atrás", command=parent.prev_page).pack(side="left", padx=10)
        ttk.Button(nav, text="Siguiente", command=self.go_to_next_page).pack(side="right", padx=10)

    def load_csv(self, type_data):
        file_path = filedialog.askopenfilename(filetypes=[("Archivos CSV", "*.csv")])
        if file_path:
            try:
                df = None
                encodings_to_try = ['utf-8', 'latin1', 'cp1252']
                delimiters_to_try = [',', ';', '\t'] 

                for encoding in encodings_to_try:
                    for delimiter in delimiters_to_try:
                        try:
                            temp_df = pd.read_csv(file_path, encoding=encoding, sep=delimiter, header=0)
                            
                            if temp_df.shape[1] > 1 and not temp_df.columns.str.contains('Unnamed:').all():
                                df = temp_df
                                print(f"DEBUG: CSV leído con éxito. Codificación: {encoding}, Delimitador: '{delimiter}'")
                                print("DEBUG: Primeras 5 filas del DataFrame original:\n", df.head())
                                print("DEBUG: Tipos de datos del DataFrame original:\n", df.dtypes)
                                break 
                            else:
                                temp_df = pd.read_csv(file_path, encoding=encoding, sep=delimiter, header=None)
                                if temp_df.shape[1] > 1 and not temp_df.columns.str.contains('Unnamed:').all():
                                    df = temp_df.rename(columns=temp_df.iloc[0]).drop(temp_df.index[0])
                                    print(f"DEBUG: CSV leído con éxito (sin encabezado). Codificación: {encoding}, Delimitador: '{delimiter}'")
                                    print("DEBUG: Primeras 5 filas del DataFrame (encabezado inferido):\n", df.head())
                                    print("DEBUG: Tipos de datos del DataFrame (encabezado inferido):\n", df.dtypes)
                                    break 
                                else:
                                    df = None
                        except (UnicodeDecodeError, pd.errors.ParserError) as e:
                            continue 
                    if df is not None: 
                        break
                
                if df is None:
                    raise ValueError("No se pudo decodificar o separar el archivo CSV con las codificaciones y delimitadores intentados.")

                # Proceso de Limpieza y Conversión a Numérico
                processed_df = df.copy() 

                print("\nDEBUG: Iniciando preprocesamiento de columnas...")
                for col in processed_df.columns:
                    # Si Pandas ya lo reconoció como numérico (int, float), dejarlo como tal
                    if pd.api.types.is_numeric_dtype(processed_df[col]):
                        print(f"DEBUG: Columna '{col}' ya es numérica (Pandas lo detectó). Manteniendo tipo.")
                        # Asegurarse de rellenar NaN si existen, aunque ya sea numérico
                        if processed_df[col].isnull().any():
                            mean_val = processed_df[col].mean()
                            processed_df[col] = processed_df[col].fillna(mean_val if pd.notna(mean_val) else 0)
                        continue # No hacer más procesamiento para esta columna
                    
                    # Para columnas que Pandas NO reconoció como numéricas, intentar convertirlas
                    cleaned_series = processed_df[col].astype(str).str.replace(',', '.', regex=False).str.strip()
                    temp_series = pd.to_numeric(cleaned_series, errors='coerce')
                    
                    # Identificar columnas que NO deben ser numéricas para el análisis
                    # Ajustar las palabras clave para ser más específicas y evitar falsos positivos
                    is_non_numeric_for_analysis_keywords = ['fecha', 'mes', 'periodo', 'id', 'nombre', 'descripcion', 'tipo', 'categoría']
                    is_non_numeric_for_analysis = any(keyword in str(col).lower() for keyword in is_non_numeric_for_analysis_keywords)
                    
                    # Condición para convertir a numérico:
                    # 1. La columna no debe ser de tipo texto/fecha/ID obvio.
                    # 2. Al menos el 50% de sus valores deben ser numéricos.
                    # 3. Debe tener al menos 2 valores numéricos válidos.
                    if (not is_non_numeric_for_analysis and 
                        temp_series.count() / len(temp_series) > 0.5 and 
                        temp_series.count() >= 2):
                        
                        processed_df[col] = temp_series
                        if processed_df[col].isnull().any():
                            mean_val = processed_df[col].mean()
                            processed_df[col] = processed_df[col].fillna(mean_val if pd.notna(mean_val) else 0)
                        print(f"DEBUG: Columna '{col}' convertida a numérica por preprocesamiento.")
                    else:
                        processed_df[col] = processed_df[col].astype(str)
                        print(f"DEBUG: Columna '{col}' mantenida como string, no numérica para análisis o es fecha/ID.")

                print("DEBUG: Tipos de datos del DataFrame PROCESADO:\n", processed_df.dtypes)

                if type_data == 'traffic':
                    self.parent.traffic_df = processed_df
                    self.display_data(self.traffic_tree, processed_df)
                    messagebox.showinfo("CSV Cargado", "CSV de Tráfico Mensual cargado y preprocesado exitosamente.")
                elif type_data == 'accidents':
                    self.parent.accidents_df = processed_df
                    self.display_data(self.accidents_tree, processed_df)
                    messagebox.showinfo("CSV Cargado", "CSV de Siniestralidad cargado y preprocesado exitosamente.")
            except Exception as e:
                messagebox.showerror("Error de Carga/Preprocesamiento", f"No se pudo cargar o preprocesar el archivo CSV:\n{e}")

    def display_data(self, tree_widget, df):
        tree_widget.delete(*tree_widget.get_children())
        tree_widget["columns"] = list(df.columns)
        for col in df.columns:
            tree_widget.heading(col, text=col, anchor="w")
            max_len_col = max(df[col].astype(str).apply(len).max(), len(col)) if not df.empty else len(col)
            tree_widget.column(col, width=max(100, int(max_len_col * 8)), stretch=True, anchor="w")
        for _, row in df.iterrows():
            tree_widget.insert("", "end", values=[str(val) for val in row])

    def go_to_next_page(self):
        if self.parent.traffic_df is None or self.parent.accidents_df is None:
            messagebox.showwarning("Advertencia", "Por favor, cargue ambos CSV (Tráfico y Siniestralidad) para continuar.")
            return
        
        traffic_df_numeric_cols = [col for col in self.parent.traffic_df.columns if pd.api.types.is_numeric_dtype(self.parent.traffic_df[col])]
        accidents_df_numeric_cols = [col for col in self.parent.accidents_df.columns if pd.api.types.is_numeric_dtype(self.parent.accidents_df[col])]

        print(f"DEBUG: Columnas numéricas detectadas en Tráfico (antes de validar): {traffic_df_numeric_cols}")
        print(f"DEBUG: Columnas numéricas detectadas en Siniestralidad (antes de validar): {accidents_df_numeric_cols}")

        if not traffic_df_numeric_cols:
            messagebox.showwarning("Advertencia", "El CSV de Tráfico Mensual no contiene columnas numéricas válidas después del preprocesamiento. Revise sus datos.")
            return
        if not accidents_df_numeric_cols:
            messagebox.showwarning("Advertencia", "El CSV de Siniestralidad no contiene columnas numéricas válidas después del preprocesamiento. Revise sus datos.")
            return

        self.parent.next_page()