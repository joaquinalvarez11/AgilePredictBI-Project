import os # TODO: usar funciones de gestion_archivos.py
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.metrics import root_mean_squared_error, mean_absolute_error, r2_score
from datetime import datetime
from config_manager import obtener_ruta

class MLRegressionLineal():
    def __init__(self):
        """Inicializa los DataFrames y el modelo"""
        self.df_siniestro = None
        self.df_vehiculos = None
        self.df_trafico = None
        self.df_modelo = None
        self.modelo = None
        self.fig = None
    
    def __log(self, msg):
        """Imprime un mensaje con marca de tiempo específico para esta clase."""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{now}] [ML Regresion Lineal] {msg}")

    # Método para la predicción
    def realizar_prediccion(self):
        """Punto de entrada para la predicción"""
        self.__log("Iniciando predicción...")
        self.__cargar_datos()
        self.__unir_fuentes()
        self.__preparar_dataset()
        self.__entrenar_modelo()
        self.__visualizar_resultados()

    # Método para la exportación
    def exportar_csv_resultado(self, nombre_base="resultado_prediccion"):
        """
        Exporta el DataFrame con las predicciones a la carpeta 'Predicciones'
        usando la ruta definida en config_manager.
        """
        if self.df_modelo is None or self.df_modelo.empty:
            self.__log("No hay datos para exportar.")
            return
        
        ruta_predicciones = obtener_ruta("ruta_predicciones")
        os.makedirs(ruta_predicciones, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        nombre_archivo = f"{nombre_base}_{timestamp}.csv"
        ruta_salida = os.path.join(ruta_predicciones, nombre_archivo)

        try:
            self.df_modelo.to_csv(ruta_salida, index=False, encoding="utf-8-sig")
            self.__log(f"Archivo exportado exitosamente a: {ruta_salida}")
        except Exception as e:
            self.__log(f"Error al exportar el archivo: {e}")
    
    # Métodos privados
    # Método para la carga de todos los CSV
    def __cargar_datos(self):
        ruta_limpio = obtener_ruta('ruta_csv_limpio')
        
        # Carpetas de los CSV limpios
        ruta_siniestro = os.path.join(ruta_limpio, "Siniestralidad", "Ficha 0")
        ruta_vehiculos = os.path.join(ruta_limpio, "Siniestralidad", "Ficha 1")
        ruta_trafico = os.path.join(ruta_limpio, "Tráfico Mensual")

        self.df_siniestro = self.__cargar_csv(ruta_siniestro)
        self.df_vehiculos = self.__cargar_csv(ruta_vehiculos)
        self.df_trafico = self.__cargar_csv(ruta_trafico)
    
    # Método para cargar un archivo CSV limpio
    def __cargar_csv(self, ruta_base):
        dfs = []
        for carpeta_anio in os.listdir(ruta_base):
            ruta_anio = os.path.join(ruta_base, carpeta_anio)
            
            if not os.path.isdir(ruta_anio):
                continue

            for archivo in os.listdir(ruta_anio):
                if archivo.endswith("_Limpio.csv"):
                    ruta = os.path.join(ruta_anio, archivo)

                    # TODO: Dejarlo así o cambiar la separación de Siniestralidad Ficha 0 para tener consistencia
                    sep = "," if "Ficha 0" not in ruta_base else "|"
                    skiprows = 0 if "Ficha 0" not in ruta_base else 1
                    quotechar = '"' if "Ficha 0" not in ruta_base else None
                    
                    try:
                        read_kwargs = {
                            "encoding": "utf-8",
                            "sep": sep,
                            "dtype": str,
                            "skiprows": skiprows
                        }
                        if quotechar:
                            read_kwargs["quotechar"] = quotechar

                        df = pd.read_csv(ruta, **read_kwargs)
                        df.columns = df.columns.str.strip()
                        
                        if "Siniestralidad" in ruta_base and "ID Accidente" not in df.columns:
                            self.__log(f"Omitido: '{archivo}' (sin columna 'ID Accidente')")
                            continue

                        dfs.append(df)
                    except Exception as e:
                        self.__log(f"Error al leer {archivo}: {e}")
        
        if not dfs:
            self.__log(f"No se encontraron archivos válidos en {ruta_base}")
            return pd.DataFrame()
        
        self.__log(f"Registros cargados desde {ruta_base}: {len(dfs)} archivos válidos")
        return pd.concat(dfs, ignore_index=True)
    
    # Método para unir los ID Accidentes
    def __unir_fuentes(self):
        # Unir vehículos por ID Accidente
        df_vehiculos = self.df_vehiculos.copy()
        df_vehiculos["ID Accidente"] = df_vehiculos["ID Accidente"].astype(str)

        df_siniestro = self.df_siniestro.copy()
        df_siniestro["ID Accidente"] = df_siniestro["ID Accidente"].astype(str)

        # Agregar cantidad de vehículos por accidente
        conteo_vehiculos = df_vehiculos.groupby("ID Accidente").size().reset_index(name="Cantidad Vehículos")
        self.df_siniestro = df_siniestro.merge(conteo_vehiculos, on="ID Accidente", how="left")
        self.df_siniestro["Cantidad Vehículos"] = self.df_siniestro["Cantidad Vehículos"].fillna(0).astype(int)

    # Método para la preparación del dataset
    def __preparar_dataset(self):
        df_siniestro = self.df_siniestro.copy()
        df_trafico = self.df_trafico.copy()

        df_siniestro["Fecha"] = pd.to_datetime(df_siniestro["Fecha"], errors="coerce")
        df_siniestro = df_siniestro[df_siniestro["Fecha"].notna()]
        df_siniestro["Fecha_dia"] = df_siniestro["Fecha"].dt.date

        columnas_afectados = [c for c in df_siniestro.columns if "Consecuencias" in c or "Cantidad Afectados" in c]
        df_siniestro["Cantidad Afectados"] = df_siniestro[columnas_afectados].apply(pd.to_numeric, errors="coerce").fillna(0).sum(axis=1)

        df_siniestro_agg = df_siniestro.groupby("Fecha_dia").agg({
            "ID Accidente": "count",
            "Cantidad Afectados": "sum",
            "Cantidad Vehículos": "sum"
        }).reset_index().rename(columns={"ID Accidente": "Cantidad Accidentes"})

        df_trafico["Fecha"] = pd.to_datetime(df_trafico["Fecha"], errors="coerce")
        df_trafico = df_trafico[df_trafico["Fecha"].notna()]
        df_trafico["Fecha_dia"] = df_trafico["Fecha"].dt.date
        df_trafico["Contar"] = pd.to_numeric(df_trafico["Contar"], errors="coerce").fillna(0)
        df_trafico_agg = df_trafico.groupby("Fecha_dia")["Contar"].sum().reset_index()

        df_modelo = df_trafico_agg.merge(df_siniestro_agg, on="Fecha_dia", how="left").fillna(0)
        df_modelo["Fecha"] = pd.to_datetime(df_modelo["Fecha_dia"])
        df_modelo["DiaSemana"] = df_modelo["Fecha"].dt.dayofweek
        df_modelo["Mes"] = df_modelo["Fecha"].dt.month

        self.__log(f"Registros diarios para modelar: {len(df_modelo)}")
        self.df_modelo = df_modelo
    
    # Método para el entrenamiento del modelo (Regresión lineal)
    def __entrenar_modelo(self):
        X = self.df_modelo[["Contar", "Cantidad Vehículos", "DiaSemana", "Mes"]]
        y = self.df_modelo["Cantidad Accidentes"]

        self.modelo = LinearRegression()
        self.modelo.fit(X, y)

        self.df_modelo["Predicción"] = self.modelo.predict(X)
    
    # Método para la generación del gráfico
    def __visualizar_resultados(self):
        df_plot = self.df_modelo.sort_values("Contar") # Ordenar por tráfico
        y = df_plot["Cantidad Accidentes"]
        y_pred = df_plot["Predicción"]

        # Calcular métricas

        # RMSE: Mide la diferencia promedio al cuadrado entre los valores reales
        # y los predichos.
        # 
        # En su interpretación, cuanto más bajo el valor, mejor.
        # 
        # Ejemplo: Predice 10 afectados y el valor real era 13, entonces el error
        # es 3 al cuadrado (9). Si el error fuera 6, el cuadrado sería 36.
        #
        # MAE: Mide el promedio de los errores absolutos entre predicción y realidad.
        # En su interpretación es como RMSE, pero es más robusto ante outliers.
        #
        # Ejemplo: El modelo se equivoca 2, 3 y 5 afectados en tres casos, entonces.
        # el MAE sería (2 + 3 + 5) / 3 = 3.33
        #
        # R² (Coeficiente de determinación): Mide qué proporción de la variabiliad
        # total de la variable objetivo es explicada por el modelo.
        # Va de 0 a 1 (pero puede ser negativo si el modelo es peor).
        #
        # En su interpretación, si R² = 1, entonces la predicción es perfecta.
        # (en este caso, R² ≥ 0.75)
        # Si R² = 0, entonces no es perfecta.
        #
        # Ejemplo: Si el tráfico y cantidad de vehículos explican el 80% de la
        # variación en afectados, entonces R² = 0.80

        rmse = root_mean_squared_error(y, y_pred)
        mae = mean_absolute_error(y, y_pred)
        r2 = r2_score(y, y_pred)

        # Si el modelo no cumple el umbral
        if r2 < 0.75:
            self.__log("Advertencia: no cumple el umbral mínimo de precisión.")

        # Agrupar histórica por tráfico
        max_contar = int(self.df_modelo["Contar"].max())
        bin_size = 50000 if max_contar > 100000 else 10000
        bins = range(0, max_contar + bin_size, bin_size)

        df_modelo = self.df_modelo.copy()
        df_modelo["Contar_bin"] = pd.cut(df_modelo["Contar"], bins=bins)
        df_agg = df_modelo.groupby("Contar_bin", observed=False).agg({
            "Contar": "mean",
            "Cantidad Accidentes": "mean"
        }).reset_index()

        # Predicción futura
        ultima_fecha = self.df_modelo["Fecha"].max()
        anio = ultima_fecha.year + (1 if ultima_fecha.month == 12 else 0)
        mes = 1 if ultima_fecha.month == 12 else ultima_fecha.month + 1
        fecha_inicio = pd.Timestamp(year=anio, month=mes, day=1)
        fecha_fin = fecha_inicio + pd.offsets.MonthEnd(1)

        fechas_futuras = pd.date_range(start=fecha_inicio, end=fecha_fin, freq="D")
        promedio_trafico = self.df_modelo["Contar"].mean()
        promedio_vehiculos = self.df_modelo["Cantidad Vehículos"].mean()

        df_futuro = pd.DataFrame({
            "Fecha": fechas_futuras,
            "Contar": promedio_trafico,
            "Cantidad Vehículos": promedio_vehiculos,
            "DiaSemana": fechas_futuras.dayofweek,
            "Mes": fechas_futuras.month
        })

        X_futuro = df_futuro[["Contar", "Cantidad Vehículos", "DiaSemana", "Mes"]]
        df_futuro["Predicción Accidentes"] = self.modelo.predict(X_futuro)

        # Crear gráfico
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
        
        # Histórico agrupado
        ax1.plot(df_agg["Contar"], df_agg["Cantidad Accidentes"], color='blue', marker='o', label='Histórico agrupado')
        ax1.set_title("Histórico agrupado")
        ax1.set_xlabel("Tráfico promedio por rango")
        ax1.set_ylabel("Accidentes promedio")
        ax1.grid(True)

        # Predicción futura
        ax2.plot(df_futuro["Fecha"], df_futuro["Predicción Accidentes"], color='red', marker='x', label=f'Predicción {fecha_inicio.strftime("%B %Y")}')
        ax2.set_title(f"Predicción futura ({fecha_inicio.strftime('%B %Y')})")
        ax2.set_xlabel("Fecha")
        ax2.set_ylabel("Accidentes estimados")
        ax2.grid(True)

        # Métricas del modelo
        metricas_txt = f"RMSE: {rmse:.2f}\nMAE: {mae:.2f}\nR²: {r2:.2f}"
        ax1.text(0.02, 0.98, metricas_txt, transform=ax1.transAxes, fontsize=10, verticalalignment='top', bbox=dict(facecolor='white', alpha=0.8))

        fig.suptitle("Accidentes históricos y predicción futura (según Tráfico y Siniestralidad)", fontsize=14)
        fig.tight_layout(rect=[0, 0, 1, 0.95])

        self.fig = fig
        plt.close(self.fig) # Cerrar el gráfico de la memoria
