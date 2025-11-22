import os
import sqlite3
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg") 
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from sklearn.linear_model import LinearRegression
from sklearn.metrics import root_mean_squared_error, mean_absolute_error, r2_score
from datetime import datetime
from config_manager import obtener_ruta

class MLRegressionLineal():
    def __init__(self):
        self.df_modelo = None
        self.modelo = None
        self.fig = None
        self.callback = None
        self.ruta_db = None

    def __log(self, msg):
        now = datetime.now().strftime("%H:%M:%S")
        log_msg = f"[{now}] {msg}"
        print(log_msg)
        if self.callback: self.callback(log_msg, None)

    def realizar_prediccion(self, callback=None):
        self.callback = callback
        
        try:
            self.ruta_db = obtener_ruta("ruta_database")
        except Exception as e:
            self.__log(f"Error config: {e}")
            return

        self.__log("Iniciando motor de análisis...")
        
        # 1. Cargar TODOS los datos (Sin filtros, Power BI filtrará)
        if callback: callback("Extrayendo datos históricos (SQL)...", 20)
        self.__cargar_datos_desde_db()

        if self.df_modelo is None or self.df_modelo.empty:
            if callback: callback("Error: Base de datos vacía.", 0)
            return

        if callback: callback("Entrenando modelo IA...", 50)
        self.__entrenar_modelo()

        if callback: callback("Generando dataset y vista previa...", 80)
        self.__visualizar_resultados() # Genera gráficos y datos futuros
        
        # Exportación especial para Power BI
        self.__exportar_para_powerbi()

        if callback: callback("Proceso finalizado.", 100)

    def __cargar_datos_desde_db(self):
        """Carga datos agregados por día."""
        conn = sqlite3.connect(self.ruta_db)
        query = """
        WITH DiarioTrafico AS (
            SELECT 
                DT.Date,
                SUM(FT.trafficVolume) as TotalTrafico
            FROM factTraffic FT
            JOIN dim_DateTime DT ON FT.idDateTime = DT.idDateTime
            GROUP BY DT.Date
        ),
        DiarioSiniestros AS (
            SELECT 
                DT.Date,
                COUNT(DISTINCT FA.idAccident) as TotalAccidentes,
                COUNT(FVA.idVehicleAccident) as TotalVehiculosInvolucrados
            FROM factAccident FA
            JOIN dim_DateTime DT ON FA.idDateTime = DT.idDateTime
            LEFT JOIN factVehicleAccident FVA ON FA.idAccident = FVA.idAccident
            GROUP BY DT.Date
        )
        SELECT 
            DT.Date as Fecha,
            DT.WeekDay,
            DT.Month,
            COALESCE(T.TotalTrafico, 0) as Contar,
            COALESCE(S.TotalAccidentes, 0) as Cantidad_Accidentes,
            COALESCE(S.TotalVehiculosInvolucrados, 0) as Cantidad_Vehiculos
        FROM dim_DateTime DT
        LEFT JOIN DiarioTrafico T ON DT.Date = T.Date
        LEFT JOIN DiarioSiniestros S ON DT.Date = S.Date
        WHERE DT.Hour = 12 AND DT.Minute = 0 
          AND DT.Date <= DATE('now')
          AND (T.TotalTrafico > 0 OR S.TotalAccidentes > 0)
        ORDER BY DT.Date ASC;
        """
        try:
            df = pd.read_sql_query(query, conn)
            df["Fecha"] = pd.to_datetime(df["Fecha"])
            
            mapa_dias = {'Lunes': 0, 'Martes': 1, 'Miércoles': 2, 'Jueves': 3, 'Viernes': 4, 'Sábado': 5, 'Domingo': 6}
            df["DiaSemana"] = df["WeekDay"].map(mapa_dias).fillna(0).astype(int)
            
            self.df_modelo = df
            self.__log(f"Datos Históricos: {len(df)} días.")
        except Exception as e:
            self.__log(f"Error SQL: {e}")
            self.df_modelo = None
        finally:
            conn.close()

    def __entrenar_modelo(self):
        X = self.df_modelo[["Contar", "Cantidad_Vehiculos", "DiaSemana", "Month"]]
        y = self.df_modelo["Cantidad_Accidentes"]
        self.modelo = LinearRegression()
        self.modelo.fit(X, y)
        self.df_modelo["Prediccion"] = self.modelo.predict(X)

    def __visualizar_resultados(self):
        # Calcular métricas
        y_true = self.df_modelo["Cantidad_Accidentes"]
        y_pred = self.df_modelo["Prediccion"]
        r2 = r2_score(y_true, y_pred)
        mae = mean_absolute_error(y_true, y_pred)

        # --- Generar Futuro (Próximo Mes) ---
        ultima_fecha = self.df_modelo["Fecha"].max()
        mes_siguiente = ultima_fecha + pd.DateOffset(months=1)
        start_date = datetime(mes_siguiente.year, mes_siguiente.month, 1)
        end_date = start_date + pd.offsets.MonthEnd(0)
        fechas = pd.date_range(start=start_date, end=end_date, freq="D")

        base_trafico = self.df_modelo["Contar"].mean()
        base_vehiculos = self.df_modelo["Cantidad_Vehiculos"].mean()
        
        # Factores semanales + Ruido Estocástico
        factores = {0:1.0, 1:1.0, 2:1.0, 3:1.05, 4:1.20, 5:0.85, 6:0.65} 

        datos_futuros = []
        for f in fechas:
            dia = f.dayofweek
            factor_base = factores.get(dia, 1.0)
            ruido = np.random.normal(1.0, 0.08) # +/- 8% variación aleatoria
            
            datos_futuros.append({
                "Fecha": f,
                "Contar": base_trafico * factor_base * ruido,
                "Cantidad_Vehiculos": base_vehiculos,
                "DiaSemana": dia,
                "Month": f.month
            })
        
        self.df_futuro = pd.DataFrame(datos_futuros)
        X_fut = self.df_futuro[["Contar", "Cantidad_Vehiculos", "DiaSemana", "Month"]]
        self.df_futuro["Prediccion"] = self.modelo.predict(X_fut)

        # --- GRÁFICOS (Vista Previa para Python) ---
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

        # 1. Histograma de Riesgo (Corrección "Heatmap")
        bins = np.linspace(self.df_modelo["Contar"].min(), self.df_modelo["Contar"].max(), 8)
        self.df_modelo['RangoTrafico'] = pd.cut(self.df_modelo['Contar'], bins=bins)
        riesgo = self.df_modelo.groupby('RangoTrafico', observed=True)['Cantidad_Accidentes'].mean()
        
        x_labels = [f"{int(i.left/1000)}k-{int(i.right/1000)}k" for i in riesgo.index]
        ax1.bar(x_labels, riesgo.values, color='#004c8c', alpha=0.8)
        ax1.set_title("Promedio Accidentes vs Volumen Tráfico", fontsize=10, fontweight='bold')
        ax1.set_ylabel("Siniestros Promedio")
        plt.setp(ax1.xaxis.get_majorticklabels(), rotation=30, ha="right", fontsize=8)
        ax1.grid(axis='y', alpha=0.3)

        # Métricas
        txt = f"Precisión Modelo: {r2*100:.1f}%\nError: +/- {mae:.2f}"
        ax1.text(0.05, 0.90, txt, transform=ax1.transAxes, fontsize=9, bbox=dict(facecolor='white', alpha=0.9))

        # 2. Proyección Orgánica
        ax2.plot(self.df_futuro["Fecha"], self.df_futuro["Prediccion"], color='#d9534f', marker='.', linestyle='-')
        ax2.set_title(f"Proyección: {start_date.strftime('%B %Y')}", fontsize=10, fontweight='bold')
        ax2.set_ylabel("Riesgo Estimado")
        ax2.grid(True, alpha=0.5)
        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%d-%b'))
        ax2.xaxis.set_major_locator(mdates.DayLocator(interval=4))
        plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha="right")

        plt.tight_layout()

        # Guardar imagen
        self.fig = fig
        ruta_out = obtener_ruta("ruta_predicciones")
        os.makedirs(ruta_out, exist_ok=True)
        fig.savefig(os.path.join(ruta_out, f"vista_previa_ml_{start_date.strftime('%Y%m')}.png"))

    def __exportar_para_powerbi(self):
        """Genera un CSV unificado: Historia + Predicción"""
        ruta_out = obtener_ruta("ruta_predicciones")
        
        # 1. Preparar Historia
        df_hist = self.df_modelo.copy()
        df_hist["Tipo_Dato"] = "Historico"
        df_hist["Valor_Accidentes"] = df_hist["Cantidad_Accidentes"] # Valor Real
        cols_hist = ["Fecha", "Contar", "DiaSemana", "Tipo_Dato", "Valor_Accidentes"]
        
        # 2. Preparar Predicción
        df_fut = self.df_futuro.copy()
        df_fut["Tipo_Dato"] = "Prediccion"
        df_fut["Valor_Accidentes"] = df_fut["Prediccion"] # Valor Estimado
        cols_fut = ["Fecha", "Contar", "DiaSemana", "Tipo_Dato", "Valor_Accidentes"]
        
        # 3. Unir
        df_final = pd.concat([df_hist[cols_hist], df_fut[cols_fut]], ignore_index=True)
        
        # 4. Guardar
        path = os.path.join(ruta_out, "dataset_powerbi_completo.csv")
        df_final.to_csv(path, index=False)
        self.__log(f"Dataset Power BI generado: {path}")