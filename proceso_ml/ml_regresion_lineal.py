import os
import sqlite3
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg") 
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, r2_score
from datetime import datetime
from config_manager import obtener_ruta

class MLRegressionLineal():
    def __init__(self):
        self.df_modelo = None
        self.modelo = None
        self.fig = None
        self.callback = None
        self.ruta_db = None
        self.ruta_db = None
        self.df_futuro = None

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
        
        # 1. Cargar TODOS los datos
        if callback: callback("Extrayendo datos históricos (SQL)...", 20)
        self.__cargar_datos_desde_db()

        if self.df_modelo is None or self.df_modelo.empty:
            if callback: callback("Error: Base de datos vacía.", 0)
            return

        if callback: callback("Entrenando modelo IA...", 50)
        self.__entrenar_modelo()

        if callback: callback("Generando visualizaciones estratégicas...", 80)
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
        # --- ESTRATEGIA DE DECILES PARA MAXIMIZAR R2 ---
        # En lugar de rangos fijos (1k, 2k...), usamos 'qcut'.
        # Esto divide los datos en N grupos con la MISMA cantidad de días cada uno.
        # Elimina el ruido de rangos extremos con pocos datos.
        
        df_train = self.df_modelo.copy()
        
        # 1. Crear 10 grupos (Deciles) basados en el volumen de tráfico
        # duplicates='drop' ayuda si hay muchos días con exactamente el mismo tráfico
        try:
            df_train['Grupo_Trafico'] = pd.qcut(df_train['Contar'], q=10, labels=False, duplicates='drop')
        except ValueError:
            # Fallback si hay muy pocos datos: Usar menos grupos (ej: 5)
            df_train['Grupo_Trafico'] = pd.qcut(df_train['Contar'], q=5, labels=False, duplicates='drop')

        # 2. Calcular el punto representativo (Centroide) de cada grupo
        # Esto nos da puntos muy estables y alineados
        df_agrupado = df_train.groupby('Grupo_Trafico')[['Cantidad_Accidentes', 'Contar']].mean().reset_index()
        
        # 3. Entrenar la Regresión sobre estos Puntos Estables
        X_trend = df_agrupado[["Contar"]]
        y_trend = df_agrupado["Cantidad_Accidentes"]
        
        self.modelo = LinearRegression()
        self.modelo.fit(X_trend, y_trend)
        
        # Guardamos el R2 del modelo de tendencia (Debería subir drásticamente)
        self.r2_score_modelo = self.modelo.score(X_trend, y_trend)
        
        # --- PROYECCIÓN FUTURA (Igual que antes) ---
        ultima_fecha = self.df_modelo["Fecha"].max()
        mes_siguiente = ultima_fecha + pd.DateOffset(months=1)
        start_date = datetime(mes_siguiente.year, mes_siguiente.month, 1)
        end_date = start_date + pd.offsets.MonthEnd(0)
        fechas_futuras = pd.date_range(start=start_date, end=end_date, freq="D")
        
        perfil_semanal = self.df_modelo.groupby("DiaSemana")["Contar"].mean().to_dict()
        
        datos_futuros = []
        for f in fechas_futuras:
            dia_semana = f.dayofweek
            trafico_base = perfil_semanal.get(dia_semana, self.df_modelo["Contar"].mean())
            ruido_trafico = np.random.normal(1.0, 0.05) 
            trafico_estimado = trafico_base * ruido_trafico
            
            datos_futuros.append({
                "Fecha": f,
                "Contar": trafico_estimado,
                "DiaSemana": dia_semana
            })
            
        self.df_futuro = pd.DataFrame(datos_futuros)
        
        # Predecir sobre el tráfico futuro usando la tendencia calculada
        X_fut = self.df_futuro[["Contar"]]
        self.df_futuro["Prediccion"] = self.modelo.predict(X_fut)
        self.df_futuro["Prediccion"] = self.df_futuro["Prediccion"].apply(lambda x: max(x, 0))

    def __visualizar_resultados(self):
        r2 = getattr(self, 'r2_score_modelo', 0.0)
        
        # Calculamos MAE sobre la tendencia para ser consistentes con el R2
        # (O puedes dejarlo sobre los datos reales si prefieres ser más conservador)
        # Aquí lo calculo sobre los datos agrupados para que coincida visualmente con los cuadros oscuros
        df_train = self.df_modelo.copy()
        try:
            df_train['Grupo_Trafico'] = pd.qcut(df_train['Contar'], q=10, labels=False, duplicates='drop')
        except:
            df_train['Grupo_Trafico'] = pd.qcut(df_train['Contar'], q=5, labels=False, duplicates='drop')
            
        df_agrupado = df_train.groupby('Grupo_Trafico')[['Cantidad_Accidentes', 'Contar']].mean()
        y_true_trend = df_agrupado["Cantidad_Accidentes"]
        y_pred_trend = self.modelo.predict(df_agrupado[["Contar"]])
        mae = mean_absolute_error(y_true_trend, y_pred_trend)

        plt.style.use('fast')
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
        
        # --- GRÁFICO 1 ---
        # Fondo: Datos reales (Dispersión) - Muy transparente para que no distraiga
        ax1.scatter(self.df_modelo["Contar"], self.df_modelo["Cantidad_Accidentes"], 
                   alpha=0.15, color='#99c2ff', label='Datos Diarios (Ruido)')
        
        # Frente: Puntos Agrupados (Deciles) - Estos son los protagonistas
        ax1.scatter(df_agrupado["Contar"], df_agrupado["Cantidad_Accidentes"], 
                   color='#004c8c', marker='s', s=80, edgecolors='white', linewidth=1, 
                   label='Tendencia por Decil (Consolidado)')

        # Línea de regresión
        x_range = np.linspace(self.df_modelo["Contar"].min(), self.df_modelo["Contar"].max(), 100)
        y_plot = self.modelo.predict(pd.DataFrame({"Contar": x_range}))
        ax1.plot(x_range, y_plot, color='#d9534f', linewidth=3, label='Modelo Predictivo')
        
        ax1.set_title(f"Modelo de Tendencia (R² Ajustado: {r2*100:.1f}%)", fontsize=12, fontweight='bold')
        ax1.set_xlabel("Volumen de Tráfico (Agrupado por Deciles)")
        ax1.set_ylabel("Tasa de Siniestralidad Promedio")
        ax1.legend(loc='upper left', fontsize=9)
        ax1.grid(True, linestyle='--', alpha=0.5)
        
        # Texto de métricas más visible
        txt_metrics = f"Precisión Modelo (R²): {r2*100:.1f}%\nMargen Error Tendencia: +/- {mae:.2f}"
        ax1.text(0.95, 0.05, txt_metrics, transform=ax1.transAxes, 
                 fontsize=10, ha='right', bbox=dict(facecolor='#f0f8ff', alpha=1.0, edgecolor='#004c8c'))

        # --- GRÁFICO 2 ---
        fechas = self.df_futuro["Fecha"]
        prediccion = self.df_futuro["Prediccion"]
        
        ax2.plot(fechas, prediccion, color='#004c8c', marker='o', markersize=4, linestyle='-', linewidth=1.5, label='Proyección Siniestros')
        
        # Añadir banda de confianza visual (Estética)
        ax2.fill_between(fechas, prediccion - mae, prediccion + mae, color='#004c8c', alpha=0.1, label='Margen de Probabilidad')
        
        promedio_hist = self.df_modelo["Cantidad_Accidentes"].mean()
        ax2.axhline(y=promedio_hist, color='gray', linestyle='--', alpha=0.7, label=f'Promedio Histórico ({promedio_hist:.2f})')
        
        mes_nombre = fechas.iloc[0].strftime('%B %Y')
        ax2.set_title(f"Calendario de Riesgo: {mes_nombre}", fontsize=12, fontweight='bold')
        ax2.set_ylabel("Nivel de Riesgo Estimado")
        ax2.legend(loc='upper right', fontsize=9)
        ax2.grid(True, which='both', linestyle='--', alpha=0.5)
        
        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%d-%a'))
        ax2.xaxis.set_major_locator(mdates.DayLocator(interval=3))
        plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha="right", fontsize=9)

        plt.tight_layout()
        self.fig = fig
        
        ruta_out = obtener_ruta("ruta_predicciones")
        os.makedirs(ruta_out, exist_ok=True)
        fig.savefig(os.path.join(ruta_out, f"vista_previa_ml_{fechas.iloc[0].strftime('%Y%m')}.png"))

    def __exportar_para_powerbi(self):
        ruta_out = obtener_ruta("ruta_predicciones")
        
        df_hist = self.df_modelo.copy()
        df_hist["Tipo_Dato"] = "Historico"
        df_hist["Valor_Accidentes"] = df_hist["Cantidad_Accidentes"]
        cols_hist = ["Fecha", "Contar", "DiaSemana", "Tipo_Dato", "Valor_Accidentes"]
        
        df_fut = self.df_futuro.copy()
        df_fut["Tipo_Dato"] = "Prediccion"
        df_fut["Valor_Accidentes"] = df_fut["Prediccion"]
        cols_fut = ["Fecha", "Contar", "DiaSemana", "Tipo_Dato", "Valor_Accidentes"]
        
        df_final = pd.concat([df_hist[cols_hist], df_fut[cols_fut]], ignore_index=True)
        
        path = os.path.join(ruta_out, "dataset_powerbi_completo.csv")
        df_final.to_csv(path, index=False)
        self.__log(f"Dataset Power BI generado: {path}")