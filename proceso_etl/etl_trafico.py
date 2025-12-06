import pandas as pd
import re
import os
from pathlib import Path
from datetime import datetime
from config_manager import obtener_ruta

class ETLTrafico():
    # Diccionario constante para traducción del nombre de cada hoja
    __CATEGORIA_VEHICULO = {
        '1 MOTO': 'Moto',
        '2 AUTOCMTA': 'Auto/Camioneta',
        '3 CAMION 2 EJES CTA RD': 'Camión 2 Ejes Cta/Rd',
        '4 BUS 2 EJES': 'Bus 2 Ejes',
        '5 CAMION +2 EJES': 'Camión +2 Ejes',
        '6 BUS +2 EJES': 'Bus +2 Ejes',
        '12 SOBREDIMEN.': 'Sobredimensionado'
    }

    __MESES_TEXTO = {
        'ENERO': 1, 'FEBRERO': 2, 'MARZO': 3, 'ABRIL': 4, 'MAYO': 5, 'JUNIO': 6,
        'JULIO': 7, 'AGOSTO': 8, 'SEPTIEMBRE': 9, 'OCTUBRE': 10, 'NOVIEMBRE': 11, 'DICIEMBRE': 12
    }

    def __init__(self):
        """Inicializa las rutas usando el config_manager."""
        base_limpios = obtener_ruta('ruta_csv_limpio')
        self.__ruta_limpia_base = os.path.join(base_limpios, 'Tráfico Mensual/')
        os.makedirs(self.__ruta_limpia_base, exist_ok=True)
        self.__log("ETL de Tráfico inicializada.")
    
    def __log(self, msg):
        """Imprime un mensaje con marca de tiempo específico para esta clase."""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{now}] [ETL Trafico] {msg}")

    # Método principal para la transformación
    def procesar_archivo(self, ruta_archivo_excel):
        """
        Punto de entrada para el controlador. Procesa un único archivo de tráfico.
        """
        self.__log(f"Iniciando procesamiento para: {ruta_archivo_excel}")
        nombre_base = Path(ruta_archivo_excel).stem

        # Lista para recolectar hallazgos de calidad en este archivo
        observaciones_calidad = []
        
        # 1. Intentar sacar año de la carpeta (Fuente más confiable)
        anio_str = self.__extraer_anio_de_ruta(ruta_archivo_excel)
        
        # 2. Si no, intentar del nombre
        if not anio_str:
            anio_nombre, _ = self.__extraer_fecha_flexible(nombre_base)
            if anio_nombre: anio_str = str(anio_nombre)

        if not anio_str:
            observaciones_calidad.append("No se pudo determinar año. Se usará raíz.")
            anio_str = ""
            
        ruta_salida_anio = os.path.join(self.__ruta_limpia_base, anio_str)
        os.makedirs(ruta_salida_anio, exist_ok=True)
        ruta_csv_salida = os.path.join(ruta_salida_anio, f"{nombre_base}_Limpio.csv")

        if os.path.exists(ruta_csv_salida):
            self.__log(f"El archivo '{nombre_base}' ya ha sido procesado. Saltando.")
            return True

        if not self.__es_hoja_valida(ruta_archivo_excel):
            self.__log(f"ADVERTENCIA: El archivo '{nombre_base}' no contiene hojas de cálculo válidas. Saltando.")
            # Devolvemos True porque no es un error, simplemente no hay nada que procesar.
            return True

        try:
            # Pasamos el año de la carpeta como respaldo
            df_transformado = self.__transformar_excel(ruta_archivo_excel, observaciones_calidad, anio_carpeta=anio_str)
            if df_transformado.empty:
                 self.__log(f"La transformación de '{nombre_base}' no produjo datos. Saltando guardado.")
                 return True

            df_transformado.to_csv(ruta_csv_salida, index=False, encoding='utf-8-sig')
            self.__log(f"Éxito: '{nombre_base}' procesado ({len(df_transformado)} filas). Guardado en '{anio_str}'.")
            
            # --- REPORTE DE CALIDAD ---
            if observaciones_calidad:
                self.__log(f"--- REPORTE CALIDAD: {nombre_base} ---")
                for obs in observaciones_calidad:
                    self.__log(f"   • {obs}")
                self.__log("------------------------------------------")
            
            return True
        except Exception as e:
            self.__log(f"ERROR CRÍTICO procesando '{nombre_base}': {e}")
            raise e
    
    # Métodos privados
    # Método para la filtración de hojas válidas
    def __filtrar_hojas_validas(self, sheet_names):
        hojas_validas = []
        
        for hoja in sheet_names:
            partes = hoja.strip().split()
            # Validar si es numérico y mayor que 0
            if partes and partes[0].isdigit() and int(partes[0]) > 0:
                hojas_validas.append(hoja)
        
        return hojas_validas
    
    # Método para revisar si la primera hoja es válida
    def __es_hoja_valida(self, ruta_excel):
        try:
            engine = 'xlrd' if ruta_excel.lower().endswith('.xls') else 'openpyxl'
            xls = pd.ExcelFile(ruta_excel, engine=engine)
            hojas_validas = self.__filtrar_hojas_validas(xls.sheet_names)
            return len(hojas_validas) > 0
        except Exception as e:
            self.__log(f"Error al verificar hojas en '{Path(ruta_excel).name}': {e}")
            return False
    
    # Método para obtener año y mes desde el nombre del archivo
    def __extraer_anio_de_ruta(self, ruta_archivo):
        """Intenta extraer el año (carpeta 'YYYY') del path del archivo."""
        try:
            parts = Path(ruta_archivo).parts
            for part in reversed(parts[:-1]):
                if len(part) == 4 and part.isdigit():
                    return part
        except Exception as e:
            self.__log(f"No se pudo extraer año de la ruta {ruta_archivo}: {e}")
        return None

    def __extraer_fecha_flexible(self, nombre_archivo):
        nombre_limpio = nombre_archivo.upper().replace('_', ' ').replace('-', ' ').replace('.', ' ')
        
        # 1. Buscar patrón numérico clásico: YYYY MM o MM YYYY
        match_num = re.search(r'(\d{4})\s+(\d{2})|(\d{2})\s+(\d{4})', nombre_limpio)
        if match_num:
            # Determinar cual es año (mayor a 1900) y cual mes (1-12)
            nums = [int(n) for n in match_num.groups() if n]
            anio = max(nums)
            mes = min(nums)
            if 1990 < anio < 2050 and 1 <= mes <= 12:
                return anio, mes

        # 2. Buscar Texto (Enero 2019, 2019 Enero)
        anio_encontrado = None
        mes_encontrado = None
        
        # Buscar año 4 digitos
        match_anio = re.search(r'(20\d{2})', nombre_limpio)
        if match_anio:
            anio_encontrado = int(match_anio.group(1))
        
        # Buscar nombre de mes
        for nombre_mes, num_mes in self.__MESES_TEXTO.items():
            if nombre_mes in nombre_limpio:
                mes_encontrado = num_mes
                break
        
        if anio_encontrado and mes_encontrado:
            return anio_encontrado, mes_encontrado
            
        return None, None
    
    def __extraer_plaza_desde_nombre(self, nombre_archivo):
        """Extrae el nombre de la plaza (peaje) desde el nombre del archivo."""
        nombre_lower = nombre_archivo.lower()
        
        if 'cachiyuyo' in nombre_lower:
            return 'Cachiyuyo'
            
        if 'punta colorada' in nombre_lower:
            return 'Punta Colorada'
        if 'colorada' in nombre_lower:
            return 'Punta Colorada'
        if 'punta' in nombre_lower:
            return 'Punta Colorada'
            
        self.__log(f"ADVERTENCIA: No se pudo determinar la Plaza para '{nombre_archivo}'. Se usará 'Desconocida'.")
        return 'Desconocida'

    # Método para la traducción del nombre de la hoja excel
    def __traducir_categoria_vehiculo(self, nombre_hoja):
        nombre = nombre_hoja.strip().upper()
        return self.__CATEGORIA_VEHICULO.get(nombre, nombre_hoja.title())
    
    # Método para la transformación en bucle
    def __transformar_excel(self, ruta_excel, observaciones, anio_carpeta=""):
        """Realiza la transformación ETL principal para un archivo de tráfico."""
        self.__log(f"Transformando archivo: {Path(ruta_excel).name}")
        engine = 'xlrd' if ruta_excel.lower().endswith('.xls') else 'openpyxl'
        try:
            xls = pd.ExcelFile(ruta_excel, engine=engine)
        except Exception as e:
             self.__log(f"Error fatal al abrir '{Path(ruta_excel).name}' (motor {engine}): {e}") # Usar __log
             return None
        
        hojas_validas = self.__filtrar_hojas_validas(xls.sheet_names)
        orden_columnas = ['Plaza', 'Categoria', 'TipoVehiculo', 'Fecha', 'Anio', 'Mes', 'Dia', 'Hora', 'Direccion', 'Contar']
        dataframes = []

        # Extraer año, mes y plaza del nombre del archivo
        nombre_archivo = os.path.basename(ruta_excel)
        
        anio, mes = self.__extraer_fecha_flexible(nombre_archivo)
        
        # Si falla el nombre, usamos la carpeta (Fallback)
        if not anio and anio_carpeta:
            anio = int(anio_carpeta)
            observaciones.append(f"Año tomado de carpeta ({anio}) porque no estaba en nombre.")
        
        # Si aún no tenemos mes, tratamos de adivinarlo por los datos o fallamos
        if not mes:
            observaciones.append("¡ALERTA! Mes no identificado en nombre. Las fechas podrían fallar.")
            mes = 1 # Default peligroso, pero permite procesar si la hoja tiene fechas completas

        plaza = self.__extraer_plaza_desde_nombre(nombre_archivo)
        
        if not anio: observaciones.append("No se pudo extraer fecha del nombre del archivo.")
        if plaza == 'Desconocida': observaciones.append("No se pudo determinar Plaza del nombre.")

        for hoja in hojas_validas:
            try:
                # Cargar desde la fila 6
                # Asumiendo que todas las hojas de cada archivo tiene el mismo formato de la matriz
                df = xls.parse(hoja, skiprows=5, header=None)
                df = df.iloc[:, :27] # Sólo las primeras 27 columnas

                # Renombrar columnas
                columna_hora = [str(h) for h in range(24)]
                df.columns = ['Col0', 'DiaRaw', 'Direccion'] + columna_hora

                # Propagar día hacia abajo
                df['Dia'] = df['DiaRaw'].ffill()

                # Filtrar sólo las filas con direction válido
                df = df[df['Direccion'].isin(['ASCENDENTE', 'DESCENDENTE'])].copy()

                # Reorganizar el dataframe en filas
                df_largo = df.melt(id_vars=['Dia', 'Direccion'], value_vars=columna_hora, var_name='Hora', value_name='Contar')

                # Limpieza final
                df_largo['Dia'] = pd.to_numeric(df_largo['Dia'], errors='coerce').fillna(-1).astype(int)
                df_largo = df_largo[df_largo['Dia'] != -1] # Excluir días no válidas

                df_largo['Hora'] = pd.to_numeric(df_largo['Hora'], errors='coerce').fillna(-1).astype(int)
                df_largo = df_largo[df_largo['Hora'].between(0, 23)]

                df_largo['Contar'] = pd.to_numeric(df_largo['Contar'], errors='coerce').fillna(0).astype(int)

                df_largo['Anio'] = anio
                df_largo['Mes'] = mes
                df_largo['Fecha'] = pd.to_datetime({'year': df_largo['Anio'], 'month': df_largo['Mes'], 'day': df_largo['Dia']}, errors='coerce')

                num_invalidas = df_largo['Fecha'].isna().sum()
                if num_invalidas > 0:
                    observaciones.append(f"Hoja '{hoja}': Se eliminaron {num_invalidas} filas con Fechas inválidas (días inexistentes en el mes).")

                # Filtrar solo fechas válidas
                df_largo = df_largo[df_largo['Fecha'].notna()]
                
                # Metadato de la categoría del vehículo
                categoria = self.__traducir_categoria_vehiculo(hoja)
                df_largo['Categoria'] = categoria

                if categoria in ['Moto', 'Auto/Camioneta']:
                    tipo = 'Ligero'
                else:
                    tipo = 'Pesado'
                
                df_largo['TipoVehiculo'] = tipo

                df_largo['Plaza'] = plaza

                dataframes.append(df_largo)
            except Exception as e:
                observaciones.append(f"Error procesando hoja '{hoja}': {e}")
        
        # Reordenar columnas
        if dataframes:
            df_final = pd.concat(dataframes, ignore_index=True)
            # Asegurar el orden final de columnas
            df_final = df_final[[col for col in orden_columnas if col in df_final.columns]]
            return df_final
        else:
            self.__log(f"No se generaron datos válidos para el archivo '{nombre_archivo}'.")
            return pd.DataFrame(columns=orden_columnas)
    
    # Método sólo como guía de la estructura excel
    def __inspeccionar_estructura(self, ruta_excel):
        try:
            engine = 'xlrd' if ruta_excel.lower().endswith('.xls') else 'openpyxl'
            xls = pd.ExcelFile(ruta_excel, engine=engine)
            hojas_validas = self.__filtrar_hojas_validas(xls.sheet_names)
            df = xls.parse(hojas_validas[0], skiprows=5, header=None)
            print(f"Columnas detectadas: {df.shape[1]}")
            print("Primeras 5 filas:")
            print(df.head())
        except Exception as e:
            self.__log(f"Error en inspección: {e}")
