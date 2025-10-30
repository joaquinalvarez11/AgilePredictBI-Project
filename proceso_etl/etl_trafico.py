import pandas as pd
import re
import os # TODO: Reemplazar librería "os" por "gestion_archivos.py" cuando tenga funcionalidad
from pathlib import Path
from datetime import datetime
from config_manager import obtener_ruta

class ETLTrafico():
    # TODO: Mover este diccionario en otro modulo
    # Diccionario constante para traducción del nombre de cada hoja
    __DICCIONARIO_TIPO_VEHICULO = {
        '1 MOTO': 'Moto',
        '2 AUTOCMTA': 'Auto/Camioneta',
        '3 CAMION 2 EJES CTA RD': 'Camión 2 Ejes Cta/Rd',
        '4 BUS 2 EJES': 'Bus 2 Ejes',
        '5 CAMION +2 EJES': 'Camión +2 Ejes',
        '6 BUS +2 EJES': 'Bus +2 Ejes',
        '12 SOBREDIMEN.': 'Sobredimensionado'
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
        anio_str = self.__extraer_anio_de_ruta(ruta_archivo_excel)
        if not anio_str:
            self.__log(f"ADVERTENCIA: No se pudo determinar el año para '{nombre_base}'. Se guardará en la raíz de 'Trafico'.")
            anio_str = "" # Guardar en la raíz de __ruta_limpia_base si no se encuentra año
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
            df_transformado = self.__transformar_excel(ruta_archivo_excel)
            if df_transformado.empty:
                 self.__log(f"La transformación de '{nombre_base}' no produjo datos. Saltando guardado.")
                 return True

            df_transformado.to_csv(ruta_csv_salida, index=False, encoding='utf-8-sig')
            self.__log(f"Éxito: '{nombre_base}' procesado ({len(df_transformado)} filas). Guardado en '{anio_str}'.")
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

    def __extraer_fecha_desde_nombre(self, nombre_archivo):
        # Separar estructura por año y mes
        partes = re.search(r'(\d{4})-(\d{2})', nombre_archivo)
        if partes:
            anio = int(partes.group(1))
            mes = int(partes.group(2))
            return anio, mes
        
        self.__log(f"ADVERTENCIA: No se pudo extraer año/mes desde el nombre '{nombre_archivo}'")
        return None, None
    
    # Método para la traducción del nombre de la hoja excel
    def __traducir_tipo_vehiculo(self, nombre_hoja):
        nombre = nombre_hoja.strip().upper()
        return self.__DICCIONARIO_TIPO_VEHICULO.get(nombre, nombre_hoja.title())
    
    # Método para la transformación en bucle
    def __transformar_excel(self, ruta_excel):
        """Realiza la transformación ETL principal para un archivo de tráfico."""
        self.__log(f"Transformando archivo: {Path(ruta_excel).name}")
        engine = 'xlrd' if ruta_excel.lower().endswith('.xls') else 'openpyxl'
        try:
            xls = pd.ExcelFile(ruta_excel, engine=engine)
        except Exception as e:
             self.__log(f"Error fatal al abrir '{Path(ruta_excel).name}' (motor {engine}): {e}") # Usar __log
             return None
        
        hojas_validas = self.__filtrar_hojas_validas(xls.sheet_names)
        orden_columnas = ['VehicleType', 'Date', 'Year', 'Month', 'Day', 'Hour', 'Direction', 'Count']
        dataframes = []

        # Extraer año y mes del nombre del archivo
        nombre_archivo = os.path.basename(ruta_excel)
        anio, mes = self.__extraer_fecha_desde_nombre(nombre_archivo)
        
        for hoja in hojas_validas:
            # Cargar desde la fila 6
            # Asumiendo que todas las hojas de cada archivo tiene el mismo formato de la matriz
            df = xls.parse(hoja, skiprows=5, header=None)
            df = df.iloc[:, :27] # Sólo las primeras 27 columnas

            # Renombrar columnas
            columna_hora = [str(h) for h in range(24)]
            df.columns = ['Col0', 'DiaRaw', 'Direction'] + columna_hora

            # Propagar día hacia abajo
            df['Day'] = df['DiaRaw'].ffill()

            # Filtrar sólo las filas con direction válido
            df = df[df['Direction'].isin(['ASCENDENTE', 'DESCENDENTE'])].copy()

            # Reorganizar el dataframe en filas
            df_largo = df.melt(id_vars=['Day', 'Direction'], value_vars=columna_hora, var_name='Hour', value_name='Count')

            # Limpieza final
            df_largo['Day'] = pd.to_numeric(df_largo['Day'], errors='coerce').fillna(-1).astype(int)
            df_largo = df_largo[df_largo['Day'] != -1] # Excluir días no válidas

            df_largo['Hour'] = pd.to_numeric(df_largo['Hour'], errors='coerce').fillna(-1).astype(int)
            df_largo = df_largo[df_largo['Hour'].between(0, 23)]

            df_largo['Count'] = pd.to_numeric(df_largo['Count'], errors='coerce').fillna(0).astype(int)

            df_largo['Year'] = anio
            df_largo['Month'] = mes
            df_largo['Date'] = pd.to_datetime({'year': df_largo['Year'], 'month': df_largo['Month'], 'day': df_largo['Day']}, errors='coerce')

            # TODO: Registrar información para el log
            num_invalidas = df_largo['Date'].isna().sum()
            if num_invalidas > 0:
                self.__log(f"[{nombre_archivo} - {hoja}] Se excluyeron {num_invalidas} filas con fecha inválidas")

            # Filtrar solo fechas válidas
            df_largo = df_largo[df_largo['Date'].notna()]
            
            # Metadato del tipo de vehículo
            df_largo['VehicleType'] = self.__traducir_tipo_vehiculo(hoja)

            dataframes.append(df_largo)
        
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
