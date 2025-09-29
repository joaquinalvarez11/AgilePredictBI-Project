import pandas as pd
from utils.config_carga import BRUTOS_PATH, LIMPIOS_PATH
import re
import os # TODO: Reemplazar librería "os" por "gestion_archivos.py" cuando tenga funcionalidad

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
        self.__ruta_bruta = BRUTOS_PATH + '/Tráfico Mensual/' # Ruta de la carpeta de tráfico mensual bruto
        self.__ruta_limpia = LIMPIOS_PATH + '/Tráfico Mensual/' # Ruta de la carpeta de tráfico mensual limpio
    
    # Método principal para la transformación
    def ejecutar_etl(self):
        # TODO: Reemplazar todos los métodos "os" con los de gestion_archivos.py
        # Bucle para cada año
        for anio in sorted(os.listdir(self.__ruta_bruta)):
            ruta_anio_bruto = os.path.join(self.__ruta_bruta, anio)
            ruta_anio_limpio = os.path.join(self.__ruta_limpia, anio)
            os.makedirs(ruta_anio_limpio, exist_ok=True)
            
            # Bucle para guardar archivo en la carpeta
            for archivo in sorted(os.listdir(ruta_anio_bruto)):
                nombre_base = os.path.splitext(archivo)[0]
                ruta_excel = os.path.join(ruta_anio_bruto, archivo)
                ruta_csv = os.path.join(ruta_anio_limpio, f"{nombre_base}.csv")

                if os.path.exists(ruta_csv):
                    continue # La transformación existe, pasar al siguiente bucle

                if not self.__es_hoja_valida(ruta_excel):
                    continue # La hoja no es válida, pasar al siguiente bucle

                df_transformado = self.__transformar_excel(ruta_excel)
                df_transformado.to_csv(ruta_csv, index=False, encoding='utf-8-sig')
    
    # Métodos privados
    # Método para revisar si la primera hoja es válida
    def __es_hoja_valida(self, ruta_excel):
        try:
            xls = pd.ExcelFile(ruta_excel)
            for hoja in xls.sheet_names:
                if hoja.startswith('1') or hoja[0].isdigit():
                    return True # La hoja empieza con "1". Es válida
            return False # No se encontró ninguna hoja válida
        
        except Exception:
            return False # El archivo está corrupto o ilegible
    
    # Método para obtener año y mes desde el nombre del archivo
    def __extraer_fecha_desde_nombre(self, nombre_archivo):
        # Separar estructura por año y mes
        partes = re.search(r'(\d{4})-(\d{2})', nombre_archivo)
        if partes:
            anio = int(partes.group(1))
            mes = int(partes.group(2))
            return anio, mes
        return None, None
    
    # Método para la traducción del nombre de la hoja excel
    def __traducir_tipo_vehiculo(self, nombre_hoja):
        nombre = nombre_hoja.strip().upper()
        return self.__DICCIONARIO_TIPO_VEHICULO.get(nombre, nombre_hoja.title())
    
    # Método para la transformación en bucle
    def __transformar_excel(self, ruta_excel):
        xls = pd.ExcelFile(ruta_excel)
        hojas_validas = [hoja for hoja in xls.sheet_names if hoja.startswith('1') or hoja[0].isdigit()]
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
                print(f"[{nombre_archivo} - {hoja}] advertencia: Se excluyeron {num_invalidas} filas con fechas inválidas")

            # Filtrar solo fechas válidas
            df_largo = df_largo[df_largo['Date'].notna()]
            
            # Metadato del tipo de vehículo
            df_largo['VehicleType'] = self.__traducir_tipo_vehiculo(hoja)

            dataframes.append(df_largo)
        
        # Reordenar columnas
        if dataframes:
            df_final = pd.concat(dataframes, ignore_index=True)
            df_final = df_final[orden_columnas]
        else:
            df_final = pd.DataFrame(columns=orden_columnas)
        
        return df_final
    
    # Método sólo como guía de la estructura excel
    def __inspeccionar_estructura(self, ruta_excel):
        xls = pd.ExcelFile(ruta_excel)
        hoja = [h for h in xls.sheet_names if h.startswith('1') or h[0].isdigit()][0]
        df = xls.parse(hoja, skiprows=5, header=None)
        print(f"Columnas detectadas: {df.shape[1]}")
        print("Primeras 5 filas:")
        print(df.head())
