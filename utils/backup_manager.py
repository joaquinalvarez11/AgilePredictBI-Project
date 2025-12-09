import os
import shutil
from datetime import datetime

class BackupManager:
    @staticmethod
    def __detectar_onedrive():
        base = os.path.expanduser("~")
        # Buscamos carpetas comunes de OneDrive
        for carpeta in os.listdir(base):
            if "OneDrive" in carpeta:
                # Preferimos "OneDrive - Personal" o "OneDrive - Empresa" si existen
                return os.path.join(base, carpeta, "SCRDA_Respaldos")
        
        # Default clásico
        return os.path.join(base, "OneDrive", "SCRDA_Respaldos")
    
    def __init__(self, carpeta_onedrive=None):
        if carpeta_onedrive:
            self.CARPETA_ONEDRIVE = carpeta_onedrive
        else:
            self.CARPETA_ONEDRIVE = self.__detectar_onedrive()
    
    def __crear_carpeta_backup(self):
        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        carpeta_backup = os.path.join(self.CARPETA_ONEDRIVE, timestamp)
        os.makedirs(carpeta_backup, exist_ok=True)
        return carpeta_backup

    def subir_a_onedrive_local(self, fuentes):
        """
        Recibe un diccionario: {'nombre_carpeta': 'ruta_local_absoluta'}
        """
        carpeta_backup = self.__crear_carpeta_backup()
        respaldadas = {}
        hay_archivos = False

        # Creamos una carpeta contenedora "SCRDA Excel" dentro del backup
        carpeta_raiz_backup = os.path.join(carpeta_backup, "SCRDA Excel")
        os.makedirs(carpeta_raiz_backup, exist_ok=True)
        
        for nombre, ruta in fuentes.items():
            if not os.path.exists(ruta):
                print(f"[ADVERTENCIA] La ruta '{ruta}' no existe, se omite.")
                continue
            
            # --- CASO 1: ES UNA CARPETA (Ej: Predicciones, Informes) ---
            if os.path.isdir(ruta):
                # Verificar si tiene archivos útiles
                tiene_archivos = any(
                    f for _, _, files in os.walk(ruta)
                    for f in files
                    if not (f.startswith("~$") or f.startswith(".~lock"))
                )
                
                if not tiene_archivos:
                    continue
                
                destino_final = os.path.join(carpeta_raiz_backup, nombre)
                
                # Copiamos todo el árbol de directorios
                shutil.copytree(ruta, destino_final, dirs_exist_ok=True)
                
                # Contamos archivos para el reporte
                count = sum([len(files) for r, d, files in os.walk(destino_final)])
                respaldadas[nombre] = count
                hay_archivos = True
                
            # --- CASO 2: ES UN ARCHIVO (Ej: database .db) ---
            elif os.path.isfile(ruta):
                destino_dir = os.path.join(carpeta_raiz_backup, nombre) # Ej: .../backup/SCRDA Excel/database
                os.makedirs(destino_dir, exist_ok=True)
                
                shutil.copy2(ruta, os.path.join(destino_dir, os.path.basename(ruta)))
                
                respaldadas[nombre] = 1
                hay_archivos = True
        
        if not hay_archivos:
            # Si no se copió nada, borramos la carpeta vacía creada
            shutil.rmtree(carpeta_backup)
            raise RuntimeError("No hay archivos válidos para respaldar en las rutas seleccionadas.")

        return carpeta_backup, respaldadas
    
    def descargar_de_onedrive_local(self, carpeta_backup_nombre, ruta_base_usuario):
        """
        Restaura el backup seleccionado en la ruta base del usuario.
        """
        # Ruta completa donde está el backup en OneDrive
        origen_backup = os.path.join(self.CARPETA_ONEDRIVE, carpeta_backup_nombre, "SCRDA Excel")
        
        if not os.path.exists(origen_backup):
            raise RuntimeError(f"El respaldo parece dañado o incompleto. No se encontró: {origen_backup}")
        
        destino_final = os.path.join(ruta_base_usuario, "SCRDA Excel")

        # Usamos copytree para restaurar, permitiendo sobreescribir (dirs_exist_ok=True)
        shutil.copytree(origen_backup, destino_final, dirs_exist_ok=True)