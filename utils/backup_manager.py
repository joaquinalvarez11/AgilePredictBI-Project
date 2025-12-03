import os
import shutil
from datetime import datetime

class BackupManager():
    @staticmethod
    def __detectar_onedrive():
        base = os.path.expanduser("~")

        for carpeta in os.listdir(base):
            if "OneDrive" in carpeta:
                return os.path.join(base, carpeta, "SCRDA_Respaldos")
        
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
        carpeta_backup = self.__crear_carpeta_backup()
        respaldadas = {}

        hay_archivos = False

        carpeta_scrda = os.path.join(carpeta_backup, "SCRDA Excel")
        os.makedirs(carpeta_scrda, exist_ok=True)
        
        for nombre, ruta in fuentes.items():
            if not os.path.exists(ruta):
                print(f"[ADVERTENCIA] La ruta '{ruta}' no existe, se omite el respaldo.")
                continue
            
            if os.path.isdir(ruta):
                tiene_archivos = any(
                    f for _, _, files in os.walk(ruta)
                    for f in files
                    if not (f.startswith("~$") or f.startswith(".~lock"))
                )
                
                if not tiene_archivos:
                    print(f"[ADVERTENCIA] La carpeta '{ruta}' está vacía, se omite el respaldo.")
                    continue
                
                destino_base = os.path.join(carpeta_scrda, nombre)
                shutil.copytree(ruta, destino_base, dirs_exist_ok=True)
                count = 0
                for root, dirs, files in os.walk(destino_base):
                    for f in files:
                        if f.startswith("~$") or f.startswith(".~lock"):
                            continue
                        hay_archivos = True
                        count += 1
                respaldadas[nombre] = count
                
            elif os.path.isfile(ruta):
                # Copiar archivo
                destino_dir = os.path.join(carpeta_scrda, nombre)
                os.makedirs(destino_dir, exist_ok=True)
                destino = os.path.join(destino_dir, os.path.basename(ruta))
                shutil.copy2(ruta, destino)
                hay_archivos = True
                respaldadas[nombre] = 1
        
        if not hay_archivos:
            raise RuntimeError("Ninguna carpeta contenía archivos.")

        return carpeta_backup, respaldadas
    
    def descargar_de_onedrive_local(self, carpeta_backup, destino_local):
        origen = os.path.join(self.CARPETA_ONEDRIVE, carpeta_backup)
        
        if not os.path.exists(origen):
            raise RuntimeError(f"No se encontró {carpeta_backup} en OneDrive local")
        
        for root, dirs, files in os.walk(origen):
            rel_path = os.path.relpath(root, origen)
            destino_dir = os.path.join(destino_local, rel_path)
            os.makedirs(destino_dir, exist_ok=True)
            for f in files:
                shutil.copy2(os.path.join(root, f), os.path.join(destino_dir, f))
