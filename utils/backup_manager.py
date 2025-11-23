import os
import zipfile
import requests
import msal # Microsoft Authentication Library (para OneDrive)
from cryptography.fernet import Fernet
from config_manager import obtener_ruta

class BackupManager():
    # TODO: Revisar cómo incluir la ID sin generar riesgos de seguridad ‼️‼️
    CLIENT_ID = ""
    AUTHORITY = f"https://login.microsoftonline.com/organizations"
    SCOPES = ["Files.ReadWrite.All", "User.Read"]
    CARPETA_DESTINO = "SCRDA_Respaldos"

    KEY_FILE = os.path.join(os.getenv("APPDATA"), "SCRDA", "backup.key")
    
    if os.path.exists(KEY_FILE):
        with open(KEY_FILE, "rb") as f:
            KEY = f.read()
    else:
        KEY = Fernet.generate_key()
        os.makedirs(os.path.dirname(KEY_FILE), exist_ok=True)
        with open(KEY_FILE, "wb") as f:
            f.write(KEY)
    
    FERNET = Fernet(KEY)
    
    # Método para respaldar
    def respaldar(self):
        archivos = self.__listar_archivos()
        zip_path = self.__crear_zip(archivos)
        enc_path = self.__cifrar_zip(zip_path)
        token = self.__obtener_token()
        self.__subir_a_onedrive(enc_path, token)
    
    # Método para recuperar
    def recuperar(self, nombre_backup="backup.zip.enc", destino="recuperado"):
        token = self.__obtener_token()
        enc_path = self.__descargar_de_onedrive(nombre_backup, token)
        zip_path = self.__descifrar_zip(enc_path)
        self.__extraer_zip(zip_path, destino)

    # Métodos privados
    def __listar_archivos(self):
        archivos = []

        # Archivos a respaldar
        carpeta_csv = obtener_ruta("ruta_csv_limpio")
        carpeta_prediccion = obtener_ruta("ruta_predicciones")
        carpeta_db = obtener_ruta("ruta_database")

        # Recorrer los CSV limpios
        for root, dirs, files in os.walk(carpeta_csv):
            for f in files:
                if f.endswith(".csv"):
                    archivos.append(os.path.join(root, f))
        
        # Recorrer las predicciones
        for f in os.listdir(carpeta_prediccion):
            if f.endswith(".csv") or f.endswith(".png"):
                archivos.append(os.path.join(carpeta_prediccion, f))
        
        # Base de datos
        if os.path.exists(carpeta_db):
            archivos.append(carpeta_db)
        
        return archivos
    
    def __crear_zip(self, archivos, zip_path="backup.zip"):
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            for path in archivos:
                zipf.write(path, arcname=os.path.basename(path))
        
        return zip_path
    
    def __cifrar_zip(self, zip_path):
        with open(zip_path, "rb") as f:
            data = f.read()
        encrypted = self.FERNET.encrypt(data)
        enc_path = zip_path + ".enc"
        with open(enc_path, "wb") as f:
            f.write(encrypted)
        
        return enc_path
    
    def __descifrar_zip(self, enc_path):
        with open(enc_path, "rb") as f:
            encrypted = f.read()
        data = self.FERNET.decrypt(encrypted)
        zip_path = enc_path.replace(".enc", "")
        with open(zip_path, "wb") as f:
            f.write(data)
        
        return zip_path
    
    def __extraer_zip(self, zip_path, destino):
        os.makedirs(destino, exist_ok=True)
        with zipfile.ZipFile(zip_path, "r") as zipf:
            zipf.extractall(destino)
    
    def __obtener_token(self):
        app = msal.PublicClientApplication(self.CLIENT_ID, authority=self.AUTHORITY)
        accounts = app.get_accounts()
        if accounts:
            result = app.acquire_token_silent(self.SCOPES, account=accounts[0])
        else:
            result = app.acquire_token_interactive(scopes=self.SCOPES)
        
        if "access_token" not in result:
            raise RuntimeError("No se pudo autenticar en OneDrive.")
        
        return result["access_token"]
    
    def __subir_a_onedrive(self, enc_path, token):
        nombre = os.path.basename(enc_path)
        url = f"https://graph.microsoft.com/v1.0/me/drive/root:/{self.CARPETA_DESTINO}/{nombre}:/content"
        with open(enc_path, "rb") as f:
            respuesta = requests.put(url, headers={"Authorization": f"Bearer {token}"}, data=f)
        if respuesta.status_code not in (200, 201):
            raise RuntimeError(f"Error al subir {nombre}: {respuesta.text}")
    
    def __descargar_de_onedrive(self, nombre_backup, token):
        url = f"https://graph.microsoft.com/v1.0/me/drive/root:/{self.CARPETA_DESTINO}/{nombre_backup}:/content"
        respuesta = requests.get(url, headers={"Authorization": f"Bearer {token}"})

        if respuesta.status_code != 200:
            raise RuntimeError(f"Error al descargar {nombre_backup}: {respuesta.text}")
        
        local_path = nombre_backup
        with open(local_path, "wb") as f:
            f.write(respuesta.content)
        
        return local_path
