import os
import ctypes
import platform

def set_file_hidden(filepath):
    """
    Oculta un archivo en Windows usando la API del kernel32.
    """
    if platform.system() == "Windows":
        try:
            # 0x02 es el atributo de archivo oculto en Windows
            FILE_ATTRIBUTE_HIDDEN = 0x02
            ret = ctypes.windll.kernel32.SetFileAttributesW(filepath, FILE_ATTRIBUTE_HIDDEN)
            if not ret:
                print(f"No se pudo ocultar el archivo: {filepath}")
        except Exception as e:
            print(f"Error al intentar ocultar archivo: {e}")

def remove_file_force(filepath):
    """
    Intenta eliminar un archivo incluso si está oculto.
    """
    if os.path.exists(filepath):
        try:
            # En Windows, a veces es mejor quitar el atributo oculto antes de borrar
            # 0x80 es FILE_ATTRIBUTE_NORMAL
            if platform.system() == "Windows":
                ctypes.windll.kernel32.SetFileAttributesW(filepath, 0x80)
            
            os.remove(filepath)
            return True, "Archivo eliminado correctamente."
        except Exception as e:
            return False, f"Error al eliminar: {e}"
    else:
        return False, "El archivo no existe."