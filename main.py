import tkinter as tk
from views.vista1_bienvenida import VistaBienvenida
from views.vista3_resultados import VistaResultados
from views.vista4_exportar import VistaExportar

from views.vista2_menu_principal import VistaMenuPrincipal
from views.vista_etl import VistaETL
# from views.vista_ml import VistaML 

class AgilePredictApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("AgilePredictBI - Asistente Predictivo")
        self.geometry("1200x700")
        self.resizable(False, False)

        # Contenedor principal para todas las vistas
        container = tk.Frame(self)
        container.pack(side="top", fill="both", expand=True)
        container.grid_rowconfigure(0, weight=1)
        container.grid_columnconfigure(0, weight=1)

        # Diccionario de frames
        # Permite cambiar de vista por su nombre de clase
        self.frames = {}

        # Lista de las clases de vistas a usar
        list_of_pages = [
            VistaBienvenida, 
            VistaMenuPrincipal, 
            VistaETL,
            VistaResultados,
            VistaExportar
        ]

        for F in list_of_pages:
            frame = F(container, self)
            self.frames[F] = frame
            frame.grid(row=0, column=0, sticky="nsew")

        self.show_frame(VistaBienvenida)

    def show_frame(self, cont):
        """Muestra un frame específico basado en su clase."""
        frame = self.frames[cont]
        frame.tkraise() # Trae el frame seleccionado al frente

    # Métodos de navegación llamados desde los botones de las vistas
    def show_menu_principal(self):
        self.show_frame(VistaMenuPrincipal)

    def show_etl_view(self):
        self.show_frame(VistaETL)

    def show_ml_view(self):
        self.show_frame(VistaResultados)
        print("Navegando a la vista de Análisis Predictivo...")

    def show_export_view(self):
        self.show_frame(VistaExportar)
        print("Navegando a la vista de Exportación...")

if __name__ == "__main__":
    app = AgilePredictApp()
    app.mainloop()