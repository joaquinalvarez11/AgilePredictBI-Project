import tkinter as tk
from views.vista1_bienvenida import VistaBienvenida
from views.vista2_cargar_csv import VistaCargarCSV
from views.vista3_resultados import VistaResultados
from views.vista4_exportar import VistaExportar

class AgilePredictApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Asistente Predictivo - Ruta del Algarrobo")
        self.geometry("1200x700")
        self.resizable(False, False)

        self.export_path = None
        self.traffic_df = None  # Para datos de tráfico
        self.accidents_df = None # Para datos de siniestralidad
        self.prediction_results_df = None # Para almacenar el DataFrame con los resultados de la predicción
        self.predicted_plot_fig = None # Para almacenar la figura del plot de predicción

        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(0, weight=1)

        self.pages = [
            VistaBienvenida,
            VistaCargarCSV,
            VistaResultados,
            VistaExportar
        ]
        self.frames = []
        self.current = 0
        self._load_pages()
        self._show_page(0)

    def _load_pages(self):
        for Page in self.pages:
            frame = Page(self)
            frame.grid(row=0, column=0, sticky="nsew", padx=20, pady=20)
            self.frames.append(frame)

    def _show_page(self, index):
        for frame in self.frames:
            frame.grid_remove()
        self.frames[index].grid()
        self.current = index

    def next_page(self):
        if self.current + 1 < len(self.frames):
            self._show_page(self.current + 1)

    def prev_page(self):
        if self.current - 1 >= 0:
            self._show_page(self.current - 1)

if __name__ == "__main__":
    app = AgilePredictApp()
    app.mainloop()