import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
from proceso_etl import deteccion_auto
import threading

class VistaETL(tk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller
        
        # Configuración del Layout
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(2, weight=1)
        
        # Widgets
        lbl_title = ttk.Label(self, text="Proceso de Transformación ETL Automático", font=("Arial", 18, "bold"))
        lbl_title.grid(row=0, column=0, pady=(40, 10))

        info_text = (
            "Este proceso buscará archivos Excel en las carpetas estructuradas\n"
            "dentro de los datos brutos. Identificará y transformará todos los archivos\n"
            "que aún no tengan un archivo CSV limpio correspondiente.\n"
            "Haga clic en 'Iniciar' para comenzar (puede tardar varios minutos)."
        )
        lbl_info = ttk.Label(self, text=info_text, justify="center")
        lbl_info.grid(row=1, column=0, pady=10, padx=40) # Usar grid

        # Botón para ejecutar
        self.btn_ejecutar = ttk.Button(self, text="Iniciar Proceso ETL", command=self.iniciar_proceso_en_thread)
        self.btn_ejecutar.grid(row=3, column=0, pady=20, ipady=10, ipadx=20) # Mover botón abajo

        # Área de texto para mostrar el estado y resultado
        self.txt_status = scrolledtext.ScrolledText(self, wrap=tk.WORD, height=15, state='disabled') # state='disabled' para hacerlo solo lectura
        self.txt_status.grid(row=2, column=0, pady=10, padx=40, sticky="nsew") # Hacer que se expanda

        # Botón para volver al menú principal
        btn_back = ttk.Button(self, text="Volver al Menú", command=lambda: controller.show_menu_principal())
        btn_back.grid(row=4, column=0, pady=(20, 20))

    def actualizar_status(self, mensaje):
        """Método seguro para actualizar el área de texto desde threads."""
        self.txt_status.config(state='normal')
        self.txt_status.insert(tk.END, mensaje + "\n")
        self.txt_status.see(tk.END)
        self.txt_status.config(state='disabled')
        self.update_idletasks()

    def iniciar_proceso_en_thread(self):
        """Inicia la ejecución del ETL en un hilo separado para no bloquear la GUI."""
        self.btn_ejecutar.config(state='disabled', text="Procesando...")
        self.txt_status.config(state='normal')
        self.txt_status.delete('1.0', tk.END) # Limpiar área de texto
        self.txt_status.config(state='disabled')
        self.actualizar_status("Iniciando proceso ETL en segundo plano...")

        # Crear y empezar el hilo
        thread = threading.Thread(target=self.ejecutar_proceso_completo)
        thread.start()

    def ejecutar_proceso_completo(self):
        """Lógica del ETL que se ejecuta en el thread."""
        try:
            # Llamada a la lógica centralizada que ahora procesa todo
            mensaje_final, _ = deteccion_auto.ejecutar_proceso_etl_completo()

            # Usar 'after' para asegurar que la actualización de la GUI ocurra en el hilo principal
            self.after(0, self.finalizar_proceso, mensaje_final)

        except Exception as e:
            error_msg = f"Ha ocurrido un error inesperado:\n{e}"
            # Actualizar GUI en el hilo principal
            self.after(0, self.finalizar_proceso_con_error, error_msg)

    def finalizar_proceso(self, mensaje):
        """Actualiza la GUI cuando el proceso termina exitosamente."""
        self.actualizar_status("\n--- PROCESO FINALIZADO ---")
        self.actualizar_status(mensaje) # Muestra el resumen final
        self.btn_ejecutar.config(state='normal', text="Iniciar Proceso ETL") # Rehabilitar botón
        messagebox.showinfo("Proceso Completado", "El proceso ETL ha finalizado. Revise el área de estado para ver los detalles.")

    def finalizar_proceso_con_error(self, error_msg):
        """Actualiza la GUI cuando el proceso termina con un error."""
        self.actualizar_status("\n--- ERROR CRÍTICO ---")
        self.actualizar_status(error_msg)
        self.btn_ejecutar.config(state='normal', text="Iniciar Proceso ETL") # Rehabilitar botón
        messagebox.showerror("Error Crítico", "El proceso ETL falló. Revise el área de estado.")