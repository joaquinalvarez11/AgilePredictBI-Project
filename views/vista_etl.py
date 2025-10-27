import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
from proceso_etl import deteccion_auto
import threading

class VistaETL(tk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller
        self.cancel_event = threading.Event()
        self.etl_thread = None

        # --- LAYOUT ---
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(3, weight=1) # Fila para el área de texto expandible
        self.grid_rowconfigure(5, weight=0) # Fila botones inferiores

        # --- Título, Info, Barra Progreso, Área Texto, Botones ---
        lbl_title = ttk.Label(self, text="Proceso de Transformación ETL Masivo", font=("Arial", 18, "bold"))
        lbl_title.grid(row=0, column=0, pady=(40, 10))
        info_text = (
            "Este proceso buscará y transformará TODOS los archivos Excel pendientes en:\n"
            "Tráfico Mensual, Siniestralidad/Ficha 0 y Siniestralidad/Ficha 1.\n"
            "Haga clic en Iniciar (Puede tardar)."
        )
        lbl_info = ttk.Label(self, text=info_text, justify="center")
        lbl_info.grid(row=1, column=0, pady=10, padx=40)

        # --- Label para el progreso en texto ---
        self.lbl_progreso = ttk.Label(self, text="Progreso: 0/0 (0.0%)", font=("Courier New", 10))
        self.lbl_progreso.grid(row=2, column=0, pady=(10, 5), padx=40, sticky="ew")
        # Área de texto para logs
        self.txt_status = scrolledtext.ScrolledText(self, wrap=tk.WORD, height=15, state='disabled', font=("Courier New", 9))
        self.txt_status.grid(row=3, column=0, pady=(5, 10), padx=40, sticky="nsew")
        # Frame para botones inferiores
        button_frame = ttk.Frame(self)
        button_frame.grid(row=5, column=0, pady=(10, 20)) # Usar fila 5
        # Botón Iniciar
        self.btn_ejecutar = ttk.Button(button_frame, text="Iniciar Proceso ETL", command=self.iniciar_proceso_en_thread)
        self.btn_ejecutar.pack(side=tk.LEFT, padx=10, ipady=5, ipadx=10)
        # Botón Cancelar
        self.btn_cancelar = ttk.Button(button_frame, text="Cancelar Proceso", command=self.cancelar_proceso, state='disabled')
        self.btn_cancelar.pack(side=tk.LEFT, padx=10, ipady=5, ipadx=10)
        # Botón Volver
        btn_back = ttk.Button(button_frame, text="Volver al Menú", command=lambda: controller.show_menu_principal())
        btn_back.pack(side=tk.LEFT, padx=10, ipady=5, ipadx=10)

    def progreso_callback(self, mensaje, progreso_actual=None, progreso_total=None):
        """Callback llamado desde la lógica ETL."""
        # Actualizar GUI de forma segura
        self.after(0, self._actualizar_gui_callback, mensaje, progreso_actual, progreso_total)

    def _actualizar_gui_callback(self, mensaje, progreso_actual, progreso_total):
        """Actualiza el texto y el label de progreso (corre en hilo principal)."""
        # --- Mostrar TODOS los mensajes en tiempo real ---
        self.txt_status.config(state='normal')
        if self.txt_status.index('end-1c') == '1.0': # Si el Text está vacío
             mensaje_a_insertar = mensaje.lstrip('\n')
        else:
             mensaje_a_insertar = mensaje
        self.txt_status.insert(tk.END, mensaje_a_insertar + "\n")
        self.txt_status.see(tk.END) # Auto-scroll
        self.txt_status.config(state='disabled')

        # --- Actualizar LABEL de progreso ---
        if progreso_actual is not None and progreso_total is not None and progreso_total > 0:
            porcentaje = (progreso_actual / progreso_total) * 100
            progreso_texto = f"Progreso: {progreso_actual} / {progreso_total} ({porcentaje:.1f}%)"
            self.lbl_progreso.config(text=progreso_texto)
        elif progreso_total is not None and progreso_actual is not None and progreso_actual >= progreso_total: # Completado
             porcentaje = 100.0
             progreso_texto = f"Progreso: {progreso_actual} / {progreso_total} ({porcentaje:.1f}%)"
             self.lbl_progreso.config(text=progreso_texto)
        
        self.update_idletasks() # Forzar actualización visual

    def iniciar_proceso_en_thread(self):
        """Inicia el ETL en un hilo separado."""
        self.btn_ejecutar.config(state='disabled', text="Procesando...")
        self.btn_cancelar.config(state='normal')
        self.lbl_progreso.config(text="Progreso: 0/0 (0.0%)") # Reiniciar label
        self.txt_status.config(state='normal')
        self.txt_status.delete('1.0', tk.END)
        self.txt_status.config(state='disabled')
        self.cancel_event.clear()

        self.etl_thread = threading.Thread(target=self.ejecutar_proceso_completo, args=(self.progreso_callback, self.cancel_event))
        self.etl_thread.start()

    def cancelar_proceso(self):
        """Señaliza al hilo ETL que debe detenerse."""
        if self.etl_thread and self.etl_thread.is_alive():
            self.progreso_callback("\n*** Solicitando cancelación... Esperando finalización del archivo actual... ***")
            self.cancel_event.set()
            self.btn_cancelar.config(state='disabled')

    def ejecutar_proceso_completo(self, callback_para_etl, cancel_event_recibido):
        """Lógica del ETL que se ejecuta en el thread."""
        mensaje_resumen_final = "Proceso terminado inesperadamente."
        proceso_cancelado = False
        try:
            mensaje_resumen_final, _, proceso_cancelado = deteccion_auto.ejecutar_proceso_etl_completo(
                callback_progreso=callback_para_etl,
                cancel_event=cancel_event_recibido
            )
            self.after(0, self.finalizar_proceso, mensaje_resumen_final, proceso_cancelado)
        except Exception as e:
            error_msg = f"Error no capturado en ejecución:\n{type(e).__name__}: {e}"
            self.after(0, self.finalizar_proceso_con_error, error_msg)

    def finalizar_proceso(self, mensaje_resumen, cancelado):
        """Actualiza la GUI al finalizar."""
        # El resumen ya se muestra en tiempo real, solo añadimos separador
        self.progreso_callback("\n" + "="*40 + " PROCESO FINALIZADO " + "="*40)
        # self.progreso_callback(mensaje_resumen) # Opcional: mostrar resumen de nuevo

        # Asegurar progreso 100% si no fue cancelado
        if not cancelado:
            # Forzar actualización final del label de progreso si es necesario
            try:
                # Extraer números del resumen para la barra final
                exitosos = int(re.search(r"- Éxito: (\d+)", mensaje_resumen).group(1))
                adv = int(re.search(r"- Adv\.: (\d+)", mensaje_resumen).group(1))
                errs = int(re.search(r"- Errores: (\d+)", mensaje_resumen).group(1))
                total = exitosos + adv + errs
                if total > 0:
                    self.lbl_progreso.config(text=f"Progreso: {total} / {total} (100.0%)")
                else:
                    self.lbl_progreso.config(text="Progreso: 0 / 0 (Completado)")
            except Exception:
                pass # No fallar si el regex no funciona
            self.update_idletasks()

        # Botones
        self.btn_ejecutar.config(state='normal', text="Iniciar Proceso ETL")
        self.btn_cancelar.config(state='disabled')

        # Popup
        if cancelado: messagebox.showwarning("Proceso Cancelado", "El proceso ETL fue cancelado.")
        else:
             if mensaje_resumen and "Errores: 0" in mensaje_resumen:
                 messagebox.showinfo("Proceso Completado", "El proceso ETL ha finalizado sin errores.")
             elif mensaje_resumen:
                 messagebox.showwarning("Proceso Completado", "El proceso ETL finalizó con advertencias o errores.")
             else:
                 messagebox.showinfo("Proceso Finalizado", "El proceso ETL ha terminado.")

    def finalizar_proceso_con_error(self, error_msg):
        """Actualiza la GUI al finalizar con error inesperado."""
        self.progreso_callback("\n--- ERROR CRÍTICO INESPERADO ---")
        self.progreso_callback(error_msg)
        self.update_idletasks()
        self.btn_ejecutar.config(state='normal', text="Iniciar Proceso ETL")
        self.btn_cancelar.config(state='disabled')
        messagebox.showerror("Error Crítico", "El proceso ETL falló inesperadamente. Revise los detalles.")