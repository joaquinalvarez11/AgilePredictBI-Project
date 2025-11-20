import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import threading
import re
import sys
import os

from proceso_etl import deteccion_auto
current_dir = os.path.dirname(__file__)
project_root = os.path.abspath(os.path.join(current_dir, '..'))
proceso_db_path = os.path.join(project_root, 'proceso_db')
sys.path.append(proceso_db_path)
import cargar_bd

class VistaETL(tk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller
        self.cancel_event = threading.Event()
        self.etl_thread = None
        
        # --- Estilos ---
        self.configure(bg="white")
        style = ttk.Style()
        style.configure("ETL.TLabel", background="white", font=("Helvetica", 10))
        style.configure("ETLTitle.TLabel", background="white", foreground="#004c8c", font=("Helvetica", 18, "bold"))

        # --- Layout ---
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(3, weight=1) 
        self.grid_rowconfigure(5, weight=0) 

        # Header
        header_frame = tk.Frame(self, bg="#f0f4f8", height=60) # Gris azulado muy claro
        header_frame.grid(row=0, column=0, sticky="ew", pady=(0, 20))
        tk.Label(header_frame, text="Sincronización de Datos Masiva", font=("Helvetica", 16, "bold"), bg="#f0f4f8", fg="#004c8c").pack(pady=15)

        # Info
        info_text = (
            "Este asistente automatizado ejecutará los procesos de normalización y carga.\n"
            "El sistema procesará archivos Excel y actualizará el repositorio central."
        )
        lbl_info = ttk.Label(self, text=info_text, justify="center", style="ETL.TLabel")
        lbl_info.grid(row=1, column=0, pady=10, padx=40)

        # Progreso
        self.lbl_progreso = tk.Label(self, text="Estado: En espera", font=("Helvetica", 10, "bold"), bg="white", fg="#00a2e8")
        self.lbl_progreso.grid(row=2, column=0, pady=(10, 5), padx=40, sticky="w")
        
        # Consola de Logs (Estilo Ejecutivo Técnico)
        frame_consola = tk.Frame(self, bg="#cccccc", bd=1)
        frame_consola.grid(row=3, column=0, pady=(5, 10), padx=40, sticky="nsew")
        
        self.txt_status = scrolledtext.ScrolledText(
            frame_consola, 
            wrap=tk.WORD, 
            height=15, 
            state='disabled', 
            font=("Consolas", 9),
            bg="#f9f9f9",
            fg="#333333",
            relief="flat"
        )
        self.txt_status.pack(fill="both", expand=True, padx=1, pady=1)

        # Botones
        button_frame = tk.Frame(self, bg="white")
        button_frame.grid(row=5, column=0, pady=(10, 30)) 
        
        self.btn_ejecutar = tk.Button(button_frame, text="Iniciar Sincronización", command=self.iniciar_proceso_en_thread, 
                                      bg="#004c8c", fg="white", font=("Helvetica", 10, "bold"), relief="flat", padx=15, pady=5)
        self.btn_ejecutar.pack(side=tk.LEFT, padx=10)
        
        self.btn_cancelar = tk.Button(button_frame, text="Cancelar", command=self.cancelar_proceso, state='disabled',
                                      bg="#d9534f", fg="white", font=("Helvetica", 10), relief="flat", padx=10, pady=5)
        self.btn_cancelar.pack(side=tk.LEFT, padx=10)
        
        btn_back = ttk.Button(button_frame, text="Volver al Menú", command=lambda: controller.show_menu_principal())
        btn_back.pack(side=tk.LEFT, padx=10)

    def progreso_callback(self, mensaje, progreso_actual=None, progreso_total=None, etapa=1):
        self.after(0, self._actualizar_gui_callback, mensaje, progreso_actual, progreso_total, etapa)

    def _actualizar_gui_callback(self, mensaje, progreso_actual, progreso_total, etapa):
        if mensaje and mensaje.strip(): 
            self.txt_status.config(state='normal')
            if self.txt_status.index('end-1c') == '1.0':
                 mensaje_a_insertar = mensaje.lstrip('\n')
            else:
                 mensaje_a_insertar = mensaje
            self.txt_status.insert(tk.END, mensaje_a_insertar + "\n")
            self.txt_status.see(tk.END)
            self.txt_status.config(state='disabled')

        if progreso_actual is not None and progreso_total is not None and progreso_total > 0:
            porcentaje_visual = 0.0
            ratio = progreso_actual / progreso_total
            
            if etapa == 1: 
                porcentaje_visual = ratio * 50.0
            elif etapa == 2: 
                porcentaje_visual = 50.0 + (ratio * 50.0)

            self.lbl_progreso.config(text=f"Progreso Global: {porcentaje_visual:.1f}%")
        
        self.update_idletasks()

    def iniciar_proceso_en_thread(self):
        self.btn_ejecutar.config(state='disabled', text="Procesando...", bg="#cccccc")
        self.btn_cancelar.config(state='normal', bg="#d9534f")
        self.lbl_progreso.config(text="Progreso: 0.0% (Iniciando...)") 
        self.txt_status.config(state='normal')
        self.txt_status.delete('1.0', tk.END)
        self.txt_status.config(state='disabled')
        self.cancel_event.clear()

        self.etl_thread = threading.Thread(target=self.ejecutar_proceso_completo, args=(self.progreso_callback, self.cancel_event))
        self.etl_thread.start()

    def cancelar_proceso(self):
        if self.etl_thread and self.etl_thread.is_alive():
            self.progreso_callback("\n*** Solicitando cancelación... ***")
            self.cancel_event.set()
            self.btn_cancelar.config(state='disabled', bg="#cccccc")

    def ejecutar_proceso_completo(self, callback_original, cancel_event_recibido):
        mensaje_resumen_final = "Proceso terminado inesperadamente."
        proceso_cancelado = False
        try:
            callback_para_etl_1 = lambda msg, curr=None, tot=None: callback_original(msg, curr, tot, etapa=1)
            callback_original("\n" + "="*30 + " INICIANDO FASE 1: EXCEL A CSV ...", etapa=1)
            
            resumen_etl1, _, proceso_cancelado = deteccion_auto.ejecutar_proceso_etl_completo(
                callback_progreso=callback_para_etl_1, 
                cancel_event=cancel_event_recibido
            )
            
            if proceso_cancelado:
                self.after(0, self.finalizar_proceso, "Proceso cancelado durante ETL 1.", True)
                return

            if "Errores: 0" not in resumen_etl1:
                callback_para_etl_1("\nADVERTENCIA: Se detectaron errores en la fase 1.")

            callback_para_etl_2 = lambda msg, curr=None, tot=None: callback_original(msg, curr, tot, etapa=2)

            callback_original("\n" + "="*30 + " INICIANDO FASE 2: CARGA A BASE DE DATOS ...", etapa=2)
            
            resumen_etl2 = cargar_bd.ejecutar_carga_db_completa(
                callback_progreso=callback_para_etl_2, 
                cancel_event=cancel_event_recibido
            )

            if cancel_event_recibido.is_set():
                self.after(0, self.finalizar_proceso, "Proceso cancelado durante ETL 2.", True)
                return

            mensaje_resumen_final = f"ETL 1: {resumen_etl1}\nETL 2: {resumen_etl2}"
            self.after(0, self.finalizar_proceso, mensaje_resumen_final, proceso_cancelado)
            
        except Exception as e:
            error_msg = f"Error no capturado:\n{type(e).__name__}: {e}"
            self.after(0, self.finalizar_proceso_con_error, error_msg)

    def finalizar_proceso(self, mensaje_resumen, cancelado):
        self.progreso_callback("\n" + "="*40 + " PROCESO FINALIZADO " + "="*40)
        
        if not cancelado:
            try:
                exitosos = int(re.search(r"- Éxito: (\d+)", mensaje_resumen).group(1))
                adv = int(re.search(r"- Adv\.: (\d+)", mensaje_resumen).group(1))
                errs = int(re.search(r"- Errores: (\d+)", mensaje_resumen).group(1))
                total = exitosos + adv + errs
                if total > 0:
                    self.lbl_progreso.config(text=f"Completado (100%)")
                else:
                    self.lbl_progreso.config(text="Completado (Sin datos nuevos)")
            except Exception:
                pass 
            self.update_idletasks()

        self.btn_ejecutar.config(state='normal', text="Iniciar Sincronización", bg="#004c8c")
        self.btn_cancelar.config(state='disabled', bg="#cccccc")

        if cancelado: messagebox.showwarning("Cancelado", "El proceso fue detenido por el usuario.")
        else:
             if mensaje_resumen and "Errores: 0" in mensaje_resumen:
                 messagebox.showinfo("Éxito", "Sincronización completada correctamente.")
             else:
                 messagebox.showwarning("Atención", "Proceso finalizado con advertencias.")

    def finalizar_proceso_con_error(self, error_msg):
        self.progreso_callback("\n--- ERROR CRÍTICO ---")
        self.progreso_callback(error_msg)
        self.update_idletasks()
        self.btn_ejecutar.config(state='normal', text="Reintentar", bg="#004c8c")
        self.btn_cancelar.config(state='disabled')
        messagebox.showerror("Error", "Fallo crítico del sistema.")