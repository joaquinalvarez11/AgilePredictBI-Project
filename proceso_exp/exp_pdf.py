import os
import time
import shutil
import tkinter as tk
from tkinter import messagebox
from tkinter import ttk
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from PyPDF2 import PdfReader, PdfWriter

CARPETA_ORIGEN = r"C:\Users\hriar\AppData\Local\Temp\Power BI Desktop"
CARPETA_DESTINO = r"C:\Users\hriar\Desktop\Test Exportar"

def mostrar_formulario(paginas_max):
    datos = {"tipo": None, "anio": "", "mes": "", "paginas": []}

    root = tk.Tk()
    root.title("Formulario de Información")
    root.geometry("250x150")
    root.eval('tk::PlaceWindow . center')  # Centrar

    frame = tk.Frame(root)
    frame.pack(expand=True, fill='both', padx=20, pady=20)

    estado = {"paso": 0}

    def mostrar_paso():
        for widget in frame.winfo_children():
            widget.destroy()

        paso = estado["paso"]

        if paso == 0:
            tk.Label(frame, text="Seleccione tipo de informe:").pack(pady=10)
            ttk.Button(frame, text="Tráfico", command=lambda: avanzar("Tráfico")).pack(pady=5)
            ttk.Button(frame, text="Siniestro", command=lambda: avanzar("Siniestro")).pack(pady=5)

        elif paso == 1:
            tk.Label(frame, text="Ingrese el año (ej: 2022):").pack(pady=10)
            anio_entry = ttk.Entry(frame)
            anio_entry.pack()
            anio_entry.insert(0, datos["anio"])
            ttk.Button(frame, text="Atrás", command=retroceder).pack(side='left', padx=10, pady=20)
            ttk.Button(frame, text="Siguiente", command=lambda: avanzar_anio(anio_entry)).pack(side='right', padx=10)

        elif paso == 2:
            tk.Label(frame, text="Ingrese el mes (ej: Febrero):").pack(pady=10)
            mes_entry = ttk.Entry(frame)
            mes_entry.pack()
            mes_entry.insert(0, datos["mes"])
            ttk.Button(frame, text="Atrás", command=retroceder).pack(side='left', padx=10, pady=20)
            ttk.Button(frame, text="Siguiente", command=lambda: avanzar_mes(mes_entry)).pack(side='right', padx=10)

        elif paso == 3:
            tk.Label(frame, text=f"Ingrese páginas (1-{paginas_max})\nEj: 1-3,5,7").pack(pady=10)
            paginas_entry = ttk.Entry(frame)
            paginas_entry.pack()
            ttk.Button(frame, text="Atrás", command=retroceder).pack(side='left', padx=10, pady=20)
            ttk.Button(frame, text="Finalizar", command=lambda: finalizar(paginas_entry)).pack(side='right', padx=10)

    def avanzar(valor=None):
        if estado["paso"] == 0:
            datos["tipo"] = valor
        estado["paso"] += 1
        mostrar_paso()

    def retroceder():
        if estado["paso"] > 0:
            estado["paso"] -= 1
        mostrar_paso()

    def avanzar_anio(entry):
        anio = entry.get()
        if anio.isdigit():
            datos["anio"] = anio
            estado["paso"] += 1
            mostrar_paso()
        else:
            messagebox.showerror("Error", "Año inválido.")

    def avanzar_mes(entry):
        mes = entry.get()
        if mes.isalpha():
            datos["mes"] = mes.capitalize()
            estado["paso"] += 1
            mostrar_paso()
        else:
            messagebox.showerror("Error", "Mes inválido.")

    def finalizar(entry):
        try:
            paginas = []
            for parte in entry.get().split(","):
                parte = parte.strip()
                if "-" in parte:
                    inicio, fin = map(int, parte.split("-"))
                    paginas.extend(range(inicio, fin + 1))
                else:
                    paginas.append(int(parte))
            paginas = sorted(set(p for p in paginas if 1 <= p <= paginas_max))
            datos["paginas"] = paginas
        except:
            messagebox.showerror("Error", "Formato de páginas inválido.")
            return

        nombre_final = f"{datos['tipo']}_{datos['anio']}_{datos['mes']}.pdf"
        confirmar = messagebox.askyesno("Confirmar nombre", f"¿Desea usar este nombre?\n\n{nombre_final}")
        if confirmar:
            datos["nombre_final"] = nombre_final
            root.destroy()
        else:
            estado["paso"] = 1
            mostrar_paso()

    mostrar_paso()
    root.mainloop()

    if "nombre_final" in datos:
        return datos["nombre_final"], datos["paginas"]
    return None, None

def limpiar_pdf(ruta_pdf, paginas_deseadas):
    reader = PdfReader(ruta_pdf)
    writer = PdfWriter()
    for i in paginas_deseadas:
        if 1 <= i <= len(reader.pages):
            writer.add_page(reader.pages[i - 1])
    limpio_path = ruta_pdf.replace(".pdf", "_Dashboard.pdf")
    with open(limpio_path, "wb") as f:
        writer.write(f)
    return limpio_path

class PDFHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            time.sleep(20)
            posibles_pdfs = [f for f in os.listdir(event.src_path) if f.lower().endswith(".pdf")]
            if not posibles_pdfs:
                return
            pdf_path = os.path.join(event.src_path, posibles_pdfs[0])
            reader = PdfReader(pdf_path)
            total_paginas = len(reader.pages)

            nuevo_nombre, paginas = mostrar_formulario(total_paginas)
            if not nuevo_nombre or not paginas:
                print("Operación cancelada.")
                os._exit(0)

            nuevo_path = os.path.join(CARPETA_DESTINO, nuevo_nombre)
            shutil.move(pdf_path, nuevo_path)
            limpio_path = limpiar_pdf(nuevo_path, paginas)
            os.remove(nuevo_path)
            os.rename(limpio_path, nuevo_path)

            messagebox.showinfo("Listo", f"PDF final guardado como:\n{nuevo_nombre}")
            os._exit(0)

if __name__ == "__main__":
    print(f"Monitoreando: {CARPETA_ORIGEN}")
    os.makedirs(CARPETA_DESTINO, exist_ok=True)
    observer = Observer()
    observer.schedule(PDFHandler(), path=CARPETA_ORIGEN, recursive=False)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()