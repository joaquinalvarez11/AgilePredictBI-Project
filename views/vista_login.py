import tkinter as tk
from tkinter import ttk, messagebox

class VistaLogin(tk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller
        self.configure(bg="white")

        # --- CREDENCIALES ---
        # Puedes agregar más usuarios aquí
        # TODO: Implementar base de datos para gestión de usuarios en Fase 2
        self.USUARIOS_VALIDOS = {
            "karina": "karina2025",
            "susana": "susana2025",
            "luis": "luis2025",
            "steffanie": "steffanie2025",
            "gerencia": "ruta2025",
            "admin": "admin123",
            "tesis": "1234",
            "algarrobo": "algarrobo",
            "rda": "rda",
            "rda2025": "rda2025",
            "hans": "hans",
            "cristofer": "cristofer",
            "joaquin": "joaquin"
        }

        # --- Estilos ---
        style = ttk.Style()
        style.configure("LoginTitle.TLabel", background="white", foreground="#004c8c", font=("Helvetica", 16, "bold"))
        style.configure("LoginLabel.TLabel", background="white", foreground="#555", font=("Helvetica", 10))

        # --- Interfaz ---
        # Frame central para centrar el formulario
        center_frame = tk.Frame(self, bg="white")
        center_frame.place(relx=0.5, rely=0.5, anchor="center")

        # Título
        ttk.Label(center_frame, text="Iniciar Sesión", style="LoginTitle.TLabel").pack(pady=(0, 20))

        # Usuario
        ttk.Label(center_frame, text="Usuario:", style="LoginLabel.TLabel").pack(anchor="w")
        self.user_var = tk.StringVar()
        self.entry_user = ttk.Entry(center_frame, textvariable=self.user_var, width=30)
        self.entry_user.pack(pady=(0, 10))
        self.entry_user.focus() # Poner el foco aquí al iniciar

        # Contraseña
        ttk.Label(center_frame, text="Contraseña:", style="LoginLabel.TLabel").pack(anchor="w")
        self.pass_var = tk.StringVar()
        self.entry_pass = ttk.Entry(center_frame, textvariable=self.pass_var, width=30, show="*") # show="*" oculta el texto
        self.entry_pass.pack(pady=(0, 20))

        # Botón Ingresar
        btn_login = ttk.Button(center_frame, text="Ingresar", command=self.validar_login)
        btn_login.pack(fill="x", ipady=5)

        # Botón Volver (opcional, por si quieren regresar a la bienvenida)
        btn_back = ttk.Button(center_frame, text="Volver", command=lambda: controller.show_frame(controller.frames[list(controller.frames.keys())[0]].__class__))
        # Nota: El comando anterior es un truco para volver a la primera vista, o puedes usar un método explícito.
        # Simplifiquemos:
        btn_back = ttk.Button(center_frame, text="Cancelar", command=lambda: controller.show_frame_by_name("VistaBienvenida"))
        btn_back.pack(fill="x", pady=5)

        # Permitir login con la tecla ENTER
        self.entry_pass.bind('<Return>', lambda event: self.validar_login())

    def validar_login(self):
        usuario = self.user_var.get().strip()
        password = self.pass_var.get().strip()

        if usuario in self.USUARIOS_VALIDOS and self.USUARIOS_VALIDOS[usuario] == password:
            # Login Exitoso
            self.limpiar_campos()
            self.controller.show_menu_principal()
        else:
            messagebox.showerror("Error de Acceso", "Usuario o contraseña incorrectos.\nPor favor intente nuevamente.")
            self.pass_var.set("")

    def limpiar_campos(self):
        self.user_var.set("")
        self.pass_var.set("")