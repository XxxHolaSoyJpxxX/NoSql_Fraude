from db.MongoDB import crear_usuario
from db.MongoDB import verificar_credenciales_usuario
from db.MongoDB import verificar_credenciales_admin
from core.admin import menu_admin
from db.MongoDB import actualizar_last_login
from usuario import menu_usuario
from datetime import datetime

def registrar_usuario():
    print("\n=== Registro de Usuario ===")
    usuario_id = input("ID de usuario (único): ")
    nombre = input("Nombre completo: ")
    email = input("Correo electrónico: ")
    password = input("Crea una contraseña: ")

    usuario = {
        "usuario_id": usuario_id,
        "nombre": nombre,
        "email": email,
        "password": password, 
        "authentication_methods": ["email", "sms"],
        "registered_at": datetime.now().isoformat(),
        "last_login": None,
        "riesgo": {
            "risk_level": "medio"
        }
    }

    if crear_usuario(usuario):
        print("Usuario registrado correctamente.")
    else:
        print("Error: El usuario ya existe.")

def login_usuario():
    print("\n=== Iniciar sesión ===")
    email = input("Correo electrónico: ")
    password = input("Contraseña: ")

    usuario = verificar_credenciales_usuario(email, password)

    if usuario:
        print(f"Bienvenido {usuario['nombre']} (ID: {usuario['usuario_id']})")
        actualizar_last_login(usuario["usuario_id"])
        menu_usuario(usuario["usuario_id"])
    else:
        print("Credenciales incorrectas.")


def registrar_admin():
    print("\n=== Registro de Administrador ===")
    usuario_id = input("ID de administrador (único): ")
    nombre = input("Nombre completo: ")
    email = input("Correo electrónico: ")
    password = input("Crea una contraseña: ")

    admin = {
        "usuario_id": usuario_id,
        "nombre": nombre,
        "email": email,
        "password": password,
        "es_admin": True,
        "authentication_methods": ["email"],
        "registered_at": datetime.now().isoformat(),
        "last_login": None
    }

    if crear_usuario(admin):
        print("Administrador registrado correctamente.")
    else:
        print("Error: Ya existe un usuario con ese ID.")

def login_admin():
    print("\n=== Iniciar sesión como Administrador ===")
    email = input("Correo electrónico: ")
    password = input("Contraseña: ")

    admin = verificar_credenciales_admin(email, password)

    if admin:
        print(f"Acceso autorizado. Bienvenido, {admin['nombre']} (ID: {admin['usuario_id']})")
        actualizar_last_login(admin["usuario_id"])
        menu_admin()
    else:
        print("Acceso denegado. Verifica tus credenciales.")