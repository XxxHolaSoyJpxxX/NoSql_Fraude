# core/registro.py
from db.MongoDB.mongo import crear_usuario
from db.MongoDB.mongo import verificar_credenciales_usuario
from db.MongoDB.mongo import verificar_credenciales_admin
from core.admin import menu_admin
from db.MongoDB.mongo import actualizar_last_login
from core.usuario import menu_usuario
from datetime import datetime
import re

def validar_email(email):
    """
    Valida el formato del correo electrónico
    """
    patron = r'^[\w\.-]+@[\w\.-]+\.\w+$'$'
    return re.match(patron, email) is not None

def validar_password(password):
    """
    Valida que la contraseña cumpla con requisitos mínimos de seguridad
    """
    # Mínimo 8 caracteres, al menos una letra mayúscula, una minúscula y un número
    if len(password) < 8:
        return False, "La contraseña debe tener al menos 8 caracteres"
    
    if not re.search(r'[A-Z]', password):
        return False, "La contraseña debe tener al menos una letra mayúscula"
    
    if not re.search(r'[a-z]', password):
        return False, "La contraseña debe tener al menos una letra minúscula"
    
    if not re.search(r'[0-9]', password):
        return False, "La contraseña debe tener al menos un número"
    
    return True, "Contraseña válida"

def registrar_usuario():
    """
    Registra un nuevo usuario en el sistema
    """
    print("\n=== Registro de Usuario ===")
    usuario_id = input("ID de usuario (único): ")
    nombre = input("Nombre completo: ")
    
    # Validar email
    while True:
        email = input("Correo electrónico: ")
        if validar_email(email):
            break
        print("❌ Formato de correo electrónico inválido. Intenta nuevamente.")
    
    # Validar contraseña
    while True:
        password = input("Crea una contraseña (mín. 8 caracteres, incluir mayúsculas, minúsculas y números): ")
        valido, mensaje = validar_password(password)
        if valido:
            break
        print(f"❌ {mensaje}. Intenta nuevamente.")
    
    # Confirmar contraseña
    while True:
        confirmacion = input("Confirma tu contraseña: ")
        if confirmacion == password:
            break
        print("❌ Las contraseñas no coinciden. Intenta nuevamente.")

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
    """
    Iniciar sesión para usuarios regulares
    """
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
    """
    Registra un nuevo administrador en el sistema
    """
    print("\n=== Registro de Administrador ===")
    usuario_id = input("ID de administrador (único): ")
    nombre = input("Nombre completo: ")
    
    # Validar email
    while True:
        email = input("Correo electrónico: ")
        if validar_email(email):
            break
        print("❌ Formato de correo electrónico inválido. Intenta nuevamente.")
    
    # Validar contraseña (requisitos más estrictos para admin)
    while True:
        password = input("Crea una contraseña (mín. 10 caracteres, incluir mayúsculas, minúsculas, números y símbolos): ")
        if len(password) < 10:
            print("❌ La contraseña debe tener al menos 10 caracteres.")
            continue
        
        if not re.search(r'[A-Z]', password):
            print("❌ La contraseña debe tener al menos una letra mayúscula.")
            continue
        
        if not re.search(r'[a-z]', password):
            print("❌ La contraseña debe tener al menos una letra minúscula.")
            continue
        
        if not re.search(r'[0-9]', password):
            print("❌ La contraseña debe tener al menos un número.")
            continue
        
        if not re.search(r'[!@#$%^&*(),.?":{}|<>]', password):
            print("❌ La contraseña debe tener al menos un símbolo especial.")
            continue
        
        break
    
    # Confirmar contraseña
    while True:
        confirmacion = input("Confirma tu contraseña: ")
        if confirmacion == password:
            break
        print("❌ Las contraseñas no coinciden. Intenta nuevamente.")

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
    """
    Iniciar sesión para administradores
    """
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
