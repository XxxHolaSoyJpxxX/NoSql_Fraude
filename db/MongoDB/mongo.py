from pymongo import MongoClient
from datetime import datetime
import uuid

from db.Dgraph.dgraph import obtener_cuenta_completa, obtener_cuenta_por_email, obtener_uid_cuenta


def get_mongo_db():
    """
    Connect to MongoDB and return database instance
    """
    client = MongoClient("mongodb://localhost:27017")
    return client["banco"]

# ─────────────────────────────
# FUNCIONES DE GESTIÓN DE USUARIOS
# ─────────────────────────────

def crear_usuario(usuario):
    """
    Create a new user in the database
    Returns True if successful, False if user already exists
    """
    db = get_mongo_db()
    if db.usuarios.find_one({"usuario_id": usuario["usuario_id"]}):
        return False
    db.usuarios.insert_one(usuario)
    return True

def usuario_existe(usuario_id):
    """
    Check if a user exists in the database
    """
    db = get_mongo_db()
    return db.usuarios.find_one({"usuario_id": usuario_id}) is not None

def verificar_credenciales_usuario(email, password):
    """
    Verify user credentials
    Returns user object if credentials are valid, None otherwise
    """
    db = get_mongo_db()
    return db.usuarios.find_one({
        "email": email,
        "password": password
    })

def verificar_credenciales_admin(email, password):
    """
    Verify admin credentials
    Returns admin object if credentials are valid, None otherwise
    """
    db = get_mongo_db()
    return db.usuarios.find_one({
        "email": email,
        "password": password,
        "es_admin": True
    })

def actualizar_last_login(usuario_id):
    """
    Update last login timestamp for a user
    """
    db = get_mongo_db()
    db.usuarios.update_one(
        {"usuario_id": usuario_id},
        {"$set": {"last_login": datetime.now().isoformat()}}
    )

def obtener_usuario(usuario_id):
    """
    Get user information by ID
    """
    db = get_mongo_db()
    usuario = db.usuarios.find_one({"usuario_id": usuario_id})
    return usuario

def crear_cuenta_mongo(usuario_id):
    """
    Crea una nueva cuenta asociada a un usuario en MongoDB
    """
    db = get_mongo_db()
    account_id = str(uuid.uuid4())
    cuenta = {
        "account_id": account_id,
        "usuario_id": usuario_id,
        "balance": 0,
        "created_at": datetime.now().isoformat()
    }
    db.cuentas.insert_one(cuenta)
    return account_id

def actualizar_balances_en_mongo(account_id_origen, account_id_destino, monto):
    db = get_mongo_db()

    cuenta_origen = db.cuentas.find_one({"account_id": account_id_origen})
    cuenta_destino = db.cuentas.find_one({"account_id": account_id_destino})

    if not cuenta_origen or not cuenta_destino:
        return False, "Una de las cuentas no existe."

    if cuenta_origen["balance"] < monto:
        return False, "Saldo insuficiente en la cuenta de origen."

    # Actualizar balances
    db.cuentas.update_one(
        {"account_id": account_id_origen},
        {"$inc": {"balance": -monto}}
    )
    db.cuentas.update_one(
        {"account_id": account_id_destino},
        {"$inc": {"balance": monto}}
    )

    return True, "Transferencia realizada correctamente."


def insertar_notificacion(usuario_id, mensaje):
    db = get_mongo_db()
    db.notificaciones.insert_one({
        "usuario_id": usuario_id,
        "mensaje": mensaje,
        "fecha": datetime.now().isoformat()
    })

def obtener_notificaciones(usuario_id):
    db = get_mongo_db()
    return list(db.notificaciones.find({
        "usuario_id": usuario_id
    }).sort("fecha", -1))

def eliminar_notificaciones(usuario_id):
    db = get_mongo_db()
    db.notificaciones.delete_many({"usuario_id": usuario_id})

def ver_cuenta(email):
    print("\n=== Información de la Cuenta ===")

    db = get_mongo_db()

    # Obtener usuario desde MongoDB
    usuario = db.usuarios.find_one({"email": email})
    if not usuario:
        print("⚠️ Usuario no encontrado en MongoDB.")
        return

    print(f"Nombre: {usuario.get('nombre', 'N/A')}")
    print(f"Email: {usuario['email']}")
    print(f"Último inicio de sesión: {usuario.get('last_login', 'N/A')}")

    # Obtener el account_id desde Dgraph
    account_id = obtener_cuenta_por_email(email)
    if not account_id:
        print("⚠️ No se encontró la cuenta asociada en Dgraph.")
        return

    # Buscar cuenta en MongoDB usando el account_id
    cuenta = db.cuentas.find_one({"account_id": account_id})
    if not cuenta:
        print("⚠️ Cuenta no encontrada en MongoDB.")
        return

    print(f"ID de Cuenta: {cuenta['account_id']}")
    print(f"Balance: ${cuenta['balance']:.2f}")