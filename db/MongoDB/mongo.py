from pymongo import MongoClient
from datetime import datetime
import uuid

from db.Dgraph.dgraph import obtener_cuenta_completa, obtener_cuenta_por_email, obtener_uid_cuenta
from db.Cassandra.cassandra import registrar_accion_admin, ver_transacciones_por_timestamp


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

def crear_cuenta_mongo(usuario_id, lat,lon):
    """
    Crea una nueva cuenta asociada a un usuario en MongoDB
    """
    db = get_mongo_db()
    account_id = str(uuid.uuid4())
    cuenta = {
        "account_id": account_id,
        "usuario_id": usuario_id,
        "balance": 0,
        "bloqueada": False,  # Nueva cuenta no está bloqueada
        "created_at": datetime.now().isoformat(),
        "ubicacion": {
            "lat": lat,
            "lon": lon
        }
    }
    db.cuentas.insert_one(cuenta)
    return account_id

def obter_ubicacion_cuenta(email_usuario):
    """
    Obtiene la ubicación de una cuenta en MongoDB
    """
    db = get_mongo_db()
    cuenta = db.usuarios.find_one({"email": email_usuario})
    print(cuenta)
    if cuenta:
        ubicacion = cuenta.get("ubicacion")
        print(ubicacion)
        return {
            "lat": ubicacion.get("lat"),
            "lon": ubicacion.get("lon")
        }
    else :
        return "No se encontró la cuenta en MongoDB."

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

    usuario = db.usuarios.find_one({"email": email})
    if not usuario:
        print(" Usuario no encontrado en MongoDB.")
        return

    print(f"Nombre: {usuario.get('nombre', 'N/A')}")
    print(f"Email: {usuario['email']}")
    print(f"Último inicio de sesión: {usuario.get('last_login', 'N/A')}")

    account_id = obtener_cuenta_por_email(email)
    if not account_id:
        print(" No se encontró la cuenta asociada en Dgraph.")
        return

    cuenta = db.cuentas.find_one({"account_id": account_id})
    if not cuenta:
        print(" Cuenta no encontrada en MongoDB.")
        return

    print(f"ID de Cuenta: {cuenta['account_id']}")
    print(f"Balance: ${cuenta['balance']:.2f}")


def auditoria(admin_id):
    print("\n=== Auditoría de Usuario ===")
    email = input("Ingresa el correo del usuario a auditar: ")
    ver_cuenta(email)
    ver_transacciones_por_timestamp(email)

    registrar_accion_admin(
        admin_id,
        "Auditoría de usuario",
        f"Cuenta auditada: {email}"
    )

def bloquear_cuenta(admin_id):
    db = get_mongo_db()
    email = input("Correo del usuario a bloquear: ").strip()

    account_id = obtener_cuenta_por_email(email)
    if not account_id:
        print(" No se encontró la cuenta en Dgraph.")
        return

    result = db.cuentas.update_one(
        {"account_id": account_id},
        {"$set": {"bloqueada": True}}
    )

    if result.matched_count:
        print(" Cuenta bloqueada exitosamente.")
        registrar_accion_admin(admin_id, "Bloqueo de cuenta", f"Usuario: {email}")
    else:
        print(" No se pudo bloquear la cuenta (no encontrada en MongoDB).")

    registrar_accion_admin(admin_id, f'Bloque cuenta {account_id}', 'Bloqueo de cuenta')

def desbloquear_cuenta(admin_id):
    db = get_mongo_db()
    email = input("Correo del usuario a desbloquear: ").strip()

    account_id = obtener_cuenta_por_email(email)
    if not account_id:
        print(" No se encontró la cuenta en Dgraph.")
        return

    result = db.cuentas.update_one(
        {"account_id": account_id},
        {"$set": {"bloqueada": False}}
    )

    if result.matched_count:
        print(" Cuenta desbloqueada exitosamente.")
        registrar_accion_admin(admin_id, "Desbloqueo de cuenta", f"Usuario: {email}")
    else:
        print(" No se pudo desbloquear la cuenta (no encontrada en MongoDB).")

    registrar_accion_admin(admin_id, f'Desbloqueo cuenta {account_id}', 'Desbloqueo de cuenta')

def actualizar_balce_cuenta(email, monto):
    account_id = obtener_cuenta_por_email(email)
    db = get_mongo_db()
    cuenta = db.cuentas.find_one({"account_id": account_id})
    if not cuenta:
        print(" Cuenta no encontrada en MongoDB.")
        return
  
    db.cuentas.update_one(
        {"account_id": account_id},
        {"$inc": {"balance": +monto}}
    )
    nuevo_balance = db.cuentas.find_one({"account_id": account_id})['balance']
    print(f" El nuevo balance de la cuenta es: {nuevo_balance}")
        