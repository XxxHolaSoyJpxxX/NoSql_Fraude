from pymongo import MongoClient
from datetime import datetime

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

# ─────────────────────────────
# FUNCIONES DE DETECCIÓN DE FRAUDE
# ─────────────────────────────

def guardar_alerta_biometrica(usuario_id, transaccion):
    """
    Save biometric alert for high-risk transaction
    """
    db = get_mongo_db()
    alerta = {
        "usuario_id": usuario_id,
        "transaction_id": transaccion.get("transaction_id", "unknown"),
        "monto": transaccion.get("monto", 0),
        "categoria": transaccion.get("categoria", "unknown"),
        "timestamp": datetime.now().isoformat(),
        "estatus": "pendiente_validacion",
        "tipo": "biometrica"
    }
    
    result = db.alertas_biometricas.insert_one(alerta)
    return str(result.inserted_id)

def obtener_ubicaciones_conocidas(usuario_id):
    """
    Get known locations for a user
    """
    db = get_mongo_db()
    usuario = db.usuarios.find_one({"usuario_id": usuario_id})
    if not usuario or "ubicaciones_conocidas" not in usuario:
        return []
    
    return usuario.get("ubicaciones_conocidas", [])

def registrar_ubicacion_conocida(usuario_id, ubicacion):
    """
    Register a new known location for a user
    """
    db = get_mongo_db()
    db.usuarios.update_one(
        {"usuario_id": usuario_id},
        {"$addToSet": {"ubicaciones_conocidas": ubicacion}}
    )

def guardar_reporte_fraude(reporte):
    """
    Save fraud report from user
    """
    db = get_mongo_db()
    reporte["timestamp"] = datetime.now().isoformat()
    reporte["estatus"] = "pendiente_revision"
    
    result = db.reportes_fraude.insert_one(reporte)
    return str(result.inserted_id)

def obtener_reportes_fraude(usuario_id=None):
    """
    Get fraud reports for a user or all reports if no user_id is provided
    """
    db = get_mongo_db()
    
    if usuario_id:
        query = {"usuario_id": usuario_id}
    else:
        query = {}
    
    reportes = list(db.reportes_fraude.find(query).sort("timestamp", -1))
    
    # Convert ObjectId to string for JSON serialization
    for reporte in reportes:
        reporte["_id"] = str(reporte["_id"])
    
    return reportes

def actualizar_nivel_riesgo_usuario(usuario_id, nivel_riesgo):
    """
    Update risk level for a user
    """
    db = get_mongo_db()
    db.usuarios.update_one(
        {"usuario_id": usuario_id},
        {"$set": {"riesgo.risk_level": nivel_riesgo}}
    )

# ─────────────────────────────
# FUNCIONES DE GESTIÓN DE LÍMITES Y NOTIFICACIONES
# ─────────────────────────────

def guardar_limite_personalizado(usuario_id, limite):
    """
    Save personalized transaction limit for a user
    """
    db = get_mongo_db()
    db.usuarios.update_one(
        {"usuario_id": usuario_id},
        {"$set": {f"limites.{limite['tipo']}": limite["valor"]}}
    )

def obtener_limites_usuario(usuario_id):
    """
    Get transaction limits for a user
    """
    db = get_mongo_db()
    usuario = db.usuarios.find_one({"usuario_id": usuario_id})
    return usuario.get("limites", {})

def guardar_notificacion(usuario_id, notificacion):
    """
    Save notification for a user
    """
    db = get_mongo_db()
    notificacion["usuario_id"] = usuario_id
    notificacion["timestamp"] = datetime.now().isoformat()
    notificacion["leida"] = False
    
    result = db.notificaciones.insert_one(notificacion)
    return str(result.inserted_id)

def obtener_notificaciones(usuario_id, incluir_leidas=False):
    """
    Get notifications for a user
    """
    db = get_mongo_db()
    
    query = {"usuario_id": usuario_id}
    if not incluir_leidas:
        query["leida"] = False
    
    notificaciones = list(db.notificaciones.find(query).sort("timestamp", -1))
    
    # Convert ObjectId to string for JSON serialization
    for notificacion in notificaciones:
        notificacion["_id"] = str(notificacion["_id"])
    
    return notificaciones

def marcar_notificacion_leida(notificacion_id):
    """
    Mark notification as read
    """
    db = get_mongo_db()
    from bson.objectid import ObjectId
    db.notificaciones.update_one(
        {"_id": ObjectId(notificacion_id)},
        {"$set": {"leida": True}}
    )

# ─────────────────────────────
# FUNCIONES DE ADMINISTRACIÓN
# ─────────────────────────────

def obtener_usuarios_bajo_vigilancia():
    """
    Get users under surveillance (watchlist)
    """
    db = get_mongo_db()
    usuarios = list(db.usuarios.find({"vigilancia": True}))
    
    # Convert ObjectId to string for JSON serialization
    for usuario in usuarios:
        usuario["_id"] = str(usuario["_id"])
    
    return usuarios

def agregar_usuario_vigilancia(usuario_id, motivo):
    """
    Add user to surveillance list
    """
    db = get_mongo_db()
    db.usuarios.update_one(
        {"usuario_id": usuario_id},
        {
            "$set": {
                "vigilancia": True,
                "vigilancia_motivo": motivo,
                "vigilancia_desde": datetime.now().isoformat()
            }
        }
    )

def remover_usuario_vigilancia(usuario_id):
    """
    Remove user from surveillance list
    """
    db = get_mongo_db()
    db.usuarios.update_one(
        {"usuario_id": usuario_id},
        {
            "$set": {"vigilancia": False},
            "$unset": {"vigilancia_motivo": "", "vigilancia_desde": ""}
        }
    )

def registrar_accion_admin(admin_id, accion, detalles):
    """
    Register admin action for audit
    """
    db = get_mongo_db()
    log = {
        "admin_id": admin_id,
        "accion": accion,
        "detalles": detalles,
        "timestamp": datetime.now().isoformat(),
        "ip_address": detalles.get("ip_address", "unknown")
    }
    
    db.admin_logs.insert_one(log)

# ─────────────────────────────
# FUNCIONES DE CONFIGURACIÓN DE REGLAS DE FRAUDE
# ─────────────────────────────

def obtener_configuracion_reglas_fraude():
    """
    Get fraud rules configuration
    """
    db = get_mongo_db()
    config = db.configuracion.find_one({"tipo": "reglas_fraude"})
    return config if config else {"tipo": "reglas_fraude", "reglas": []}

def actualizar_regla_fraude(regla_id, regla):
    """
    Update fraud detection rule
    """
    db = get_mongo_db()
    db.configuracion.update_one(
        {"tipo": "reglas_fraude", "reglas.id": regla_id},
        {"$set": {"reglas.$": regla}}
    )

def agregar_regla_fraude(regla):
    """
    Add new fraud detection rule
    """
    db = get_mongo_db()
    db.configuracion.update_one(
        {"tipo": "reglas_fraude"},
        {"$push": {"reglas": regla}},
        upsert=True
    )

def eliminar_regla_fraude(regla_id):
    """
    Delete fraud detection rule
    """
    db = get_mongo_db()
    db.configuracion.update_one(
        {"tipo": "reglas_fraude"},
        {"$pull": {"reglas": {"id": regla_id}}}
    )

# ─────────────────────────────
# INICIALIZACIÓN DE LA BASE DE DATOS
# ─────────────────────────────

def inicializar_colecciones():
    """
    Initialize MongoDB collections with indexes
    """
    db = get_mongo_db()
    
    # Ensure indexes for usuarios collection
    db.usuarios.create_index("usuario_id", unique=True)
    db.usuarios.create_index("email", unique=True)
    
    # Ensure indexes for alertas_biometricas collection
    db.alertas_biometricas.create_index("usuario_id")
    db.alertas_biometricas.create_index("timestamp")
    
    # Ensure indexes for reportes_fraude collection
    db.reportes_fraude.create_index("usuario_id")
    db.reportes_fraude.create_index("timestamp")
    
    # Ensure indexes for notificaciones collection
    db.notificaciones.create_index("usuario_id")
    db.notificaciones.create_index("timestamp")
    
    # Ensure indexes for admin_logs collection
    db.admin_logs.create_index("admin_id")
    db.admin_logs.create_index("timestamp")
    
    print("MongoDB collections initialized with indexes")

# Inicializar si se ejecuta este módulo directamente
if __name__ == "__main__":
    inicializar_colecciones()
