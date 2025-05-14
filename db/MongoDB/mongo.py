from pymongo import MongoClient
from datetime import datetime

def get_mongo_db():
    client = MongoClient("mongodb://localhost:27017")
    return client["banco"]

def crear_usuario(usuario):
    db = get_mongo_db()
    if db.usuarios.find_one({"usuario_id": usuario["usuario_id"]}):
        return False
    db.usuarios.insert_one(usuario)
    return True

def usuario_existe(usuario_id):
    db = get_mongo_db()
    return db.usuarios.find_one({"usuario_id": usuario_id}) is not None

def verificar_credenciales_usuario(email, password):
    db = get_mongo_db()
    return db.usuarios.find_one({
        "email": email,
        "password": password
    })

def verificar_credenciales_admin(email, password):
    db = get_mongo_db()
    return db.usuarios.find_one({
        "email": email,
        "password": password,
        "es_admin": True
    })

def actualizar_last_login(usuario_id):
    db = get_mongo_db()
    db.usuarios.update_one(
        {"usuario_id": usuario_id},
        {"$set": {"last_login": datetime.now().isoformat()}}
    )