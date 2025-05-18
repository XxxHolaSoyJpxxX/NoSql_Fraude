import pydgraph
import json
from datetime import datetime
import uuid

# ─────────────────────────────
# CONEXIÓN A DGRAPH
# ─────────────────────────────

def get_dgraph_client():
    stub = pydgraph.DgraphClientStub("localhost:9080")
    return pydgraph.DgraphClient(stub)

# ─────────────────────────────
# DEFINIR ESQUEMA DE USUARIOS Y CUENTAS
# ─────────────────────────────

def definir_schema():
    client = get_dgraph_client()
    schema = """
    type User {
        email
        owns_account
    }

    type Account {
        account_id
        owned_by
        from_transactions
        to_transactions
    }

    type Transaccion {
        transaction_id
        from_account
        to_account
        fue_reportada
        reporte_id
        location
    }

    type ReporteFraude {
    reporte_id
    }

    # User fields
    email: string @index(exact) .
    owns_account: uid .

    # Account fields
    account_id: string @index(exact) .
    owned_by: uid @reverse .
    from_transactions: [uid] @reverse .
    to_transactions: [uid] @reverse .

    # Transaccion fields
    transaction_id: string @index(exact) .
    from_account: uid .
    to_account: uid .
    fue_reportada: bool .   
    location: geo @index(geo) .


    reporte_id: string @index(exact) .
    """

    op = pydgraph.Operation(schema=schema)
    client.alter(op)


# ─────────────────────────────
# AGREGAR USUARIO
# ─────────────────────────────

def agregar_usuario(email, account_id):
    client = get_dgraph_client()
    txn = client.txn()
    try:
        data = {
            "dgraph.type": "User",
            "email": email,
            "owns_account": {
                "dgraph.type": "Account",
                "account_id": account_id
            }
        }
        txn.mutate(set_obj=data)
        txn.commit()
    finally:
        txn.discard()



# ─────────────────────────────
# CONSULTAR CUENTAS DE UN USUARIOS
# ─────────────────────────────

def obtener_usuarios():  # consulta todos los usuarios (admin)
    client = get_dgraph_client()
    query = """
    {
        usuarios(func: type(User)) {
            uid
            user_id
            owns_account {
                uid
                account_id
            }
        }
    }
    """
    res = client.txn(read_only=True).query(query)
    return json.loads(res.json)

def obtener_cuenta_por_email(email):  # obtener cuenta desde el email
    client = get_dgraph_client()
    txn = client.txn(read_only=True)

    query = """
    query getAccount($correo: string) {
        usuario(func: eq(email, $correo)) {
            owns_account {
                uid
                account_id
            }
        }
    }
    """
    variables = {"$correo": email}

    try:
        res = txn.query(query, variables=variables)
        data = json.loads(res.json)
        usuario = data.get("usuario", [])
        if usuario and usuario[0].get("owns_account"):
            return usuario[0]["owns_account"]["account_id"]
        return None
    finally:
        txn.discard()

def obtener_uid_cuenta(account_id):
    client = get_dgraph_client()
    txn = client.txn(read_only=True)
    query = """
    query getUID($id: string) {
        cuenta(func: eq(account_id, $id)) {
            uid
        }
    }
    """
    try:
        res = txn.query(query, variables={"$id": account_id})
        data = json.loads(res.json)
        cuenta = data.get("cuenta", [])
        if cuenta:
            return cuenta[0]["uid"]
        return None
    finally:
        txn.discard()

def registrar_transaccion_dgraph(account_from_uid, account_to_uid, id_transaction, latitude, longitude):
    client = get_dgraph_client()
    txn = client.txn()
    try:
        transaccion = {
            "dgraph.type": "Transaccion",
            "transaction_id": str(id_transaction),
            "from_account": {"uid": account_from_uid},
            "to_account": {"uid": account_to_uid},
            "location": {"type": "Point", "coordinates": [float(latitude), float(longitude)]},
        }
        txn.mutate(set_obj=transaccion)
        txn.commit()
        return "✅ Transacción registrada en Dgraph"
    finally:
        txn.discard()

def obtener_uid_usuario_por_email(email):
    client = get_dgraph_client()
    txn = client.txn(read_only=True)

    query = """
    query getUID($correo: string) {
        usuario(func: eq(email, $correo)) {
            uid
        }
    }
    """
    variables = {"$correo": email}

    try:
        res = txn.query(query, variables=variables)
        data = json.loads(res.json)
        usuario = data.get("usuario", [])
        if usuario:
            return usuario[0]["uid"]
        return None
    finally:
        txn.discard()

def obtener_cuenta_completa(uid):
    client = get_dgraph_client()
    txn = client.txn(read_only=True)

    query = """
    query obtenerCuenta($id: string) {
        cuenta(func: uid($id)) {
            uid
            account_id
            from_account
            # Opcional: incluir transacciones si las necesitas
            # from_transactions { transaction_id }
            # to_transactions { transaction_id }
        }
    }
    """

    try:
        res = txn.query(query, variables={"$id": uid})
        data = json.loads(res.json)
        cuentas = data.get("cuenta", [])
        if cuentas:
            return cuentas[0]
        return None
    finally:
        txn.discard()

def cerca_de_ubicacion(latitud, longitud, radio):
    client = get_dgraph_client()
    txn = client.txn(read_only=True)

    query = """
    query cercaDeUbicacion($lat: float, $lon: float, $radio: float) {
        cuentas(func: near(location, [$lat, $lon], $radio)) {
            uid
            transaction_id
            from_account{
			    account_id
            }
        }
    }
    """

    try:
        res = txn.query(query, variables={"$lat": latitud, "$lon": longitud, "$radio": radio})
        data = json.loads(res.json)
        return data.get("cuentas", [])
    finally:
        txn.discard()