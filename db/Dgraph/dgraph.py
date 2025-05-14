import pydgraph
import json
from datetime import datetime

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
        user_id
        full_name
        email
        phone
        registration_date
        owns_accounts
    }

    type Account {
        account_id
        account_type
        balance
        currency
        status
        spending_limit
        owned_by
        outgoing_transactions
        incoming_transactions
    }

    type Transaccion {
        transaction_id
        source_location
        destination_location
        from_account
        to_account
        amount
        timestamp
        description
        transaction_status
    }

    # Usuarios
    user_id: string @index(exact) .
    full_name: string @index(term) .
    email: string @index(exact) .
    phone: string @index(exact) .
    registration_date: datetime .
    owns_accounts: [uid] @reverse .

    # Cuentas
    account_id: string @index(exact) .
    account_type: string @index(term) .
    balance: float .
    currency: string .
    status: string @index(term) .
    spending_limit: float .
    owned_by: uid @reverse .
    outgoing_transactions: [uid] @reverse .
    incoming_transactions: [uid] @reverse .

    # Transacciones
    transaction_id: string @index(exact) .
    source_location: geo .
    destination_location: geo .
    from_account: uid @reverse .
    to_account: uid @reverse .
    amount: float .
    timestamp: datetime .
    description: string .
    transaction_status: string @index(term) .  # e.g., completed, failed, pending
    """
    op = pydgraph.Operation(schema=schema)
    client.alter(op)

# ─────────────────────────────
# AGREGAR USUARIO
# ─────────────────────────────

def agregar_usuario(user_id, full_name, email, phone):
    client = get_dgraph_client()
    txn = client.txn()
    try:
        usuario = {
            "dgraph.type": "User",
            "user_id": user_id,
            "full_name": full_name,
            "email": email,
            "phone": phone,
            "registration_date": datetime.utcnow().isoformat()
        }
        response = txn.mutate(set_obj=usuario)
        txn.commit()
        return response.uids
    finally:
        txn.discard()

# ─────────────────────────────
# CONSULTAR CUENTAS DE UN USUARIOS
# ─────────────────────────────

def obtener_usuarios():
    client = get_dgraph_client()
    query = """
    {
        usuarios(func: type(User)) {
            uid
            user_id
            full_name
            email
            phone
            registration_date
            owns_accounts {
                uid
                account_id
                balance
            }
        }
    }
    """
    res = client.txn(read_only=True).query(query)
    return json.loads(res.json)

# ─────────────────────────────
# ACTUALIZAR USUARIO
# ─────────────────────────────

def actualizar_nombre_usuario(user_id, nuevo_nombre):
    client = get_dgraph_client()
    txn = client.txn()
    try:
        query = f"""
        {{
            user(func: eq(user_id, "{user_id}")) {{
                uid
            }}
        }}
        """
        res = txn.query(query)
        uid = json.loads(res.json)["user"][0]["uid"]

        update = {
            "uid": uid,
            "full_name": nuevo_nombre
        }
        txn.mutate(set_obj=update)
        txn.commit()
    finally:
        txn.discard()
        
# ─────────────────────────────
# ELEMINAR USARIO
# ─────────────────────────────
        
def eliminar_usuario(user_id):
    client = get_dgraph_client()
    txn = client.txn()
    try:
        query = f"""
        {{
            user(func: eq(user_id, "{user_id}")) {{
                uid
            }}
        }}
        """
        res = txn.query(query)
        uid = json.loads(res.json)["user"][0]["uid"]

        delete_obj = {
            "uid": uid
        }
        txn.mutate(del_obj=delete_obj)
        txn.commit()
    finally:
        txn.discard()

# ─────────────────────────────
# Agregar Cuenta
# ─────────────────────────────

def agregar_cuenta(account_id, account_type, balance, currency, status, spending_limit, user_uid):
    client = get_dgraph_client()
    txn = client.txn()
    try:
        cuenta = {
            "dgraph.type": "Account",
            "account_id": account_id,
            "account_type": account_type,
            "balance": balance,
            "currency": currency,
            "status": status,
            "spending_limit": spending_limit,
            "owned_by": {"uid": user_uid}
        }
        response = txn.mutate(set_obj=cuenta)
        txn.commit()
        return response.uids
    finally:
        txn.discard()

# ─────────────────────────────
# Consultar Cuentas
# ─────────────────────────────

def obtener_cuentas():
    client = get_dgraph_client()
    query = """
    {
        cuentas(func: type(Account)) {
            uid
            account_id
            account_type
            balance
            currency
            status
            spending_limit
            owned_by {
                uid
                user_id
                full_name
            }
        }
    }
    """
    res = client.txn(read_only=True).query(query)
    return json.loads(res.json)

# ─────────────────────────────
# Actualizar Balance
# ─────────────────────────────

def actualizar_balance(account_id, nuevo_balance):
    client = get_dgraph_client()
    txn = client.txn()
    try:
        query = f"""
        {{
            cuenta(func: eq(account_id, "{account_id}")) {{
                uid
            }}
        }}
        """
        res = txn.query(query)
        uid = json.loads(res.json)["cuenta"][0]["uid"]

        update = {
            "uid": uid,
            "balance": nuevo_balance
        }
        txn.mutate(set_obj=update)
        txn.commit()
    finally:
        txn.discard()

# ─────────────────────────────
#  Eliminar Cuenta
# ─────────────────────────────

def eliminar_cuenta(account_id):
    client = get_dgraph_client()
    txn = client.txn()
    try:
        query = f"""
        {{
            cuenta(func: eq(account_id, "{account_id}")) {{
                uid
            }}
        }}
        """
        res = txn.query(query)
        uid = json.loads(res.json)["cuenta"][0]["uid"]

        txn.mutate(del_obj={"uid": uid})
        txn.commit()
    finally:
        txn.discard()

# ─────────────────────────────
#  Agregar Transacción
# ─────────────────────────────

def agregar_transaccion(transaction_id, from_uid, to_uid, amount, description, transaction_status):
    client = get_dgraph_client()
    txn = client.txn()
    try:
        transaccion = {
            "dgraph.type": "Transaccion",
            "transaction_id": transaction_id,
            "from_account": {"uid": from_uid},
            "to_account": {"uid": to_uid},
            "amount": amount,
            "timestamp": datetime.utcnow().isoformat(),
            "description": description,
            "transaction_status": transaction_status
        }
        response = txn.mutate(set_obj=transaccion)
        txn.commit()
        return response.uids
    finally:
        txn.discard()

# ─────────────────────────────
#   Consultar Transacciones
# ─────────────────────────────

def obtener_transacciones():
    client = get_dgraph_client()
    query = """
    {
        transacciones(func: type(Transaccion)) {
            uid
            transaction_id
            amount
            timestamp
            description
            transaction_status
            from_account {
                uid
                account_id
            }
            to_account {
                uid
                account_id
            }
        }
    }
    """
    res = client.txn(read_only=True).query(query)
    return json.loads(res.json)

# ─────────────────────────────
#  Actualizar Estado de Transacción
# ─────────────────────────────

def actualizar_estado_transaccion(transaction_id, nuevo_estado):
    client = get_dgraph_client()
    txn = client.txn()
    try:
        query = f"""
        {{
            trans(func: eq(transaction_id, "{transaction_id}")) {{
                uid
            }}
        }}
        """
        res = txn.query(query)
        uid = json.loads(res.json)["trans"][0]["uid"]

        update = {
            "uid": uid,
            "transaction_status": nuevo_estado
        }
        txn.mutate(set_obj=update)
        txn.commit()
    finally:
        txn.discard()

# ─────────────────────────────
#   Eliminar Transacción
# ─────────────────────────────

def eliminar_transaccion(transaction_id):
    client = get_dgraph_client()
    txn = client.txn()
    try:
        query = f"""
        {{
            trans(func: eq(transaction_id, "{transaction_id}")) {{
                uid
            }}
        }}
        """
        res = txn.query(query)
        uid = json.loads(res.json)["trans"][0]["uid"]

        txn.mutate(del_obj={"uid": uid})
        txn.commit()
    finally:
        txn.discard()

# ─────────────────────────────
# CONSULTAR CUENTAS DE UN USUARIO
# ─────────────────────────────

def obtener_cuentas_usuario(usuario_id):
    client = get_dgraph_client()
    query = f"""
    {{
      cuentas(func: eq(user_id, "{usuario_id}")) {{
        owns_accounts {{
          account_id
          account_type
          balance
          currency
          status
        }}
      }}
    }}
    """
    res = client.txn(read_only=True).query(query)
    data = json.loads(res.json)
    if not data.get("cuentas"):
        return []

    return data["cuentas"][0].get("owns_accounts", [])
