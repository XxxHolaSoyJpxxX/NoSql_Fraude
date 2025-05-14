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
Usuarios
user_id: string @index(exact) .
full_name: string @index(term) .
email: string @index(exact) .
phone: string @index(exact) .
registration_date: datetime .
owns_accounts: [uid] @reverse .

Cuentas
account_id: string @index(exact) .
account_type: string @index(term) .
balance: float .
currency: string .
status: string @index(term) .
spending_limit: float .
owned_by: uid @reverse .
outgoing_transactions: [uid] @reverse .
incoming_transactions: [uid] @reverse .

Transacciones
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
# CREAR CUENTA ASOCIADA A UN USUARIO
# ─────────────────────────────

def crear_cuenta_para_usuario(usuario_id, cuenta):
    client = get_dgraph_client()
    txn = client.txn()
    try:
        # Buscar el UID del usuario
        query = f"""
        {{
          usuario(func: eq(usuario_id, "{usuario_id}")) {{
            uid
          }}
        }}
        """
        res = client.txn(read_only=True).query(query)
        data = json.loads(res.json)
        uid_usuario = data.get("usuario", [{}])[0].get("uid")

        # Si el usuario no existe, lo crea
        if not uid_usuario:
            uid_usuario = f"_:{usuario_id}"

        # Crear cuenta vinculada
        data = {
            "uid": uid_usuario,
            "usuario_id": usuario_id,
            "tiene_cuenta": [
                {
                    "account_id": cuenta["account_id"],
                    "account_type": cuenta["account_type"],
                    "balance": cuenta["balance"],
                    "currency": cuenta["currency"],
                    "status": cuenta["status"]
                }
            ]
        }
        txn.mutate(set_obj=data, commit_now=True)
    finally:
        txn.discard()

# ─────────────────────────────
# CONSULTAR CUENTAS DE UN USUARIO
# ─────────────────────────────

def obtener_cuentas_usuario(usuario_id):
    client = get_dgraph_client()
    query = f"""
    {{
      cuentas(func: eq(usuario_id, "{usuario_id}")) {{
        tiene_cuenta {{
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
    return data.get("cuentas", [])[0].get("tiene_cuenta", []) if data.get("cuentas") else []

def 