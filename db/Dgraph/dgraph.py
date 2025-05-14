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
    usuario_id: string @index(exact) .
    account_id: string @index(exact) .
    account_type: string .
    balance: float .
    currency: string .
    status: string .

    tiene_cuenta: [uid] .

    type Usuario {
        usuario_id
        tiene_cuenta
    }

    type Cuenta {
        account_id
        account_type
        balance
        currency
        status
    }
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
