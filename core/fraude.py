from datetime import datetime
from uuid import uuid4
import uuid
from db.Dgraph.dgraph import get_dgraph_client, obtener_cuenta_por_email, obtener_uid_usuario_por_email
from db.MongoDB.mongo import get_mongo_db
from db.Cassandra.cassandra import ver_transacciones_por_amount
import json


def reportar_transaccion(email_usuario):

    print("\n=== Reportar Transacción Fraudulenta ===")

    # Mostrar las transacciones del usuario
    ver_transacciones_por_amount(email_usuario)

    # Pedir datos del reporte
    id_transaccion = input("\nIngresa el ID de la transacción que deseas reportar: ").strip()
    motivo = input("Describe el motivo del reporte: ").strip()

    # Registrar reporte
    resultado = reportar_transaccion_fraudulenta(id_transaccion, email_usuario, motivo)
    print(resultado)



def reportar_transaccion_fraudulenta(transaction_id, email_usuario, motivo):
    client = get_dgraph_client()

    # Obtener UID del usuario (NO el account_id)
    usuario_uid = obtener_uid_usuario_por_email(email_usuario)
    if not usuario_uid:
        return "No se encontró el usuario en Dgraph."

    # Obtener UID de la transacción desde el transaction_id
    query = """
    query obtenerUID($id: string) {
        transacciones(func: eq(transaction_id, $id)) {
            uid
        }
    }
    """
    try:
        res = client.txn(read_only=True).query(query, variables={"$id": transaction_id})
        data = json.loads(res.json)
        transacciones = data.get("transacciones", [])

        if not transacciones:
            return "No se encontró la transacción en Dgraph."

        transaccion_uid = transacciones[0]["uid"]

        # Crear el nodo de ReporteFraude y enlazarlo con la transacción
        txn = client.txn()
        reporte = {
            "dgraph.type": "ReporteFraude",
            "reporte_id": str(uuid4()),
            "motivo": motivo,
            "fecha": datetime.now().isoformat(),
            "reportado_por": {"uid": usuario_uid},
            "transaccion_reportada": {"uid": transaccion_uid}
        }
        txn.mutate(set_obj=reporte)
        txn.commit()

        return "✅ Reporte de fraude registrado correctamente."
    except Exception as e:
        return f" Error al registrar el reporte: {e}"
    finally:
        txn.discard()