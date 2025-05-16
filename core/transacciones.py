from db.Dgraph.dgraph import obtener_cuenta_por_email, obtener_uid_cuenta, registrar_transaccion_dgraph
from db.MongoDB.mongo import actualizar_balances_en_mongo, get_mongo_db, insertar_notificacion
from db.Cassandra.cassandra import crear_tabla_transacciones_amount, crear_tabla_transacciones_status, crear_tabla_transacciones_timestap, insertar_transaccion_amount, insertar_transaccion_status, insertar_transaccion_timestap
from uuid import uuid4


def realizar_transaccion(emailOrigen):
    print("\n=== Realizar Transacción ===", emailOrigen)
    email_destino = input("Correo del destinatario: ")
    monto = float(input("Monto a transferir: "))

    id_origen = obtener_cuenta_por_email(emailOrigen)
    id_destino = obtener_cuenta_por_email(email_destino)

    if not id_origen or not id_destino:
        print("Error: cuenta no encontrada.")
        return

    success, mensaje = actualizar_balances_en_mongo(id_origen, id_destino, monto)
    if success:
        from db.Dgraph.dgraph import obtener_cuenta_por_email

def realizar_transaccion(email_origen):
    print("\n=== Realizar Transacción ===")
    email_destino = input("Correo del destinatario: ")
    monto = float(input("Monto a transferir: "))

    id_origen = obtener_cuenta_por_email(email_origen)
    id_destino = obtener_cuenta_por_email(email_destino)

    if not id_origen or not id_destino:
        print("Error: cuenta no encontrada.", id_origen, id_destino)
        return

    # Obtener cuenta origen desde MongoDB
    db = get_mongo_db()
    cuenta_origen = db.cuentas.find_one({"account_id": id_origen})
    if not cuenta_origen:
        print("Error: no se encontró la cuenta origen en MongoDB.")
        return

    # ✅ Verificar si está bloqueada
    if cuenta_origen.get("bloqueada", False):
        print(" La cuenta está bloqueada y no puede realizar transacciones.")
        return

    # Obtener coordenadas
    lat = cuenta_origen.get("ubicacion", {}).get("lat")
    lon = cuenta_origen.get("ubicacion", {}).get("lon")

    if lat is None or lon is None:
        print("Error: la cuenta origen no tiene coordenadas registradas.")
        return

    # Actualizar balances
    success, mensaje = actualizar_balances_en_mongo(id_origen, id_destino, monto)
    if success:
        transaction_id = uuid4()

        insertar_transaccion_timestap(id_origen, id_destino, monto, transaction_id, lat, lon)
        insertar_transaccion_amount(id_origen, transaction_id, id_destino, monto, lat, lon)
        insertar_transaccion_status(id_origen, transaction_id, id_destino, monto, lat, lon, email_origen, email_destino)

        uid_origen = obtener_uid_cuenta(id_origen)
        uid_destino = obtener_uid_cuenta(id_destino)

        if uid_origen and uid_destino:
            print(registrar_transaccion_dgraph(uid_origen, uid_destino, transaction_id))
        else:
            print("No se pudo registrar la transacción en Dgraph (UIDs no encontrados).")

        print("✅ Transacción completada.")

        insertar_notificacion(
            id_origen,
            f"Hiciste una transacción de ${monto:.2f} a la cuenta asociada a {email_destino}."
        )
        insertar_notificacion(
            id_destino,
            f"Recibiste ${monto:.2f} de la cuenta asociada a {email_origen}."
        )
    else:
        print(f"Error: {mensaje}")



