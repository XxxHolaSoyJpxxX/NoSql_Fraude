from db.Dgraph.dgraph import obtener_cuenta_por_email, obtener_uid_cuenta, registrar_transaccion_dgraph
from db.MongoDB.mongo import actualizar_balances_en_mongo, get_mongo_db, insertar_notificacion
from db.Cassandra.cassandra import crear_tabla_transacciones_amount, crear_tabla_transacciones_status, crear_tabla_transacciones_timestap, insertar_transaccion_amount, insertar_transaccion_status, insertar_transaccion_timestap,actualizar_estado_transaccion,insertar_transaccion
from uuid import uuid4
from core.fraude import verificar_geolocalizacion, reportar_fraude_auto,detectar_duplicacion_transacciones,tiempo_entre_transacciones,detectar_gasto_inusual


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
        insertar_transaccion_amount(id_origen, id_destino, monto, transaction_id, lat, lon)
        insertar_transaccion_status(id_origen, id_destino, monto, transaction_id, lat, lon)
        insertar_transaccion(id_origen, id_destino, monto, transaction_id, lat, lon)

        uid_origen = obtener_uid_cuenta(id_origen)
        uid_destino = obtener_uid_cuenta(id_destino)
        
            
            # Aquí podrías agregar lógica para bloquear la cuenta o notificar al usuario.
        
        if uid_origen and uid_destino:
            print(registrar_transaccion_dgraph(uid_origen, uid_destino, transaction_id,lat, lon))
        else:
            print("No se pudo registrar la transacción en Dgraph (UIDs no encontrados).")

        
        if not verificar_geolocalizacion(email_origen,transaction_id):
            print("No se detectó fraude en la transacción.")
        else:
            print("Se detectó un posible fraude en la transacción. generado por reporte")
            reportar_fraude_auto(email_origen, "tipo: geolocalizacion", transaction_id)
            actualizar_estado_transaccion(transaction_id, "fraude")
        if not detectar_duplicacion_transacciones(transaction_id):
            print("No se detectó duplicación de transacciones.")
        else:
            print("Se detectó un posible fraude en la transacción. generado por duplicacion")
            reportar_fraude_auto(email_origen, "tipo: duplicado", transaction_id)
            actualizar_estado_transaccion(transaction_id, "fraude")
        if not tiempo_entre_transacciones(email_origen):
            print("No se detectó muchas iregularidades entre el timpo de transacciones.")
        else:
            print("Se detectó un posible fraude en la transacción. generado por tiempo entre transacciones")
            reportar_fraude_auto(email_origen, "tipo: tiempo", transaction_id)
            actualizar_estado_transaccion(transaction_id, "fraude")
        if not detectar_gasto_inusual(transaction_id):
            print("No se detectó gasto inusual en la transacción.")
        else:
            print("Se detectó un posible gasto inusual en la transacción. generado por gasto inusual")
            reportar_fraude_auto(email_origen, "tipo: gasto inusual", transaction_id)
            actualizar_estado_transaccion(transaction_id, "fraude")
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



