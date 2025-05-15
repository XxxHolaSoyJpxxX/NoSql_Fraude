from db.MongoDB.mongo import obtener_notificaciones, eliminar_notificaciones

def ver_notificaciones(email):
    from db.Dgraph.dgraph import obtener_cuenta_por_email

    cuenta = obtener_cuenta_por_email(email)
    if not cuenta:
        print(" No se encontró la cuenta asociada a este correo.")
        return

    usuario_id = cuenta
    notificaciones = obtener_notificaciones(usuario_id)

    if not notificaciones:
        print("No tienes notificaciones.")
        return

    print("\nTus notificaciones:")
    for notif in notificaciones:
        print(f"- [{notif['fecha']}] {notif['mensaje']}")

    opcion = input("\n¿Quieres eliminar todas las notificaciones? (s/n): ")
    if opcion.lower() == 's':
        eliminar_notificaciones(usuario_id)
        print("Notificaciones eliminadas.")
    else:
        print("Notificaciones conservadas.")
