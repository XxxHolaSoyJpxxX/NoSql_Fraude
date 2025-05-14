# core/usuario.py
from db.Dgraph.dgraph import obtener_cuentas_usuario
from db.Cassandra.cassandra import obtener_ultimas_transacciones_usuario, obtener_transacciones_cuenta, obtener_estadisticas_usuario
from db.MongoDB.mongo import guardar_reporte_fraude, guardar_notificacion, obtener_notificaciones, marcar_notificacion_leida
from core.fraude import SistemaDeteccionFraude

def menu_usuario(usuario_id):
    """
    Menú principal del usuario enfocado en la detección de fraude
    """
    while True:
        print(f"\n=== Menú del Usuario: {usuario_id} ===")
        print("1. Realizar transaccion") #realizar una transaccion a otra cuenta
        print("1. Ver historial de transacciones")
        print("2. Reportar transacción no reconocida")
        print("1. Ver mi cuenta") #ver toda la informacion de la cuenta
        print("5. Ver notificaciones")
        print("1. Pedir ayuda")
        print("6. Salir")

        opcion = input("Selecciona una opción: ")

        if opcion == "1":
            ver_historial_transacciones(usuario_id)
        elif opcion == "2":
            reportar_fraude(usuario_id)
        elif opcion == "3":
            ver_alertas_seguridad(usuario_id)
        elif opcion == "4":
            ver_nivel_riesgo(usuario_id)
        elif opcion == "5":
            ver_notificaciones(usuario_id)
        elif opcion == "6":
            print("Saliendo del menú de usuario.")
            break
        else:
            print("Opción no válida.")
