# core/usuario
from core.transacciones import realizar_transaccion
from db.Cassandra.cassandra import ver_transacciones_por_amount
from core.notificaciones import ver_notificaciones
from core.fraude import reportar_transaccion
from db.MongoDB.mongo import ver_cuenta,actualizar_balce_cuenta


def menu_usuario(email):
    """
    Menú principal del usuario enfocado en la detección de fraude
    """
    while True:
        print(f"\n=== Menú del Usuario: {email} ===")
        print("1. Realizar transaccion") #realizar una transaccion a otra cuenta
        print("2. Ver historial de transacciones")
        print("3. Reportar transacción no reconocida")
        print("4. Ver mi cuenta") #ver toda la informacion de la cuenta
        print("5. Ver notificaciones")
        print("6. Pedir ayuda")
        print("7. Ingrsar Dinero") #ingresar dinero a la cuenta
        print("8. Salir")

        opcion = input("Selecciona una opción: ")

        if opcion == "1":
            realizar_transaccion(email)
        elif opcion == "2":
            ver_transacciones_por_amount(email)
        elif opcion == "3":
            reportar_transaccion(email)
        elif opcion == "4":
            ver_cuenta(email)
        elif opcion == "5":
            ver_notificaciones(email)
        elif opcion == "6":
            print("Pidiendo ayuda...")
            print("Pero nadien vendrá a ayudarte.")
            break
        elif opcion == "7":
            print("Cuanto dinero deseas ingresar?")
            monto = float(input("Monto a transferir: "))
            actualizar_balce_cuenta(email, monto)
            print("Ingresando dinero...")
            break
        elif opcion == "8":
            print("Saliendo del menú...")
            break
        else:
            print("Opción no válida.")
