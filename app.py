from core.registro import registrar_usuario, login_usuario, login_admin
from db.Dgraph.dgraph import definir_schema
from db.Cassandra.cassandra import crear_tabla_transacciones_amount, crear_tabla_transacciones_status, crear_tabla_transacciones_timestap

def main():
    definir_schema() 
    crear_tabla_transacciones_timestap()
    crear_tabla_transacciones_status()
    crear_tabla_transacciones_amount()

    while True:
        print("\n==============================")
        print("  Bienvenido al Banco Virtual")
        print("==============================")
        print("1. Iniciar sesión como Usuario")
        print("2. Registrarse como Usuario")
        print("3. Iniciar sesión como Administrador")
        print("4. Salir")
        opcion = input("Selecciona una opción: ")

        if opcion == "1":
            login_usuario()
        elif opcion == "2":
            registrar_usuario()
        elif opcion == "3":
            login_admin()
        elif opcion == "4":
            print("Saliendo del sistema.")
            break
        else:
            print("Opción no válida. Intenta de nuevo.")

if __name__ == "__main__":
    main()
