from core.registro import registrar_admin, registrar_usuario, login_usuario, login_admin
from db.Dgraph.dgraph import definir_schema
from db.Cassandra.cassandra import crear_tabla_transaccion,crear_tabla_acciones_admin_global, crear_tabla_acciones_admin_por_admin, crear_tabla_transacciones_amount, crear_tabla_transacciones_status, crear_tabla_transacciones_timestap
from db.MongoDB.mongo import crear_indices
from data.inserion import cargar_usuarios_desde_csv

def main():
    definir_schema() 
    crear_indices()
    crear_tabla_transacciones_timestap()
    crear_tabla_transacciones_status()
    crear_tabla_transacciones_amount()
    crear_tabla_transaccion()
    crear_tabla_acciones_admin_por_admin()
    crear_tabla_acciones_admin_global()

    while True:
        print("\n==============================")
        print("  Bienvenido al Banco Virtual")
        print("==============================")
        print("1. Iniciar sesión como Usuario")
        print("2. Registrarse como Usuario")
        print("3. Iniciar sesión como Administrador")
        print("4. Registrarse como Administrador")
        print("5. Cargar usuarios desde CSV")
        print("6. Salir")
        opcion = input("Selecciona una opción: ")

        if opcion == "1":
            login_usuario()
        elif opcion == "2":
            registrar_usuario()
        elif opcion == "3":
            login_admin()
        elif opcion == "4":
            registrar_admin()
        elif opcion == "5":
            ruta_csv = "./data/mongo_data.csv"
            num_transacciones = int(input("Número de transacciones por usuario (default 3): ") or 3)
            cargar_usuarios_desde_csv(ruta_csv, num_transacciones)
            
        elif opcion == "6":
            break
        else:
            print("Opción no válida. Intenta de nuevo.")

if __name__ == "__main__":
    main()
