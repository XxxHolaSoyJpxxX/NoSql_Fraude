from db.Dgraph.dgraph import obtener_cuentas_usuario, crear_cuenta_para_usuario

def menu_usuario(usuario_id):
    while True:
        print(f"\n=== Menú del Usuario: {usuario_id} ===")
        print("1. Ver y administrar cuentas/tarjetas")
        print("2. Realizar transacción")
        print("3. Reportar transacción no reconocida")
        print("4. Ver historial financiero")
        print("5. Gestionar beneficiarios frecuentes")
        print("6. Configurar límites personalizados")
        print("7. Ver notificaciones")
        print("8. Crear nueva cuenta")
        print("9. Salir")

        opcion = input("Selecciona una opción: ")

        if opcion == "1":
            cuentas = obtener_cuentas_usuario(usuario_id)
            if not cuentas:
                print("🔍 No tienes cuentas asociadas.")
            else:
                print("\nTus cuentas:")
                for c in cuentas:
                    print(f"• {c['account_type'].capitalize()} ({c['account_id']}): {c['balance']} {c['currency']} - Estado: {c['status']}")
        elif opcion == "2":
            print("[TODO] Realizar transacción (Cassandra + MongoDB + Dgraph)")
        elif opcion == "3":
            print("[TODO] Reporte de transacciones no reconocidas (MongoDB)")
        elif opcion == "4":
            print("[TODO] Visualización de historial financiero (Cassandra)")
        elif opcion == "5":
            print("[TODO] Gestión de beneficiarios frecuentes (Dgraph)")
        elif opcion == "6":
            print("[TODO] Configuración de límites personalizados (MongoDB)")
        elif opcion == "7":
            print("[TODO] Visualización de notificaciones (MongoDB)")
        elif opcion == "8":
            print("\n=== Crear nueva cuenta ===")
            account_id = input("ID de cuenta: ")
            account_type = input("Tipo de cuenta (ahorro, nómina, etc.): ")
            currency = input("Moneda (MXN, USD, etc.): ")
            try:
                balance = float(input("Saldo inicial: "))
            except ValueError:
                print("Error: El saldo debe ser un número.")
                continue

            cuenta = {
                "account_id": account_id,
                "account_type": account_type,
                "balance": balance,
                "currency": currency,
                "status": "activa"
            }

            crear_cuenta_para_usuario(usuario_id, cuenta)
            print("Cuenta creada y asociada exitosamente.")
        elif opcion == "9":
            print("Saliendo del menú de usuario.")
            break
        else:
            print("Opción no válida.")
