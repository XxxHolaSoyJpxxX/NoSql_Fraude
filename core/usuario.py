from db.Dgraph.dgraph import obtener_cuentas_usuario, crear_cuenta_para_usuario, obtener_beneficiarios, agregar_beneficiario, eliminar_beneficiario
from db.Cassandra.cassandra import obtener_ultimas_transacciones_usuario, registrar_transaccion, obtener_transacciones_cuenta, obtener_estadisticas_usuario
from db.MongoDB.mongo import guardar_reporte_fraude, guardar_notificacion, obtener_notificaciones, marcar_notificacion_leida, guardar_limite_personalizado, obtener_limites_usuario

def menu_usuario(usuario_id):
    """
    Menú principal del usuario que permite realizar diversas operaciones bancarias
    """
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
            administrar_cuentas(usuario_id)
        elif opcion == "2":
            realizar_transaccion(usuario_id)
        elif opcion == "3":
            reportar_fraude(usuario_id)
        elif opcion == "4":
            ver_historial_financiero(usuario_id)
        elif opcion == "5":
            gestionar_beneficiarios(usuario_id)
        elif opcion == "6":
            configurar_limites(usuario_id)
        elif opcion == "7":
            ver_notificaciones(usuario_id)
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
            print("❌ Error al crear la cuenta.")
            
    except ValueError:
        print("Error: El saldo debe ser un número.")
