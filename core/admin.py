import os
import time
from datetime import datetime
import uuid

from db.Cassandra.cassandra import actualizar_estado_transaccion,mostrar_todas_transacciones, ver_acciones_de_admin, ver_todas_las_acciones_admin
from db.MongoDB.mongo import auditoria, bloquear_cuenta, desbloquear_cuenta,insertar_notificacion
from db.Dgraph.dgraph import obtener_todos_los_reportes, obtener_cuenta_por_email
from core.fraude  import  obtener_reportes_geograficos,estadsitcas_fraude,perfil_riesgo

def menu_admin(admin_id):
    """
    Panel de administración principal para el sistema de detección de fraude
    """
    
    while True:
        print("\n=== Panel de Administración ===")
        print("1. Dashboard de monitoreo en tiempo real") #Todas las transacciones de todos los usuarios
        print("2. Ver todos los fraudes") 
        print("3. Reportes y estadísticas de fraude") #estadisticas de tipo de fraude
        print("4. Gestión de perfiles de riesgo") #top global de perfiles de fraude
        print("5. Listas de vigilancia") #ver todos los perfiles con fraude
        print("6. Auditoría de Usuario") #mostrar toda la info de un usuario en especifico
        #print("10. Configuración de umbrales de alerta")
        print("7. Bloquear cuenta") #bloquear transacciones de un usuario
        print("8. Desbloquear cuenta")
        print("9. Aprobación de transacciones de alto riesgo") #cambiar el status de una transaccion fraude
        print("10. Visualización de anomalías geográficas") #ver solamente los fraudes geograficos
        print("11. Análisis de comportamiento de administradores") #ver todas las acciones de cuentas de administradores
        print("12. Auditoria de administrador")
        print("13. Soporte")
        print("14. Notificar usuario")
        print("15. Salir")

        opcion = input("Selecciona una opción: ")

        if opcion == "1":
            mostrar_todas_transacciones(admin_id)
        elif opcion == "2":
            print(obtener_todos_los_reportes())
        elif opcion == "3":
            estadsitcas_fraude()
        elif opcion == "4":
            print("=== Gestión de Perfiles de Riesgo ===")
            print("Cuantos perfiles de riesgo?")
            cantidad = 5
            cantidad_input = int(input("Cantidad de perfiles de riesgo a mostrar default 5: "))
            if cantidad_input > 0:
                cantidad = cantidad_input
            perfil_riesgo(cantidad)
        elif opcion == "5":
            perfil_riesgo(0)
        elif opcion == "6":
            auditoria(admin_id)
        elif opcion == "7":
            bloquear_cuenta(admin_id)
        elif opcion == "8":
            desbloquear_cuenta(admin_id)
        elif opcion == "9":
            transaction_id = input("Ingrese el ID de la transacción a aprobar: ")
            id_transaccion = uuid.UUID(transaction_id)
            actualizar_estado_transaccion( id_transaccion, "completada")
        elif opcion == "10":
             obtener_reportes_geograficos()
        elif opcion == "11":
            ver_todas_las_acciones_admin(admin_id)
        elif opcion == "12":
            ver_acciones_de_admin(admin_id)
        elif opcion == "13":
            print("=== Soporte ===")
            print("1. Crear ticket de soporte")
            print("2. Ver tickets de soporte")
            print("3. Responder ticket de soporte")
            print("4. Cerrar ticket de soporte")
            print("5. Salir")

            sub_opcion = input("Selecciona una opción: ")

            if sub_opcion == "1":
                # Lógica para crear un ticket de soporte
                pass
            elif sub_opcion == "2":
                # Lógica para ver tickets de soporte
                pass
            elif sub_opcion == "3":
                # Lógica para responder un ticket de soporte
                pass
            elif sub_opcion == "4":
                # Lógica para cerrar un ticket de soporte
                pass
            elif sub_opcion == "5":
                continue
            else:
                print("Opción no válida.")
        elif opcion == "14":
            email_destino = input("Ingrese el email del usario a notificar: ")
            id_origen = obtener_cuenta_por_email(email_destino)
            mensaje = input("Ingrese el mensaje a enviar: ")
            insertar_notificacion(
            id_origen,
            f"{mensaje} enviado por {admin_id}."
            )
        elif opcion == "15":
            break
        else:
            print("Opción no válida.")

