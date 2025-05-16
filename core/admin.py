import os
import time
from datetime import datetime

from db.Cassandra.cassandra import mostrar_todas_transacciones, ver_acciones_de_admin, ver_todas_las_acciones_admin
from db.MongoDB.mongo import auditoria, bloquear_cuenta, desbloquear_cuenta

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
        print("6. Auditoría de acciones de administradores") #mostrar toda la info de un usuario en especifico
        #print("10. Configuración de umbrales de alerta")
        print("7. Bloquear cuenta") #bloquear transacciones de un usuario
        print("8. Desbloquear cuenta")
        print("9. Aprobación de transacciones de alto riesgo") #cambiar el status de una transaccion fraude
        print("10. Visualización de anomalías geográficas") #ver solamente los fraudes geograficos
        print("11. Análisis de comportamiento de administradores") #ver todas las acciones de cuentas de administradores
        print("12. Auditoria de administrador")
        print("12. Soporte")
        print("13. Notificar usuario")
        print("14. Salir")

        opcion = input("Selecciona una opción: ")

        if opcion == "1":
            mostrar_todas_transacciones(admin_id)
        elif opcion == "2":
            gestion_reglas_fraude(admin_id)
        elif opcion == "3":
            casos_investigacion(admin_id)
        elif opcion == "4":
            analisis_redes(admin_id)
        elif opcion == "5":
            reportes_estadisticas(admin_id)
        elif opcion == "6":
            auditoria(admin_id)
        elif opcion == "7":
            bloquear_cuenta(admin_id)
        elif opcion == "8":
            desbloquear_cuenta(admin_id)
        elif opcion == "9":
            auditoria_acciones(admin_id)
        elif opcion == "10":
            ver_todas_las_acciones_admin(admin_id)
        elif opcion == "11":
            ver_todas_las_acciones_admin(admin_id)
        elif opcion == "12":
            ver_acciones_de_admin(admin_id)
        elif opcion == "13":
            aprobacion_transacciones(admin_id)
        elif opcion == "14":
            break
        elif opcion == "15":
            identificacion_patrones(admin_id)
        elif opcion == "16":
            anomalias_geograficas(admin_id)
        elif opcion == "17":
            respuesta_incidentes(admin_id)
        elif opcion == "18":
            verificacion_identidad(admin_id)
        elif opcion == "19":
            analisis_administradores(admin_id)
        elif opcion == "20":
            integracion_sistemas_externos(admin_id)
        elif opcion == "21":
            break
        else:
            print("Opción no válida.")

