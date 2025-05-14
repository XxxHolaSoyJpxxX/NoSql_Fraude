from core.fraude import SistemaDeteccionFraude
import os
import time
from datetime import datetime

def menu_admin():
    """
    Panel de administración principal para el sistema de detección de fraude
    """
    admin_id = None  # En un sistema real, se pasaría como parámetro
    
    while True:
        print("\n=== Panel de Administración ===")
        print("1. Dashboard de monitoreo en tiempo real") #Todas las transacciones de todos los usuarios
        print("2. Ver todos los fraudes") 
        print("5. Reportes y estadísticas de fraude") #estadisticas de tipo de fraude
        print("6. Gestión de perfiles de riesgo") #top global de perfiles de fraude
        print("7. Listas de vigilancia") #ver todos los perfiles con fraude
        print("9. Auditoría de acciones de administradores") #mostrar toda la info de un usuario en especifico
        #print("10. Configuración de umbrales de alerta")
        print("12. Bloquear cuenta") #bloquear transacciones de un usuario
        print("1. Desbloquear cuenta")
        print("13. Aprobación de transacciones de alto riesgo") #cambiar el status de una transaccion fraude
        print("16. Visualización de anomalías geográficas") #ver solamente los fraudes geograficos
        print("19. Análisis de comportamiento de administradores") #ver todas las acciones de cuentas de administradores
        print("1. Soporte")
        print("1. Notificar usuario")
        print("21. Salir")

        opcion = input("Selecciona una opción: ")

        if opcion == "1":
            dashboard_monitoreo(admin_id)
        elif opcion == "2":
            gestion_reglas_fraude(admin_id)
        elif opcion == "3":
            casos_investigacion(admin_id)
        elif opcion == "4":
            analisis_redes(admin_id)
        elif opcion == "5":
            reportes_estadisticas(admin_id)
        elif opcion == "6":
            gestion_perfiles_riesgo(admin_id)
        elif opcion == "7":
            listas_vigilancia(admin_id)
        elif opcion == "8":
            alertas_masivas(admin_id)
        elif opcion == "9":
            auditoria_acciones(admin_id)
        elif opcion == "10":
            configuracion_umbrales(admin_id)
        elif opcion == "11":
            simulacion_fraude(admin_id)
        elif opcion == "12":
            gestion_bloqueos(admin_id)
        elif opcion == "13":
            aprobacion_transacciones(admin_id)
        elif opcion == "14":
            cumplimiento_regulatorio(admin_id)
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

