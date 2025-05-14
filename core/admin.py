# core/admin.py

def menu_admin():
    while True:
        print("\n=== Panel de Administración ===")
        print("1. Dashboard de monitoreo en tiempo real")
        print("2. Gestión de reglas de detección de fraude")
        print("3. Sistema de casos de investigación")
        print("4. Herramientas de análisis de redes")
        print("5. Reportes y estadísticas de fraude")
        print("6. Gestión de perfiles de riesgo")
        print("7. Listas de vigilancia")
        print("8. Alertas masivas")
        print("9. Auditoría de acciones de administradores")
        print("10. Configuración de umbrales de alerta")
        print("11. Simulación de fraude")
        print("12. Gestión de bloqueos automáticos")
        print("13. Aprobación de transacciones de alto riesgo")
        print("14. Cumplimiento regulatorio")
        print("15. Identificación de patrones emergentes")
        print("16. Visualización de anomalías geográficas")
        print("17. Respuesta a incidentes de fraude")
        print("18. Verificación de identidad")
        print("19. Análisis de comportamiento de administradores")
        print("20. Integración con sistemas externos")
        print("21. Salir")

        opcion = input("Selecciona una opción: ")

        if opcion == "1":
            print("[TODO] Dashboard de monitoreo en tiempo real")
        elif opcion == "2":
            print("[TODO] Gestión de reglas de detección de fraude")
        elif opcion == "3":
            print("[TODO] Sistema de casos de investigación")
        elif opcion == "4":
            print("[TODO] Análisis de redes (Dgraph)")
        elif opcion == "5":
            print("[TODO] Reportes y estadísticas de fraude")
        elif opcion == "6":
            print("[TODO] Gestión de perfiles de riesgo")
        elif opcion == "7":
            print("[TODO] Sistema de listas de vigilancia")
        elif opcion == "8":
            print("[TODO] Gestión de alertas masivas")
        elif opcion == "9":
            print("[TODO] Auditoría de acciones de administradores (Cassandra)")
        elif opcion == "10":
            print("[TODO] Configuración de umbrales de alerta")
        elif opcion == "11":
            print("[TODO] Simulación de fraude")
        elif opcion == "12":
            print("[TODO] Gestión de bloqueos automáticos")
        elif opcion == "13":
            print("[TODO] Aprobación de transacciones riesgosas")
        elif opcion == "14":
            print("[TODO] Panel de cumplimiento regulatorio")
        elif opcion == "15":
            print("[TODO] Detección de patrones emergentes (Dgraph)")
        elif opcion == "16":
            print("[TODO] Visualización de anomalías geográficas")
        elif opcion == "17":
            print("[TODO] Respuesta a incidentes")
        elif opcion == "18":
            print("[TODO] Gestión de verificación de identidad")
        elif opcion == "19":
            print("[TODO] Análisis de comportamiento de administradores")
        elif opcion == "20":
            print("[TODO] Integración con sistemas externos de fraude")
        elif opcion == "21":
            break
        else:
            print("Opción no válida.")
