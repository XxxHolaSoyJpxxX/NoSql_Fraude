# core/admin.py
from db.MongoDB.mongo import (
    obtener_usuarios_bajo_vigilancia, 
    agregar_usuario_vigilancia, 
    remover_usuario_vigilancia,
    registrar_accion_admin, 
    obtener_reportes_fraude,
    obtener_configuracion_reglas_fraude,
    actualizar_regla_fraude,
    agregar_regla_fraude,
    eliminar_regla_fraude,
    obtener_usuario
)
from db.Cassandra.cassandra import (
    obtener_alertas_fraude_usuario,
    actualizar_estatus_alerta,
    obtener_acciones_admin,
    obtener_estadisticas_usuario,
    crear_tablas
)
from db.Dgraph.dgraph import (
    obtener_usuarios,
    obtener_cuentas,
    obtener_transacciones
)
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

def dashboard_monitoreo(admin_id):
    """
    Dashboard para monitorear transacciones y alertas en tiempo real
    """
    print("\n=== Dashboard de Monitoreo en Tiempo Real ===")
    
    # Registro de actividad administrativa
    registrar_accion_admin(admin_id, "acceso_dashboard", {"seccion": "monitoreo"})
    
    # Obtener datos de las diferentes bases de datos
    try:
        # Transacciones recientes de Dgraph
        transacciones = obtener_transacciones()
        transacciones_recientes = transacciones.get('transacciones', [])[:5]
        
        # Alertas recientes de Cassandra (ejemplo con usuario random)
        alertas = obtener_alertas_fraude_usuario(None, 5)
        
        # Mostrar resumen
        print("\n📊 Resumen de actividad reciente:")
        print(f"• Transacciones en las últimas 24h: {len(transacciones_recientes)}")
        print(f"• Alertas de fraude activas: {len(alertas)}")
        
        # Mostrar transacciones recientes
        if transacciones_recientes:
            print("\n🔄 Últimas transacciones:")
            for t in transacciones_recientes:
                print(f"• ID: {t.get('transaction_id')} - Monto: {t.get('amount')} - Estado: {t.get('transaction_status')}")
        
        # Mostrar alertas recientes
        if alertas:
            print("\n⚠️ Alertas recientes:")
            for a in alertas:
                print(f"• {a.get('fecha')} - {a.get('tipo_alerta')} - Nivel: {a.get('nivel_riesgo')} - Estado: {a.get('estatus')}")
        
        input("\nPresiona Enter para volver al menú principal...")
    
    except Exception as e:
        print(f"Error al cargar el dashboard: {str(e)}")
        input("Presiona Enter para volver al menú principal...")

def gestion_reglas_fraude(admin_id):
    """
    Permite gestionar las reglas de detección de fraude
    """
    print("\n=== Gestión de Reglas de Detección de Fraude ===")
    print("[TODO] Implementación simple de gestión de reglas de fraude")
    
    # Registrar acción administrativa
    registrar_accion_admin(admin_id, "acceso_reglas_fraude", {
        "detalles": "Acceso al módulo de gestión de reglas de fraude"
    })
    
    input("\nPresiona Enter para volver al menú principal...")

def listas_vigilancia(admin_id):
    """
    Gestiona la lista de usuarios bajo vigilancia especial
    """
    print("\n=== Listas de Vigilancia ===")
    print("[TODO] Implementación simple del sistema de listas de vigilancia")
    
    # Registrar acción administrativa
    registrar_accion_admin(admin_id, "acceso_vigilancia", {
        "detalles": "Acceso al módulo de listas de vigilancia"
    })
    
    input("\nPresiona Enter para volver al menú principal...")

def auditoria_acciones(admin_id):
    """
    Muestra el registro de acciones realizadas por administradores
    """
    print("\n=== Auditoría de Acciones de Administradores ===")
    print("[TODO] Implementación simple de la auditoría de acciones")
    
    # Registrar acción administrativa
    registrar_accion_admin(admin_id, "consulta_auditoria", {
        "detalles": "Consulta del registro de auditoría"
    })
    
    input("\nPresiona Enter para volver al menú principal...")

def casos_investigacion(admin_id):
    """
    Sistema de gestión de casos de investigación de fraude
    """
    print("\n=== Sistema de Casos de Investigación ===")
    print("[TODO] Implementación simple del sistema de casos de investigación")
    
    # Registrar acción administrativa
    registrar_accion_admin(admin_id, "acceso_casos", {
        "detalles": "Acceso al sistema de casos de investigación"
    })
    
    input("\nPresiona Enter para volver al menú principal...")

# Implementar el resto de las funciones del menú de forma similar
# Estas son funciones dummy para completar el menú

def analisis_redes(admin_id):
    print("\n[TODO] Análisis de redes (Dgraph)")
    input("Presiona Enter para volver al menú principal...")

def reportes_estadisticas(admin_id):
    print("\n[TODO] Reportes y estadísticas de fraude")
    input("Presiona Enter para volver al menú principal...")

def gestion_perfiles_riesgo(admin_id):
    print("\n[TODO] Gestión de perfiles de riesgo")
    input("Presiona Enter para volver al menú principal...")

def alertas_masivas(admin_id):
    print("\n[TODO] Gestión de alertas masivas")
    input("Presiona Enter para volver al menú principal...")

def configuracion_umbrales(admin_id):
    print("\n[TODO] Configuración de umbrales de alerta")
    input("Presiona Enter para volver al menú principal...")

def simulacion_fraude(admin_id):
    print("\n[TODO] Simulación de fraude")
    input("Presiona Enter para volver al menú principal...")

def gestion_bloqueos(admin_id):
    print("\n[TODO] Gestión de bloqueos automáticos")
    input("Presiona Enter para volver al menú principal...")

def aprobacion_transacciones(admin_id):
    print("\n[TODO] Aprobación de transacciones de alto riesgo")
    input("Presiona Enter para volver al menú principal...")

def cumplimiento_regulatorio(admin_id):
    print("\n[TODO] Panel de cumplimiento regulatorio")
    input("Presiona Enter para volver al menú principal...")

def identificacion_patrones(admin_id):
    print("\n[TODO] Detección de patrones emergentes (Dgraph)")
    input("Presiona Enter para volver al menú principal...")

def anomalias_geograficas(admin_id):
    print("\n[TODO] Visualización de anomalías geográficas")
    input("Presiona Enter para volver al menú principal...")

def respuesta_incidentes(admin_id):
    print("\n[TODO] Respuesta a incidentes")
    input("Presiona Enter para volver al menú principal...")

def verificacion_identidad(admin_id):
    print("\n[TODO] Gestión de verificación de identidad")
    input("Presiona Enter para volver al menú principal...")

def analisis_administradores(admin_id):
    print("\n[TODO] Análisis de comportamiento de administradores")
    input("Presiona Enter para volver al menú principal...")

def integracion_sistemas_externos(admin_id):
    print("\n[TODO] Integración con sistemas externos de fraude")
    input("Presiona Enter para volver al menú principal...")
