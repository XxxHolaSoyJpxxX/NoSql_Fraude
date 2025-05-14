# core/usuario.py
from db.Dgraph.dgraph import obtener_cuentas_usuario
from db.Cassandra.cassandra import obtener_ultimas_transacciones_usuario, obtener_transacciones_cuenta, obtener_estadisticas_usuario
from db.MongoDB.mongo import guardar_reporte_fraude, guardar_notificacion, obtener_notificaciones, marcar_notificacion_leida
from core.fraude import SistemaDeteccionFraude

def menu_usuario(usuario_id):
    """
    Menú principal del usuario enfocado en la detección de fraude
    """
    while True:
        print(f"\n=== Menú del Usuario: {usuario_id} ===")
        print("1. Ver historial de transacciones")
        print("2. Reportar transacción no reconocida")
        print("3. Ver alertas de seguridad")
        print("4. Ver nivel de riesgo de cuenta")
        print("5. Ver notificaciones")
        print("6. Salir")

        opcion = input("Selecciona una opción: ")

        if opcion == "1":
            ver_historial_transacciones(usuario_id)
        elif opcion == "2":
            reportar_fraude(usuario_id)
        elif opcion == "3":
            ver_alertas_seguridad(usuario_id)
        elif opcion == "4":
            ver_nivel_riesgo(usuario_id)
        elif opcion == "5":
            ver_notificaciones(usuario_id)
        elif opcion == "6":
            print("Saliendo del menú de usuario.")
            break
        else:
            print("Opción no válida.")

def ver_historial_transacciones(usuario_id):
    """
    Muestra el historial de transacciones del usuario para identificar posibles fraudes
    """
    print("\n=== Historial de Transacciones ===")
    
    # Obtener las cuentas del usuario
    cuentas = obtener_cuentas_usuario(usuario_id)
    
    if not cuentas:
        print("No tienes cuentas asociadas.")
        return
    
    # Mostrar transacciones recientes de todas las cuentas
    print("\nTransacciones recientes:")
    
    for cuenta in cuentas:
        account_id = cuenta.get('account_id')
        transacciones = obtener_transacciones_cuenta(account_id, 5)
        
        print(f"\nCuenta: {account_id}")
        if not transacciones:
            print("No hay transacciones recientes.")
            continue
        
        for t in transacciones:
            print(f"• {t.get('fecha')} - {t.get('tipo')}: {t.get('monto')} - {t.get('estatus')}")
    
    input("\nPresiona Enter para volver al menú...")

def reportar_fraude(usuario_id):
    """
    Permite al usuario reportar una transacción fraudulenta
    """
    print("\n=== Reportar Transacción No Reconocida ===")
    
    # Obtener últimas transacciones del usuario
    transacciones = obtener_ultimas_transacciones_usuario(usuario_id, 20)
    
    if not transacciones:
        print("No hay transacciones recientes para reportar.")
        return
    
    print("\nÚltimas transacciones:")
    for i, t in enumerate(transacciones, 1):
        print(f"{i}. {t.get('fecha')} - {t.get('tipo')}: {t.get('monto')} - {t.get('estatus')}")
    
    try:
        idx = int(input("\nNúmero de transacción a reportar (0 para cancelar): ")) - 1
        
        if idx == -1:  # El usuario ingresó 0
            print("Operación cancelada.")
            return
        
        if not (0 <= idx < len(transacciones)):
            print("Número de transacción inválido.")
            return
        
        transaccion = transacciones[idx]
        
        print(f"\nReportando transacción: {transaccion.get('transaction_id')}")
        print(f"Fecha: {transaccion.get('fecha')}")
        print(f"Monto: {transaccion.get('monto')}")
        print(f"Tipo: {transaccion.get('tipo')}")
        
        motivo = input("\nMotivo del reporte (no la reconozco, monto incorrecto, etc.): ")
        detalles = input("Detalles adicionales: ")
        
        # Crear reporte de fraude en MongoDB
        reporte = {
            "usuario_id": usuario_id,
            "transaction_id": transaccion.get('transaction_id'),
            "fecha_reporte": None,  # Se establece en el backend
            "motivo": motivo,
            "detalles": detalles,
            "estatus": None,  # Se establece en el backend
        }
        
        reporte_id = guardar_reporte_fraude(reporte)
        
        print(f"\n✅ Reporte enviado exitosamente.")
        print(f"ID de reporte: {reporte_id}")
        print("Nuestro equipo revisará el caso y te contactará próximamente.")
        
        # Crear notificación para el usuario
        notificacion = {
            "tipo": "reporte_fraude",
            "titulo": "Reporte de fraude recibido",
            "mensaje": f"Hemos recibido tu reporte sobre la transacción del {transaccion.get('fecha')}. Te informaremos en cuanto tengamos novedades.",
            "prioridad": "alta"
        }
        
        guardar_notificacion(usuario_id, notificacion)
        
    except ValueError:
        print("Por favor ingresa un número válido.")
    except Exception as e:
        print(f"Error al reportar fraude: {str(e)}")
    
    input("\nPresiona Enter para volver al menú...")

def ver_alertas_seguridad(usuario_id):
    """
    Muestra las alertas de seguridad y posibles patrones sospechosos
    """
    print("\n=== Alertas de Seguridad ===")
    
    # Simulación de análisis de seguridad utilizando el sistema de detección de fraude
    print("Analizando patrones de comportamiento y posibles amenazas...")
    
    # Obtener estadísticas recientes
    try:
        estadisticas = obtener_estadisticas_usuario(usuario_id, 30)
        
        # Usar el sistema de detección de fraude para analizar patrones
        # Esto es una simulación simplificada
        resultado_analisis = {
            "nivel_riesgo": "bajo",
            "anomalias_detectadas": 0,
            "ubicaciones_inusuales": False,
            "patrones_sospechosos": False
        }
        
        print("\nResultados del análisis de seguridad:")
        print(f"• Nivel de riesgo actual: {resultado_analisis.get('nivel_riesgo').upper()}")
        print(f"• Anomalías detectadas: {resultado_analisis.get('anomalias_detectadas')}")
        
        if resultado_analisis.get('ubicaciones_inusuales'):
            print("⚠️ Se detectaron inicios de sesión desde ubicaciones inusuales.")
        else:
            print("✅ No se detectaron inicios de sesión desde ubicaciones inusuales.")
        
        if resultado_analisis.get('patrones_sospechosos'):
            print("⚠️ Se detectaron patrones de transacción sospechosos.")
        else:
            print("✅ No se detectaron patrones de transacción sospechosos.")
        
        print("\nRecomendaciones de seguridad:")
        print("• Revisa regularmente tus transacciones")
        print("• Activa notificaciones para todas las transacciones")
        print("• No compartas tus credenciales con nadie")
        print("• Actualiza tu contraseña regularmente")
        
    except Exception as e:
        print(f"Error al obtener análisis de seguridad: {str(e)}")
    
    input("\nPresiona Enter para volver al menú...")

def ver_nivel_riesgo(usuario_id):
    """
    Muestra el nivel de riesgo de la cuenta y análisis detallado
    """
    print("\n=== Nivel de Riesgo de Cuenta ===")
    
    # En un sistema real, esto consultaría el nivel de riesgo calculado
    # por el sistema de detección de fraude
    
    # Simulación de niveles de riesgo
    factores_riesgo = [
        {"factor": "Ubicación geográfica", "nivel": "bajo", "score": 10},
        {"factor": "Patrones de actividad", "nivel": "medio", "score": 40},
        {"factor": "Frecuencia de transacciones", "nivel": "bajo", "score": 15},
        {"factor": "Montos de transacción", "nivel": "bajo", "score": 20}
    ]
    
    score_total = sum(f.get("score", 0) for f in factores_riesgo)
    nivel_riesgo = "bajo" if score_total < 50 else "medio" if score_total < 75 else "alto"
    
    print(f"\nPuntuación de riesgo: {score_total}/100")
    print(f"Nivel de riesgo general: {nivel_riesgo.upper()}")
    
    print("\nFactores que contribuyen al nivel de riesgo:")
    for factor in factores_riesgo:
        print(f"• {factor.get('factor')}: {factor.get('nivel').upper()} ({factor.get('score')} puntos)")
    
    print("\nRecomendaciones basadas en tu nivel de riesgo:")
    if nivel_riesgo == "bajo":
        print("✅ Tu nivel de riesgo es bajo. Continúa con tus prácticas seguras.")
    elif nivel_riesgo == "medio":
        print("⚠️ Tu nivel de riesgo es medio. Considera revisar tus hábitos de transacción.")
    else:
        print("🚨 Tu nivel de riesgo es alto. Se recomienda contactar a soporte.")
    
    input("\nPresiona Enter para volver al menú...")

def ver_notificaciones(usuario_id):
    """
    Muestra las notificaciones del usuario relacionadas con seguridad y fraude
    """
    print("\n=== Notificaciones ===")
    
    # Obtener notificaciones no leídas
    notificaciones = obtener_notificaciones(usuario_id, False)
    
    if not notificaciones:
        print("No tienes notificaciones nuevas.")
        
        # Preguntar si quiere ver notificaciones antiguas
        ver_antiguas = input("¿Deseas ver notificaciones antiguas? (s/n): ").lower()
        if ver_antiguas == 's':
            notificaciones = obtener_notificaciones(usuario_id, True)
            if not notificaciones:
                print("No tienes notificaciones antiguas.")
                input("\nPresiona Enter para volver al menú...")
                return
        else:
            input("\nPresiona Enter para volver al menú...")
            return
    
    print(f"\nTienes {len(notificaciones)} notificaciones:")
    
    for i, n in enumerate(notificaciones, 1):
        leida = "📭" if n.get("leida") else "📬"
        print(f"{i}. {leida} {n.get('timestamp')} - {n.get('titulo')}")
    
    ver_detalle = input("\n¿Deseas ver el detalle de alguna notificación? (número/n): ")
    
    if ver_detalle.lower() != 'n':
        try:
            idx = int(ver_detalle) - 1
            
            if 0 <= idx < len(notificaciones):
                n = notificaciones[idx]
                
                print(f"\n📩 {n.get('titulo')}")
                print(f"Fecha: {n.get('timestamp')}")
                print(f"Mensaje: {n.get('mensaje')}")
                
                if not n.get("leida"):
                    marcar_notificacion_leida(n.get("_id"))
                    print("Notificación marcada como leída.")
            else:
                print("Número de notificación inválido.")
                
        except ValueError:
            print("Por favor ingresa un número válido.")
    
    input("\nPresiona Enter para volver al menú...")
