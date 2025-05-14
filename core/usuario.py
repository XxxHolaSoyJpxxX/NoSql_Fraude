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
            crear_nueva_cuenta(usuario_id)
        elif opcion == "9":
            print("Saliendo del menú de usuario.")
            break
        else:
            print("Opción no válida.")

def administrar_cuentas(usuario_id):
    """
    Muestra y permite administrar las cuentas del usuario
    """
    while True:
        print("\n=== Administración de Cuentas ===")
        cuentas = obtener_cuentas_usuario(usuario_id)
        
        if not cuentas:
            print("🔍 No tienes cuentas asociadas.")
            return
        
        print("\nTus cuentas:")
        for i, c in enumerate(cuentas, 1):
            print(f"{i}. {c['account_type'].capitalize()} ({c['account_id']}): {c['balance']} {c['currency']} - Estado: {c['status']}")
        
        print("\nOpciones:")
        print("1. Ver detalles de cuenta")
        print("2. Ver transacciones recientes")
        print("3. Volver al menú principal")
        
        sub_opcion = input("Selecciona una opción: ")
        
        if sub_opcion == "1":
            idx = int(input("Número de cuenta a ver: ")) - 1
            if 0 <= idx < len(cuentas):
                print("\nDetalles de la cuenta:")
                for key, value in cuentas[idx].items():
                    print(f"• {key}: {value}")
            else:
                print("Número de cuenta inválido.")
        
        elif sub_opcion == "2":
            idx = int(input("Número de cuenta a ver transacciones: ")) - 1
            if 0 <= idx < len(cuentas):
                account_id = cuentas[idx]['account_id']
                transacciones = obtener_transacciones_cuenta(account_id, 10)
                
                if not transacciones:
                    print(f"No hay transacciones recientes para la cuenta {account_id}")
                else:
                    print(f"\nÚltimas transacciones de la cuenta {account_id}:")
                    for t in transacciones:
                        print(f"• {t['fecha']} - {t['tipo']}: {t['monto']} {cuentas[idx]['currency']} - {t['estatus']}")
            else:
                print("Número de cuenta inválido.")
        
        elif sub_opcion == "3":
            break
        
        else:
            print("Opción no válida.")

def realizar_transaccion(usuario_id):
    """
    Permite al usuario realizar una transacción
    """
    print("\n=== Realizar Transacción ===")
    
    # Obtener cuentas del usuario
    cuentas = obtener_cuentas_usuario(usuario_id)
    if not cuentas:
        print("No tienes cuentas disponibles para realizar transacciones.")
        return
    
    # Seleccionar cuenta de origen
    print("\nSelecciona la cuenta de origen:")
    for i, c in enumerate(cuentas, 1):
        print(f"{i}. {c['account_type'].capitalize()} ({c['account_id']}): {c['balance']} {c['currency']}")
    
    try:
        idx_origen = int(input("Número de cuenta: ")) - 1
        if not (0 <= idx_origen < len(cuentas)):
            print("Número de cuenta inválido.")
            return
        
        cuenta_origen = cuentas[idx_origen]
        
        # Datos de la transacción
        destino_account_id = input("ID de cuenta destino: ")
        monto = float(input(f"Monto a transferir ({cuenta_origen['currency']}): "))
        
        if monto <= 0:
            print("El monto debe ser mayor a cero.")
            return
        
        if monto > float(cuenta_origen['balance']):
            print("Saldo insuficiente para realizar la transacción.")
            return
        
        categoria = input("Categoría (opcional, ej: alimentación, transporte): ") or "general"
        descripcion = input("Descripción (opcional): ") or "Transferencia"
        
        # Preparar datos de la transacción
        datos_transaccion = {
            "usuario_id": usuario_id,
            "account_id": cuenta_origen['account_id'],
            "monto": monto,
            "tipo": "transferencia",
            "origen": cuenta_origen['account_id'],
            "destino": destino_account_id,
            "categoria": categoria,
            "descripcion": descripcion,
            # Datos para Dgraph
            "origen_account_id": cuenta_origen['account_id'],
            "destino_account_id": destino_account_id
        }
        
        # Registro en Cassandra (historial temporal)
        transaction_id = registrar_transaccion(datos_transaccion)
        
        # En un sistema real, aquí se registraría también en Dgraph para actualizar balances
        # y realizar el análisis de fraude
        
        print(f"\n✅ Transacción realizada exitosamente")
        print(f"ID de transacción: {transaction_id}")
        print(f"De: {cuenta_origen['account_id']}")
        print(f"Para: {destino_account_id}")
        print(f"Monto: {monto} {cuenta_origen['currency']}")
        print(f"Categoría: {categoria}")
        
    except ValueError:
        print("Por favor ingresa valores numéricos válidos.")
    except Exception as e:
        print(f"Error al realizar la transacción: {str(e)}")

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
        print(f"{i}. {t['fecha']} - {t['tipo']}: {t['monto']} - {t['estatus']}")
    
    try:
        idx = int(input("\nNúmero de transacción a reportar (0 para cancelar): ")) - 1
        
        if idx == -1:  # El usuario ingresó 0
            print("Operación cancelada.")
            return
        
        if not (0 <= idx < len(transacciones)):
            print("Número de transacción inválido.")
            return
        
        transaccion = transacciones[idx]
        
        print(f"\nReportando transacción: {transaccion['transaction_id']}")
        print(f"Fecha: {transaccion['fecha']}")
        print(f"Monto: {transaccion['monto']}")
        print(f"Tipo: {transaccion['tipo']}")
        
        motivo = input("\nMotivo del reporte (no la reconozco, monto incorrecto, etc.): ")
        detalles = input("Detalles adicionales: ")
        
        # Crear reporte de fraude en MongoDB
        reporte = {
            "transaction_id": transaccion['transaction_id'],
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
            "mensaje": f"Hemos recibido tu reporte sobre la transacción del {transaccion['fecha']}. Te informaremos en cuanto tengamos novedades.",
            "prioridad": "alta"
        }
        
        guardar_notificacion(usuario_id, notificacion)
        
    except ValueError:
        print("Por favor ingresa un número válido.")
    except Exception as e:
        print(f"Error al reportar fraude: {str(e)}")

def ver_historial_financiero(usuario_id):
    """
    Muestra el historial financiero y estadísticas del usuario
    """
    print("\n=== Historial Financiero ===")
    
    # Obtener estadísticas de los últimos 30 días
    estadisticas = obtener_estadisticas_usuario(usuario_id, 30)
    
    if estadisticas["total_transacciones"] == 0:
        print("No hay datos de transacciones para mostrar.")
        return
    
    print("\n📊 Resumen últimos 30 días:")
    print(f"• Total de transacciones: {estadisticas['total_transacciones']}")
    print(f"• Monto total: {estadisticas['monto_total']:.2f}")
    print(f"• Monto promedio por transacción: {estadisticas['monto_promedio']:.2f}")
    print(f"• Transacciones por día: {estadisticas['transacciones_por_dia']:.2f}")
    
    print("\n📋 Desglose por categoría:")
    for categoria, monto in estadisticas["montos_por_categoria"].items():
        print(f"• {categoria}: {monto:.2f}")
    
    print("\n📋 Desglose por tipo:")
    for tipo, monto in estadisticas["montos_por_tipo"].items():
        print(f"• {tipo}: {monto:.2f}")
    
    # Ver transacciones específicas
    ver_mas = input("\n¿Deseas ver transacciones específicas? (s/n): ").lower()
    if ver_mas == 's':
        transacciones = obtener_ultimas_transacciones_usuario(usuario_id, 30)
        
        print("\nÚltimas 30 transacciones:")
        for t in transacciones:
            print(f"• {t['fecha']} - {t['tipo']}: {t['monto']} - {t['estatus']} - {t.get('categoria', 'general')}")

def gestionar_beneficiarios(usuario_id):
    """
    Permite gestionar los beneficiarios frecuentes
    """
    while True:
        print("\n=== Gestión de Beneficiarios Frecuentes ===")
        print("1. Ver beneficiarios")
        print("2. Agregar beneficiario")
        print("3. Eliminar beneficiario")
        print("4. Volver al menú principal")
        
        opcion = input("Selecciona una opción: ")
        
        if opcion == "1":
            beneficiarios = obtener_beneficiarios(usuario_id)
            
            if not beneficiarios:
                print("No tienes beneficiarios registrados.")
            else:
                print("\nTus beneficiarios:")
                for i, b in enumerate(beneficiarios, 1):
                    apodo = f" ({b['apodo']})" if b.get('apodo') else ""
                    print(f"{i}. {b['nombre']}{apodo} - {b['user_id']}")
                    print(f"   Tipo: {b['tipo']} | Transacciones: {b['transacciones']}")
        
        elif opcion == "2":
            beneficiario_id = input("ID de usuario beneficiario: ")
            apodo = input("Apodo (opcional): ")
            
            if agregar_beneficiario(usuario_id, beneficiario_id, apodo):
                print(f"✅ Beneficiario {beneficiario_id} agregado correctamente.")
            else:
                print("❌ Error al agregar beneficiario.")
        
        elif opcion == "3":
            beneficiarios = obtener_beneficiarios(usuario_id)
            
            if not beneficiarios:
                print("No tienes beneficiarios para eliminar.")
                continue
            
            print("\nSelecciona el beneficiario a eliminar:")
            for i, b in enumerate(beneficiarios, 1):
                print(f"{i}. {b['nombre']} - {b['user_id']}")
            
            try:
                idx = int(input("Número de beneficiario (0 para cancelar): ")) - 1
                
                if idx == -1:
                    print("Operación cancelada.")
                    continue
                
                if not (0 <= idx < len(beneficiarios)):
                    print("Número de beneficiario inválido.")
                    continue
                
                beneficiario = beneficiarios[idx]
                
                if eliminar_beneficiario(usuario_id, beneficiario['user_id']):
                    print(f"✅ Beneficiario {beneficiario['nombre']} eliminado correctamente.")
                else:
                    print("❌ Error al eliminar beneficiario.")
                
            except ValueError:
                print("Por favor ingresa un número válido.")
        
        elif opcion == "4":
            break
        
        else:
            print("Opción no válida.")

def configurar_limites(usuario_id):
    """
    Permite configurar límites personalizados para transacciones
    """
    print("\n=== Configuración de Límites Personalizados ===")
    
    # Obtener límites actuales
    limites = obtener_limites_usuario(usuario_id)
    
    if limites:
        print("\nLímites actuales:")
        for tipo, valor in limites.items():
            print(f"• {tipo}: {valor}")
    else:
        print("No tienes límites configurados.")
    
    print("\nTipos de límites disponibles:")
    print("1. diario - Límite de gasto diario")
    print("2. transferencia - Límite por transferencia")
    print("3. retiro - Límite por retiro")
    print("4. compra - Límite por compra")
    print("5. internacional - Límite para operaciones internacionales")
    
    tipo_limite = input("\nSelecciona el tipo de límite a configurar: ")
    
    tipos_validos = {
        "1": "diario",
        "2": "transferencia",
        "3": "retiro",
        "4": "compra",
        "5": "internacional"
    }
    
    if tipo_limite in tipos_validos:
        tipo_limite = tipos_validos[tipo_limite]
    elif tipo_limite not in ["diario", "transferencia", "retiro", "compra", "internacional"]:
        print("Tipo de límite no válido.")
        return
    
    try:
        valor = float(input(f"Valor del límite para '{tipo_limite}': "))
        
        if valor <= 0:
            print("El valor debe ser mayor a cero.")
            return
        
        guardar_limite_personalizado(usuario_id, {"tipo": tipo_limite, "valor": valor})
        print(f"✅ Límite para '{tipo_limite}' configurado a {valor}.")
        
    except ValueError:
        print("Por favor ingresa un valor numérico válido.")

def ver_notificaciones(usuario_id):
    """
    Muestra las notificaciones del usuario
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
                return
        else:
            return
    
    print(f"\nTienes {len(notificaciones)} notificaciones:")
    
    for i, n in enumerate(notificaciones, 1):
        leida = "📭" if n.get("leida") else "📬"
        print(f"{i}. {leida} {n['timestamp']} - {n['titulo']}")
    
    ver_detalle = input("\n¿Deseas ver el detalle de alguna notificación? (número/n): ")
    
    if ver_detalle.lower() != 'n':
        try:
            idx = int(ver_detalle) - 1
            
            if 0 <= idx < len(notificaciones):
                n = notificaciones[idx]
                
                print(f"\n📩 {n['titulo']}")
                print(f"Fecha: {n['timestamp']}")
                print(f"Mensaje: {n['mensaje']}")
                
                if not n.get("leida"):
                    marcar_notificacion_leida(n["_id"])
                    print("Notificación marcada como leída.")
            else:
                print("Número de notificación inválido.")
                
        except ValueError:
            print("Por favor ingresa un número válido.")

def crear_nueva_cuenta(usuario_id):
    """
    Permite al usuario crear una nueva cuenta
    """
    print("\n=== Crear nueva cuenta ===")
    
    account_id = input("ID de cuenta: ")
    account_type = input("Tipo de cuenta (ahorro, nómina, etc.): ")
    currency = input("Moneda (MXN, USD, etc.): ")
    
    try:
        balance = float(input("Saldo inicial: "))
        
        if balance < 0:
            print("El saldo inicial no puede ser negativo.")
            return
        
        cuenta = {
            "account_id": account_id,
            "account_type": account_type,
            "balance": balance,
            "currency": currency,
            "status": "activa"
        }
        
        if crear_cuenta_para_usuario(usuario_id, cuenta):
            print("✅ Cuenta creada y asociada exitosamente.")
        else:
            print("❌ Error al crear la cuenta.")
            
    except ValueError:
        print("Error: El saldo debe ser un número.")
