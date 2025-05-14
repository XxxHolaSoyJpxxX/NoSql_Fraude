from datetime import datetime
from db.MongoDB.mongo import guardar_alerta_biometrica, obtener_ubicaciones_conocidas
from db.Cassandra.cassandra import obtener_ultimas_transacciones_usuario
from db.Dgraph.dgraph import consultar_perfil_comportamiento, obtener_conexiones_sospechosas, obtener_ubicacion_transaccion

class SistemaDeteccionFraude:
    """
    Sistema de detección de fraudes bancarios que utiliza
    datos de diferentes bases de datos para realizar análisis multicapa.
    """
    
    @staticmethod
    def analizar_transaccion(usuario_id, transaccion):
        """
        Analiza una transacción utilizando múltiples métodos de detección
        para determinar si puede ser fraudulenta.
        
        Args:
            usuario_id: ID del usuario que realiza la transacción
            transaccion: Diccionario con los datos de la transacción
            
        Returns:
            dict: Resultado del análisis con indicador de fraude y motivo
        """
        # Aplicar múltiples capas de análisis
        resultado_comportamiento = SistemaDeteccionFraude.evaluar_comportamiento_usuario(usuario_id, transaccion)
        if resultado_comportamiento["es_fraude"]:
            return resultado_comportamiento
        
        resultado_geolocalizacion = SistemaDeteccionFraude.verificar_geolocalizacion(usuario_id, transaccion)
        if resultado_geolocalizacion["es_fraude"]:
            return resultado_geolocalizacion
        
        resultado_red = SistemaDeteccionFraude.analizar_red_fraude(usuario_id)
        if resultado_red["es_fraude"]:
            return resultado_red
        
        resultado_velocidad = SistemaDeteccionFraude.evaluar_velocidad_transacciones(usuario_id)
        if resultado_velocidad["es_fraude"]:
            return resultado_velocidad
        
        resultado_biometria = SistemaDeteccionFraude.validar_biometria_si_riesgo_alto(transaccion)
        if resultado_biometria["es_fraude"]:
            return resultado_biometria
        
        # Si pasa todas las verificaciones, no es fraude
        return {
            "es_fraude": False,
            "motivo": "Todas las verificaciones pasaron correctamente"
        }
    
    @staticmethod
    def evaluar_comportamiento_usuario(usuario_id, transaccion):
        """
        Analiza si la transacción es consistente con el comportamiento
        habitual del usuario utilizando el perfil almacenado en Dgraph.
        """
        # Obtener perfil de comportamiento de Dgraph
        perfil = consultar_perfil_comportamiento(usuario_id)
        
        # Verificar si la categoría y horario son consistentes con el perfil
        categoria = transaccion.get("categoria", "general")
        hora_actual = datetime.now().hour
        dia_actual = datetime.now().weekday()
        
        # Análisis de consistencia
        inconsistencias = []
        
        # Verificar categoría
        if perfil["categorias_frecuentes"] and categoria not in perfil["categorias_frecuentes"]:
            inconsistencias.append(f"Categoría '{categoria}' no habitual")
        
        # Verificar horario
        if perfil["horarios_habituales"] and hora_actual not in perfil["horarios_habituales"]:
            inconsistencias.append("Horario inusual")
        
        # Verificar día de la semana
        if perfil["dias_habituales"] and dia_actual not in perfil["dias_habituales"]:
            inconsistencias.append("Día de la semana inusual")
        
        # Verificar monto
        monto = float(transaccion.get("monto", 0))
        if perfil["monto_promedio"] > 0 and monto > perfil["monto_promedio"] * 3:
            inconsistencias.append(f"Monto 3 veces mayor al promedio ({monto} vs {perfil['monto_promedio']})")
        
        # Determinar resultado
        if len(inconsistencias) >= 2:
            return {
                "es_fraude": True,
                "nivel": "medio",
                "motivo": "Comportamiento inusual: " + ", ".join(inconsistencias)
            }
        
        return {
            "es_fraude": False,
            "nivel": "bajo",
            "motivo": "Comportamiento consistente con el perfil del usuario"
        }

    @staticmethod
    def verificar_geolocalizacion(usuario_id, transaccion):
        """
        Verifica si la ubicación de la transacción es consistente
        con las ubicaciones conocidas del usuario.
        """
        # Solo verificar si hay información de ubicación
        if "ubicacion" not in transaccion:
            return {
                "es_fraude": False,
                "motivo": "No hay información de ubicación para verificar"
            }
        
        # Obtener ubicaciones conocidas de MongoDB
        ubicaciones_conocidas = obtener_ubicaciones_conocidas(usuario_id)
        
        # Si no hay ubicaciones registradas, no podemos determinar fraude
        if not ubicaciones_conocidas:
            return {
                "es_fraude": False,
                "motivo": "No hay ubicaciones conocidas para comparar"
            }
        
        # Verificar si la ubicación actual está entre las conocidas
        ubicacion_actual = transaccion["ubicacion"]
        
        if ubicacion_actual not in ubicaciones_conocidas:
            return {
                "es_fraude": True,
                "nivel": "alto",
                "motivo": f"Ubicación desconocida: {ubicacion_actual}"
            }
        
        return {
            "es_fraude": False,
            "motivo": "Ubicación conocida para este usuario"
        }

    @staticmethod
    def analizar_red_fraude(usuario_id):
        """
        Analiza si el usuario tiene conexiones con entidades sospechosas
        en la red de fraude utilizando los datos de Dgraph.
        """
        # Obtener conexiones sospechosas de Dgraph
        conexiones = obtener_conexiones_sospechosas(usuario_id)
        
        if not conexiones:
            return {
                "es_fraude": False,
                "motivo": "No se encontraron conexiones sospechosas"
            }
        
        # Verificar probabilidad de fraude
        if conexiones["fraud_probability"] > 0.8:
            return {
                "es_fraude": True,
                "nivel": "alto",
                "motivo": f"Conexiones de alto riesgo en la red (prob: {conexiones['fraud_probability']:.2f})"
            }
        elif conexiones["fraud_probability"] > 0.5:
            return {
                "es_fraude": True,
                "nivel": "medio",
                "motivo": f"Conexiones de riesgo medio en la red (prob: {conexiones['fraud_probability']:.2f})"
            }
        
        return {
            "es_fraude": False,
            "motivo": "Conexiones dentro de niveles aceptables de riesgo"
        }

    @staticmethod
    def evaluar_velocidad_transacciones(usuario_id):
        """
        Analiza la velocidad de las transacciones recientes para
        detectar patrones sospechosos utilizando datos de Cassandra.
        """
        # Obtener últimas transacciones de Cassandra
        transacciones = obtener_ultimas_transacciones_usuario(usuario_id, 3)
        
        # Si hay menos de 3 transacciones, no podemos evaluar la velocidad
        if len(transacciones) < 3:
            return {
                "es_fraude": False,
                "motivo": "No hay suficientes transacciones recientes para evaluar"
            }
        
        # Calcular tiempos entre transacciones
        tiempos = [datetime.strptime(t["fecha"], "%Y-%m-%d %H:%M:%S") for t in transacciones]
        
        # Ordenar cronológicamente (más reciente primero)
        tiempos.sort(reverse=True)
        
        # Calcular diferencias de tiempo en segundos
        dif1 = (tiempos[0] - tiempos[1]).total_seconds()
        dif2 = (tiempos[1] - tiempos[2]).total_seconds()
        
        # Verificar si las transacciones son demasiado rápidas
        if dif1 < 30 and dif2 < 30:
            return {
                "es_fraude": True,
                "nivel": "alto",
                "motivo": f"Transacciones excesivamente rápidas ({dif1:.1f}s y {dif2:.1f}s entre ellas)"
            }
        elif dif1 < 60 and dif2 < 60:
            return {
                "es_fraude": True,
                "nivel": "medio",
                "motivo": f"Transacciones sospechosamente rápidas ({dif1:.1f}s y {dif2:.1f}s entre ellas)"
            }
        
        return {
            "es_fraude": False,
            "motivo": "Velocidad de transacciones normal"
        }

    @staticmethod
    def validar_biometria_si_riesgo_alto(transaccion):
        """
        Determina si una transacción requiere validación biométrica
        por su nivel de riesgo y guarda una alerta en MongoDB.
        """
        usuario_id = transaccion.get("usuario_id")
        monto = float(transaccion.get("monto", 0))
        categoria = transaccion.get("categoria", "")
        
        # Criterios para requerir validación biométrica
        requiere_validacion = False
        motivo = []
        
        if monto > 10000:
            requiere_validacion = True
            motivo.append(f"Monto elevado ({monto})")
        
        if categoria in ["internacional", "criptomonedas", "casino"]:
            requiere_validacion = True
            motivo.append(f"Categoría de alto riesgo ({categoria})")
        
        if requiere_validacion:
            # Guardar alerta en MongoDB
            guardar_alerta_biometrica(usuario_id, transaccion)
            
            return {
                "es_fraude": True,
                "nivel": "validacion",
                "motivo": "Se requiere validación biométrica: " + ", ".join(motivo)
            }
        
        return {
            "es_fraude": False,
            "motivo": "No requiere validación biométrica"
        }


# Para compatibilidad con el código existente, mantenemos las funciones individuales
# que ahora simplemente llaman a los métodos de la clase

def evaluar_comportamiento_usuario(usuario_id, transaccion):
    return SistemaDeteccionFraude.evaluar_comportamiento_usuario(usuario_id, transaccion)

def verificar_geolocalizacion(usuario_id, ubicacion_actual):
    transaccion = {"ubicacion": ubicacion_actual}
    return SistemaDeteccionFraude.verificar_geolocalizacion(usuario_id, transaccion)

def analizar_red_fraude(usuario_id):
    return SistemaDeteccionFraude.analizar_red_fraude(usuario_id)

def evaluar_velocidad_transacciones(usuario_id):
    return SistemaDeteccionFraude.evaluar_velocidad_transacciones(usuario_id)

def validar_biometria_si_riesgo_alto(transaccion):
    return SistemaDeteccionFraude.validar_biometria_si_riesgo_alto(transaccion)

def es_consistente_con_perfil(perfil, transaccion):
    """
    Verifica si una transacción es consistente con el perfil de comportamiento
    del usuario. Función de ayuda para evaluar_comportamiento_usuario.
    """
    # Obtener datos de la transacción
    categoria = transaccion.get("categoria", "general")
    hora = datetime.strptime(transaccion.get("fecha", datetime.now().strftime("%Y-%m-%d %H:%M:%S")), "%Y-%m-%d %H:%M:%S").hour
    
    # Comprobar si la categoría y el horario son típicos para este usuario
    categorias_ok = categoria in perfil.get("categorias_frecuentes", [])
    horario_ok = hora in perfil.get("horarios_habituales", range(24))
    
    return categorias_ok and horario_ok

def calcular_distancia_promedio(ubicaciones, actual):
    """
    Calcula la distancia promedio entre las ubicaciones conocidas
    y la ubicación actual. 
    
    Esta es una función de ayuda simplificada para verificar_geolocalizacion.
    En un sistema real, usaría cálculos geoespaciales reales.
    """
    # Implementación simplificada
    if actual in ubicaciones:
        return 0  # Ubicación exacta conocida
    
    # Retornamos un valor de ejemplo para que se active la detección
    # En un sistema real, esto calcularía distancias reales
    return 120  # km
