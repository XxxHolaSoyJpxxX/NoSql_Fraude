from datetime import datetime
from db.mongo import guardar_alerta_biometrica, obtener_ubicaciones_conocidas
from db.cassandra import obtener_ultimas_transacciones_usuario
from db.dgraph import consultar_perfil_comportamiento, obtener_conexiones_sospechosas, obtener_ubicacion_transaccion

# 1. Análisis de Comportamiento del Usuario (Dgraph)
def evaluar_comportamiento_usuario(usuario_id, transaccion):
    perfil = consultar_perfil_comportamiento(usuario_id)
    if not es_consistente_con_perfil(perfil, transaccion):
        return {
            "es_fraude": True,
            "motivo": "Comportamiento inusual comparado con el historial"
        }
    return {"es_fraude": False}

def es_consistente_con_perfil(perfil, transaccion):
    # Simulación simple: comparar categoría, horario, monto
    categoria = transaccion.get("categoria")
    hora = datetime.strptime(transaccion["fecha"], "%Y-%m-%d %H:%M:%S").hour
    return categoria in perfil["categorias_frecuentes"] and hora in perfil["horarios_habituales"]


# 2. Verificación de Geolocalización (Dgraph)
def verificar_geolocalizacion(usuario_id, ubicacion_actual):
    ubicaciones = obtener_ubicaciones_conocidas(usuario_id)
    distancia = calcular_distancia_promedio(ubicaciones, ubicacion_actual)
    if distancia > 100:  # km, por ejemplo
        return {
            "es_fraude": True,
            "motivo": f"Ubicación fuera del rango habitual: {distancia:.1f}km"
        }
    return {"es_fraude": False}

def calcular_distancia_promedio(ubicaciones, actual):
    # Dummy: retornar valor fijo
    return 120  # km


# 3. Detección de Redes de Fraude (Dgraph)
def analizar_red_fraude(usuario_id):
    conexiones = obtener_conexiones_sospechosas(usuario_id)
    if conexiones and conexiones["fraud_probability"] > 0.8:
        return {
            "es_fraude": True,
            "motivo": "Conexiones sospechosas en la red de usuarios"
        }
    return {"es_fraude": False}


# 4. Análisis de Velocidad de Transacciones (Cassandra)
def evaluar_velocidad_transacciones(usuario_id):
    transacciones = obtener_ultimas_transacciones_usuario(usuario_id)
    if len(transacciones) >= 3:
        tiempos = [datetime.strptime(t["fecha"], "%Y-%m-%d %H:%M:%S") for t in transacciones]
        dif1 = (tiempos[0] - tiempos[1]).seconds
        dif2 = (tiempos[1] - tiempos[2]).seconds
        if dif1 < 30 and dif2 < 30:
            return {
                "es_fraude": True,
                "motivo": "Transacciones excesivamente rápidas (menos de 30s entre ellas)"
            }
    return {"es_fraude": False}


# 5. Validación Biométrica en Transacciones de Alto Riesgo (MongoDB)
def validar_biometria_si_riesgo_alto(transaccion):
    if transaccion["monto"] > 10000 or transaccion["categoria"] == "transferencia internacional":
        guardar_alerta_biometrica(transaccion["usuario_id"], transaccion)
        return {
            "es_fraude": True,
            "motivo": "Se requiere validación biométrica por riesgo alto"
        }
    return {"es_fraude": False}
