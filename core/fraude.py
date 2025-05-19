from datetime import datetime
from uuid import uuid4
import uuid
from db.Dgraph.dgraph import obtener_todos_los_reportes,get_dgraph_client, obtener_cuenta_por_email, obtener_uid_usuario_por_email
from db.MongoDB.mongo import get_mongo_db,obter_ubicacion_cuenta
from db.Cassandra.cassandra import ver_transacciones_por_amount,obterner_trsaccion_por_id,obtener_transacciones_por_cuenta
import json
from datetime import timedelta


#///////////////////
# Reporte de Usuario
#///////////////////
def reportar_fraude_auto(email_usuario, tipo, id_transaccion,motivo="Reporte automático"):
    resultado = reportar_transaccion_fraudulenta(id_transaccion, email_usuario,motivo,tipo)
    print(resultado)

def reportar_transaccion(email_usuario):

    print("\n=== Reportar Transacción Fraudulenta ===")

    # Mostrar las transacciones del usuario
    ver_transacciones_por_amount(email_usuario)

    # Pedir datos del reporte
    id_transaccion = input("\nIngresa el ID de la transacción que deseas reportar: ").strip()
    motivo = input("Describe el motivo del reporte: ").strip()
    tipo = "reporte de usuario"
    # Registrar reporte
    resultado = reportar_transaccion_fraudulenta(id_transaccion, email_usuario,motivo,tipo)
    print(resultado)

def reportar_transaccion_fraudulenta(transaction_id, email_usuario, motivo,tipo):
    client = get_dgraph_client()
    txn = None
    # Obtener UID del usuario (NO el account_id)
    usuario_uid = obtener_uid_usuario_por_email(email_usuario)
    if not usuario_uid:
        return "No se encontró el usuario en Dgraph."

    # Obtener UID de la transacción desde el transaction_id
    query = """
    query obtenerUID($id: string) {
        transacciones(func: eq(transaction_id, $id)) {
            uid
        }
    }
    """
    try:
        res = client.txn(read_only=True).query(query, variables={"$id": str(transaction_id)})
        data = json.loads(res.json)
        transacciones = data.get("transacciones", [])

        if not transacciones:
            return "No se encontró la transacción en Dgraph."

        transaccion_uid = transacciones[0]["uid"]

        # Crear el nodo de ReporteFraude y enlazarlo con la transacción
        txn = client.txn()
        reporte = {
            "dgraph.type": "ReporteFraude",
            "reporte_id": str(uuid4()),
            "motivo": motivo,
            "tipo": tipo,
            "fecha": datetime.now().isoformat(),
            "reportado_por": {"uid": usuario_uid},
            "transaccion_reportada": {"uid": transaccion_uid}
        }
        txn.mutate(set_obj=reporte)
        txn.commit()

        return "✅ Reporte de fraude registrado correctamente."
    except Exception as e:
        return f" Error al registrar el reporte: {e}"
    finally:
        if txn:
            txn.discard()
        
#///////////////////
# Verificación de Geolocalización
#///////////////////

def verificar_geolocalizacion(email_usuario, trasaccion_id):
    ubicacion_cuenta = obter_ubicacion_cuenta(email_usuario)
    if ubicacion_cuenta is None:
        print("No se encontró la ubicación de la cuenta en MongoDB.")
        return ubicacion_cuenta
    lat_cuenta = ubicacion_cuenta["lat"]
    lon_cuenta = ubicacion_cuenta["lon"]
    trsaccion = obterner_trsaccion_por_id(trasaccion_id)
    if not trsaccion:
        print("No se encontró la transacción en Cassandra.")
        return 
    trsaccion_lat = trsaccion["lat"]
    trsaccion_lon = trsaccion["lon"]
    # Comparar la ubicación de la cuenta con la ubicación de la transacción
    if lat_cuenta == trsaccion_lat and lon_cuenta == trsaccion_lon:
        return False
    else:
        return True
    
    
    # Obtener la ubicación de la transacción
    
#///////////////////
# Análisis de Comportamiento del Usuario
#///////////////////

#///////////////////
# Análisis de Gastos del Usuario
#///////////////////
def detectar_gasto_inusual(trasaccion_id):
    trsaccion = obterner_trsaccion_por_id(trasaccion_id)
    if not trsaccion:
        print("No se encontró la transacción.")
        return False
    account_id = trsaccion.get("account_id")
    if not account_id:
        print("La transacción no tiene un 'account_id'.")
        return False
    # Obtener todas las transacciones asociadas a la cuenta
    acount_trasacciones = obtener_transacciones_por_cuenta(account_id)
    # Verificar si hay transacciones
    if not acount_trasacciones or len(acount_trasacciones) <= 1:
        print("No hay suficientes transacciones para calcular el promedio.")
        return False
    montos = [
    row["amount"]
    for row in acount_trasacciones
    if row["id_transaccion"] != trsaccion["id_transaccion"] and row["amount"] is not None
    ]
    if not montos:
        print("No hay montos válidos para comparar.")
        return False

    promedio = sum(montos) / len(montos)
    limite = promedio * 1.5
    if trsaccion["amount"] > limite:
        print(f"Transacción inusual: monto {trsaccion['amount']} supera el 50% del promedio ({promedio:.2f}).")
        return True
    return False
#///////////////////
# Duplicación de Transacciones
#///////////////////

def detectar_duplicacion_transacciones(trasaccion_id):
    trsaccion = obterner_trsaccion_por_id(trasaccion_id)
    if not trsaccion:
        print("No se encontró la transacción.")
        return False
    account_id = trsaccion.get("account_id")
    if not account_id:
        print("La transacción no tiene un 'account_id'.")
        return False
    # Obtener todas las transacciones asociadas a la cuenta
    acount_trasacciones = obtener_transacciones_por_cuenta(account_id)
    if not acount_trasacciones:
        print("No se encontraron transacciones para la cuenta.")
        return False
    # Comparar transacciones
    for row in acount_trasacciones:
        if row["id_transaccion"] != trsaccion["id_transaccion"]:
            if row["timestamp"] == trsaccion["timestamp"]:
                print("Se detectó una transacción duplicada.")
                return True
    return False

#///////////////////
# Tiempo entre transacciones 
#///////////////////

def tiempo_entre_transacciones(email_usuario):
    account_id = obtener_cuenta_por_email(email_usuario)

    acount_trasacciones = obtener_transacciones_por_cuenta(account_id)
    if not acount_trasacciones:
        print("No se encontraron transacciones para la cuenta.")
        return False

    transacciones_ordenadas = sorted(acount_trasacciones, key=lambda x: x["timestamp"])

    inicio = 0
    for fin in range(len(transacciones_ordenadas)):
        while (transacciones_ordenadas[fin]["timestamp"] - transacciones_ordenadas[inicio]["timestamp"]) > timedelta(minutes=5):
            inicio += 1
        if (fin - inicio + 1) > 3:
            print("Se detectaron más de 3 transacciones en menos de 5 minutos.")
            return True

    return False



def estadsitcas_fraude():
    # Obtener todos los reportes de fraude
    reportes = obtener_todos_los_reportes()
    if not reportes:
        print("No se encontraron reportes de fraude.")
        return

    # Contar el número de reportes por tipo
    conteo_reportes = {}
    for reporte in reportes:
        tipo = reporte.get("tipo")
        if tipo:
            conteo_reportes[tipo] = conteo_reportes.get(tipo, 0) + 1

    # Mostrar estadísticas
    print("\n=== Estadísticas de Fraude ===")
    for tipo, conteo in conteo_reportes.items():
        print(f"{tipo}: {conteo} reportes")
        
def perfil_riesgo(cantidad):
    reportes = obtener_todos_los_reportes()
    if not reportes:
        print("No se encontraron reportes de fraude.")
        return

    conteo_reportes_por_email = {}

    for reporte in reportes:
        email = reporte.get("reportado_por", {}).get("email")
        if email:
            conteo_reportes_por_email[email] = conteo_reportes_por_email.get(email, 0) + 1

    conteo_ordenado = dict(sorted(conteo_reportes_por_email.items(), key=lambda item: item[1], reverse=True))
    if cantidad == 0:
        top = conteo_ordenado
    else:
        top = dict(list(conteo_ordenado.items())[:cantidad])

    print("\n=== Perfiles de Riesgo ===")
    print(top)

def obtener_reportes_geograficos():
    reportes = obtener_todos_los_reportes()
    if not reportes:
        print("No se encontraron reportes de fraude.")
        return

    reportes_geograficos = [reporte for reporte in reportes if reporte.get("tipo") == "geolocalizacion"]
    if not reportes_geograficos:
        print("No se encontraron reportes geográficos.")
        return

    print("\n=== Reportes Geográficos ===")
    for reporte in reportes_geograficos:
        print(reporte)