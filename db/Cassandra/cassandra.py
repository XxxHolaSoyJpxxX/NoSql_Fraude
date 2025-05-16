from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from datetime import datetime, timedelta
import logging
import time
from time_uuid import TimeUUID
import uuid
from db.Dgraph.dgraph import obtener_cuenta_por_email
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[logging.FileHandler("cassandra.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ─────────────────────────────
# CONEXIÓN A CASSANDRA
# ─────────────────────────────

logging.getLogger("cassandra").setLevel(logging.ERROR)

# Tu propio logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[logging.FileHandler("cassandra.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def get_cassandra_session():
    try:
        logger.info("Connecting to Cluster")
        cluster = Cluster(['localhost'], port=9042)
        session = cluster.connect()

        keyspace_name = "transacciones"
        replication_factor = 1

        rows = session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
        if keyspace_name.lower() not in [row.keyspace_name for row in rows]:
            logger.info(f"Creating keyspace: {keyspace_name} with replication factor {replication_factor}")
            session.execute(f"""
                CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
                WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': {replication_factor}}}
            """)

        session.set_keyspace(keyspace_name)
        return session
    except Exception as e:
        logger.error(f"Error connecting to Cassandra: {e}")
        raise

# ─────────────────────────────
# CREACIÓN DE TABLAS
# ─────────────────────────────

def crear_tabla_transacciones_timestap():
    session = get_cassandra_session()
    query = """
    CREATE TABLE IF NOT EXISTS transaccionestimesstap (
    account_id text,
    timestamp timestamp,
    id_transaccion uuid,
    to_account text,
    amount double,
    status text,
    lat double,
    lon double,
    PRIMARY KEY (account_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);
    """
    session.execute(query)

def crear_tabla_transacciones_amount():
    session = get_cassandra_session()
    query = """
    CREATE TABLE IF NOT EXISTS transacciones_por_amount (
    account_id text,
    timestamp timestamp,
    id_transaccion uuid,
    to_account text,
    amount double,
    status text,
    lat double,
    lon double,
    PRIMARY KEY (account_id, amount)
) WITH CLUSTERING ORDER BY (amount DESC);
    """
    session.execute(query)

def crear_tabla_transacciones_status():
    session = get_cassandra_session()
    query = """
    CREATE TABLE IF NOT EXISTS transacciones_por_status (
    account_id text,
    timestamp timestamp,
    id_transaccion uuid,
    to_account text,
    amount double,
    status text,
    lat double,
    lon double,
    PRIMARY KEY (account_id, status)
) WITH CLUSTERING ORDER BY (status DESC);
    """
    session.execute(query)

#Insersion de datos:
# Función para insertar transacción en tabla con clustering por timestamp
def insertar_transaccion_timestap(account_id, to_account, amount, id_transaction, lat, lon):
    session = get_cassandra_session()
    query = """
    INSERT INTO transaccionestimesstap (
        account_id, timestamp, id_transaccion, to_account, amount, status, lat, lon
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    session.execute(query, (
        account_id,
        datetime.now(),
        id_transaction,
        to_account,
        amount,
        'pendiente',
        lat,
        lon
    ))
    return "✅ Transacción insertada en transaccionestimesstap"



def insertar_transaccion_amount(account_id, to_account, amount, id_transaction, lat, lon):
    session = get_cassandra_session()
    query = """
    INSERT INTO transacciones_por_amount (
        account_id, timestamp, id_transaccion, to_account, amount, status, lat, lon
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    session.execute(query, (
        account_id,
        datetime.now(),
        id_transaction,
        to_account,
        amount,
        'pendiente',
        lat,
        lon
    ))
    return "✅ Transacción insertada en transacciones_por_amount"

def insertar_transaccion_status(account_id, to_account, amount, id_transaction, lat, lon):
    session = get_cassandra_session()
    query = """
    INSERT INTO transacciones_por_status (
        account_id, timestamp, id_transaccion, to_account, amount, status, lat, lon
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    session.execute(query, (
        account_id,
        datetime.now(),
        id_transaction,
        to_account,
        amount,
        'pendiente',
        lat,
        lon
    ))
    return "✅ Transacción insertada en transacciones_por_status"



def ver_transacciones_por_amount(email):
    # Obtener account_id desde Dgraph
    cuenta = obtener_cuenta_por_email(email)
    if not cuenta:
        print("⚠️ Cuenta no encontrada para este correo.")
        return

    account_id = cuenta
    session = get_cassandra_session()

    query = """
    SELECT timestamp, amount, to_account, status,id_transaccion, lat, lon
    FROM transacciones_por_amount
    WHERE account_id = %s
    """

    try:
        rows = session.execute(query, (account_id,))
        print(f"\nHistorial de transacciones ordenado por monto (desc) para {email}:\n")
        for row in rows:
            print("────────────────────────────────────────────────")
            print(f"Fecha/Hora     : {row.timestamp}")
            print(f"Monto          : ${row.amount:.2f}")
            print(f"ID Transacción : {row.id_transaccion}")
            print(f"Estado         : {row.status}")
            print(f"Cuenta destino : {row.to_account}")
            print(f"Ubicación      : lat={row.lat}, lon={row.lon}")
        print("────────────────────────────────────────────────")
    except Exception as e:
        print(f"Error al consultar Cassandra: {e}")

def ver_transacciones_por_timestamp(email):
    # Obtener account_id desde Dgraph
    cuenta = obtener_cuenta_por_email(email)
    if not cuenta:
        print("⚠️ Cuenta no encontrada para este correo.")
        return

    account_id = cuenta
    session = get_cassandra_session()

    query = """
    SELECT timestamp, amount, to_account, status,id_transaccion, lat, lon
    FROM transaccionestimesstap
    WHERE account_id = %s
    """

    try:
        rows = session.execute(query, (account_id,))
        print(f"\nHistorial de transacciones ordenado por fecha (desc) para {email}:\n")
        for row in rows:
            print("────────────────────────────────────────────────")
            print(f"Fecha/Hora     : {row.timestamp}")
            print(f"Monto          : ${row.amount:.2f}")
            print(f"ID Transacción : {row.id_transaccion}")
            print(f"Estado         : {row.status}")
            print(f"Cuenta destino : {row.to_account}")
            print(f"Ubicación      : lat={row.lat}, lon={row.lon}")
        print("────────────────────────────────────────────────")
    except Exception as e:
        print(f"Error al consultar Cassandra: {e}")


def obtener_todas_las_transacciones():
    session = get_cassandra_session()
    query = "SELECT * FROM transaccionestimesstap ALLOW FILTERING"
    rows = session.execute(query)

    transacciones = []
    for row in rows:
        transacciones.append({
            "account_id": row.account_id,
            "timestamp": row.timestamp,
            "id_transaccion": row.id_transaccion,
            "to_account": row.to_account,
            "amount": row.amount,
            "status": row.status,
            "lat": row.lat,
            "lon": row.lon
        })

    transacciones.sort(key=lambda t: t["timestamp"], reverse=True)
    return transacciones

def mostrar_todas_transacciones(admin_id):
    transacciones = obtener_todas_las_transacciones()

    if not transacciones:
        print("No hay transacciones registradas.")
        return

    print("\n=== Historial de Transacciones ===\n")
    print("{:<36} {:<10} {:<15} {:<10} {:<10} {:<10} {:<25}".format(
        "ID", "Cuenta", "Destinatario", "Monto", "Estado", "Hora", "Ubicación"
    ))
    print("-" * 140)

    for t in transacciones:
        print("{:<36} {:<10} {:<15} {:<10.2f} {:<10} {:<10} {:<25}".format(
            str(t["id_transaccion"]),
            t["account_id"],
            t["to_account"],
            t["amount"],
            t["status"],
            t["timestamp"].strftime("%H:%M:%S"),
            f"({t['lat']}, {t['lon']})"
        ))

    print("\nTotal de transacciones:", len(transacciones))

    registrar_accion_admin(
        admin_id,
        "Visualización de historial de transacciones",
        f"{len(transacciones)} transacciones listadas"
    )



def crear_tabla_acciones_admin_por_admin():
    session = get_cassandra_session()
    query = """
    CREATE TABLE IF NOT EXISTS acciones_admin_por_admin (
        admin_id text,
        timestamp timestamp,
        accion_id uuid,
        accion text,
        detalle text,
        PRIMARY KEY ((admin_id), timestamp)
    ) WITH CLUSTERING ORDER BY (timestamp DESC);
    """
    session.execute(query)

def crear_tabla_acciones_admin_global():
    session = get_cassandra_session()
    query = """
    CREATE TABLE IF NOT EXISTS acciones_admin_global (
        accion_global text,
        timestamp timestamp,
        accion_id uuid,
        admin_id text,
        accion text,
        detalle text,
        PRIMARY KEY ((accion_global), timestamp, admin_id)
    ) WITH CLUSTERING ORDER BY (timestamp DESC);
    """
    session.execute(query)



def registrar_accion_admin(admin_id, accion, detalle=""):
    session = get_cassandra_session()
    now = datetime.now()
    accion_id = uuid.uuid4()

    # Insertar en tabla por admin
    query_admin = """
    INSERT INTO acciones_admin_por_admin (
        admin_id, timestamp, accion_id, accion, detalle
    ) VALUES (%s, %s, %s, %s, %s)
    """
    session.execute(query_admin, (
        admin_id, now, accion_id, accion, detalle
    ))

    # Insertar en tabla global
    query_global = """
    INSERT INTO acciones_admin_global (
        accion_global, timestamp, accion_id, admin_id, accion, detalle
    ) VALUES (%s, %s, %s, %s, %s, %s)
    """
    session.execute(query_global, (
        "global", now, accion_id, admin_id, accion, detalle
    ))


def ver_todas_las_acciones_admin():
    session = get_cassandra_session()
    query = "SELECT * FROM acciones_admin_global WHERE accion_global = 'global'"
    rows = session.execute(query)

    acciones = sorted(rows, key=lambda row: row.timestamp, reverse=True)

    print("\n=== Historial de Acciones (Global) ===")
    print("{:<36} {:<20} {:<25} {:<50}".format("Accion ID", "Admin ID", "Fecha", "Acción / Detalle"))
    print("-" * 140)

    for a in acciones:
        print("{:<36} {:<20} {:<25} {:<50}".format(
            str(a.accion_id),
            a.admin_id,
            a.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            f"{a.accion} - {a.detalle}"
        ))


def ver_acciones_de_admin(admin_id):
    session = get_cassandra_session()
    query = "SELECT * FROM acciones_admin_por_admin WHERE admin_id = %s"
    rows = session.execute(query, (admin_id,))

    acciones = sorted(rows, key=lambda row: row.timestamp, reverse=True)

    print(f"\n=== Historial del Admin: {admin_id} ===")
    print("{:<36} {:<25} {:<50}".format("Accion ID", "Fecha", "Acción / Detalle"))
    print("-" * 120)

    for a in acciones:
        print("{:<36} {:<25} {:<50}".format(
            str(a.accion_id),
            a.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            f"{a.accion} - {a.detalle}"
        ))


def ver_todas_las_acciones_admin(admin_id):
    session = get_cassandra_session()
    query = "SELECT * FROM acciones_admin_global WHERE accion_global = 'global'"
    rows = session.execute(query)

    acciones = sorted(rows, key=lambda row: row.timestamp, reverse=True)

    print("\n=== Historial de Acciones (Global) ===")
    print("{:<36} {:<20} {:<25} {:<50}".format("Accion ID", "Admin ID", "Fecha", "Acción / Detalle"))
    print("-" * 140)

    for a in acciones:
        print("{:<36} {:<20} {:<25} {:<50}".format(
            str(a.accion_id),
            a.admin_id,
            a.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            f"{a.accion} - {a.detalle}"
        ))

        registrar_accion_admin(admin_id, 'Historial de administradores', 'Vio el historial de administradores')

def ver_acciones_de_admin(admin_id):
    session = get_cassandra_session()
    
    admin_id_input = input("\nIngresa el ID del administrador que deseas consultar: ").strip()

    query = "SELECT * FROM acciones_admin_por_admin WHERE admin_id = %s"
    rows = session.execute(query, (admin_id_input,))

    acciones = sorted(rows, key=lambda row: row.timestamp, reverse=True)

    if not acciones:
        print(f"No se encontraron acciones para el admin '{admin_id_input}'.")
        return

    print(f"\n=== Historial del Admin: {admin_id_input} ===")
    print("{:<36} {:<25} {:<50}".format("Accion ID", "Fecha", "Acción / Detalle"))
    print("-" * 120)

    for a in acciones:
        print("{:<36} {:<25} {:<50}".format(
            str(a.accion_id),
            a.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            f"{a.accion} - {a.detalle}"
        ))

    print(f"\nTotal de acciones encontradas: {len(acciones)}")
    registrar_accion_admin(admin_id, f'Auditoria de {admin_id_input}', 'Vio el historial de acciones')

