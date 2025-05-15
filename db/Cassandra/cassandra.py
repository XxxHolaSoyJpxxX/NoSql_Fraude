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
    source text,
    destination text,
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
    source text,
    destination text,
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
    source text,
    destination text,
    PRIMARY KEY (account_id, status)
) WITH CLUSTERING ORDER BY (status DESC);
    """
    session.execute(query)

#Insersion de datos:
# Función para insertar transacción en tabla con clustering por timestamp
def insertar_transaccion_timestap(account_id, to_account, amount, source, destination, id_transaction):
    session = get_cassandra_session()
    query = """
    INSERT INTO transaccionestimesstap (
        account_id, timestamp, id_transaccion, to_account, amount, status, source, destination
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    session.execute(query, (
        account_id,
        datetime.now(),
        id_transaction,
        to_account,
        amount,
        'pendiente',
        source,
        destination
    ))
    return "✅ Transacción insertada en transaccionestimesstap"


def insertar_transaccion_amount(account_id, to_account, amount, source, destination, id_transaction):
    session = get_cassandra_session()
    query = """
    INSERT INTO transacciones_por_amount (
        account_id, timestamp, id_transaccion, to_account, amount, status, source, destination
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    session.execute(query, (
        account_id,
        datetime.now(),
        id_transaction,
        to_account,
        amount,
        'pendiente',
        source,
        destination
    ))
    return "✅ Transacción insertada en transacciones_por_amount"

def insertar_transaccion_status(account_id, to_account, amount, source, destination, id_transaction):
    print(account_id, to_account, amount, source, destination)
    session = get_cassandra_session()
    query = """
    INSERT INTO transacciones_por_status (
        account_id, timestamp, id_transaccion, to_account, amount, status, source, destination
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    session.execute(query, (
        account_id,
        datetime.now(),
        id_transaction,
        to_account,
        amount,
        'pendiente',
        source,
        destination
    ))
    return "✅ Transacción insertada en transaccionestimesstap_por_status"


def ver_transacciones_por_amount(email):
    # Obtener account_id desde Dgraph
    cuenta = obtener_cuenta_por_email(email)
    if not cuenta:
        print("⚠️ Cuenta no encontrada para este correo.")
        return

    account_id = cuenta
    session = get_cassandra_session()

    # Ejecutar consulta
    query = """
    SELECT timestamp, amount, to_account, status, source, destination, id_transaccion
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
            print(f"De             : {row.source}")
            print(f"Para           : {row.destination}")
            print(f"ID Transacción : {row.id_transaccion}")
            print(f"Estado         : {row.status}")
            print(f"Cuenta destino : {row.to_account}")
        print("────────────────────────────────────────────────")
    except Exception as e:
        print(f"Error al consultar Cassandra: {e}")

