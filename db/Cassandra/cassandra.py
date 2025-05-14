from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from datetime import datetime, timedelta
import logging
import time
from time_uuid import TimeUUID
import uuid

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

def get_cassandra_session():
    """
    Connect to Cassandra cluster and return a session
    """
    try:
        logger.info("Connecting to Cluster")
        cluster = Cluster(['localhost'], port=9042)
        session = cluster.connect()
        
        # Create keyspace if it doesn't exist
        keyspace_name = "proyectofraude"
        replication_factor = 1
        
        # Check if keyspace exists
        rows = session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
        if keyspace_name.lower() not in [row.keyspace_name for row in rows]:
            logger.info(f"Creating keyspace: {keyspace_name} with replication factor {replication_factor}")
            session.execute(f"""
                CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
                WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': {replication_factor}}}
            """)
        
        # Connect to keyspace
        session.set_keyspace(keyspace_name)
        return session
    except Exception as e:
        logger.error(f"Error connecting to Cassandra: {e}")
        raise

# ─────────────────────────────
# CREACIÓN DE TABLAS
# ─────────────────────────────

def crear_tablas():
    """
    Create the necessary tables for transaction and fraud detection
    """
    session = get_cassandra_session()
    
    # Tabla de transacciones - Optimizada para consultas temporales
    session.execute("""
        CREATE TABLE IF NOT EXISTS transacciones (
            account_id text,
            transaction_id uuid,
            fecha timestamp,
            monto decimal,
            tipo text,
            origen text,
            destino text, 
            estatus text,
            dispositivo text,
            ip_address text,
            ubicacion text,
            categoria text,
            usuario_id text,
            PRIMARY KEY (account_id, fecha, transaction_id)
        ) WITH CLUSTERING ORDER BY (fecha DESC, transaction_id ASC)
    """)
    
    # Tabla de transacciones por usuario - Para facilitar búsquedas por usuario
    session.execute("""
        CREATE TABLE IF NOT EXISTS transacciones_por_usuario (
            usuario_id text,
            transaction_id uuid,
            account_id text,
            fecha timestamp,
            monto decimal,
            tipo text,
            origen text,
            destino text, 
            estatus text,
            categoria text,
            PRIMARY KEY (usuario_id, fecha, transaction_id)
        ) WITH CLUSTERING ORDER BY (fecha DESC, transaction_id ASC)
    """)
    
    # Tabla de velocidad de transacciones - Para detección de fraude por velocidad
    session.execute("""
        CREATE TABLE IF NOT EXISTS velocidad_transacciones (
            usuario_id text,
            intervalo_tiempo text,  
            fecha timestamp,
            conteo counter,
            PRIMARY KEY (usuario_id, intervalo_tiempo, fecha)
        )
    """)
    
    # Tabla de patrones de transacción - Para análisis de comportamiento
    session.execute("""
        CREATE TABLE IF NOT EXISTS patrones_transaccion (
            usuario_id text,
            tipo_transaccion text,
            franja_horaria int,
            dia_semana int,
            monto_promedio decimal,
            frecuencia counter,
            PRIMARY KEY (usuario_id, tipo_transaccion, franja_horaria, dia_semana)
        )
    """)
    
    # Tabla de alertas de fraude - Registro histórico de fraudes detectados
    session.execute("""
        CREATE TABLE IF NOT EXISTS alertas_fraude (
            alert_id uuid,
            usuario_id text,
            fecha timestamp,
            transaction_id uuid,
            tipo_alerta text,
            nivel_riesgo text,
            descripcion text,
            estatus text,
            PRIMARY KEY (usuario_id, fecha, alert_id)
        ) WITH CLUSTERING ORDER BY (fecha DESC, alert_id ASC)
    """)
    
    # Tabla de auditoría de administradores - Para seguimiento de acciones de admin
    session.execute("""
        CREATE TABLE IF NOT EXISTS auditoria_admins (
            admin_id text,
            fecha timestamp,
            accion_id uuid,
            tipo_accion text,
            detalles text,
            ip_address text,
            PRIMARY KEY (admin_id, fecha, accion_id)
        ) WITH CLUSTERING ORDER BY (fecha DESC, accion_id ASC)
    """)
    
    logger.info("Tablas creadas exitosamente en Cassandra")

# ─────────────────────────────
# FUNCIONES DE TRANSACCIONES
# ─────────────────────────────

def registrar_transaccion(transaction_data):
    """
    Register a new transaction in the database
    """
    session = get_cassandra_session()
    
    # Generate a TimeUUID for the transaction
    transaction_id = TimeUUID.now()
    now = datetime.now()
    
    # Insert in transactions table
    query1 = """
        INSERT INTO transacciones (
            account_id, transaction_id, fecha, monto, tipo, origen, 
            destino, estatus, dispositivo, ip_address, ubicacion, 
            categoria, usuario_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    session.execute(query1, (
        transaction_data['account_id'],
        transaction_id,
        now,
        transaction_data['monto'],
        transaction_data['tipo'],
        transaction_data.get('origen', ''),
        transaction_data.get('destino', ''),
        transaction_data.get('estatus', 'completada'),
        transaction_data.get('dispositivo', 'unknown'),
        transaction_data.get('ip_address', ''),
        transaction_data.get('ubicacion', ''),
        transaction_data.get('categoria', 'general'),
        transaction_data['usuario_id']
    ))
    
    # Insert in transactions by user table
    query2 = """
        INSERT INTO transacciones_por_usuario (
            usuario_id, transaction_id, account_id, fecha, monto, 
            tipo, origen, destino, estatus, categoria
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    session.execute(query2, (
        transaction_data['usuario_id'],
        transaction_id,
        transaction_data['account_id'],
        now,
        transaction_data['monto'],
        transaction_data['tipo'],
        transaction_data.get('origen', ''),
        transaction_data.get('destino', ''),
        transaction_data.get('estatus', 'completada'),
        transaction_data.get('categoria', 'general')
    ))
    
    # Update transaction velocity counters
    # Update minute, hour, and day counters
    intervals = [
        f"{now.strftime('%Y%m%d%H%M')}",  # Minute
        f"{now.strftime('%Y%m%d%H')}",    # Hour
        f"{now.strftime('%Y%m%d')}"       # Day
    ]
    
    for intervalo in intervals:
        session.execute("""
            UPDATE velocidad_transacciones 
            SET conteo = conteo + 1 
            WHERE usuario_id = %s AND intervalo_tiempo = %s AND fecha = %s
        """, (transaction_data['usuario_id'], intervalo, now))
    
    # Update transaction patterns
    hora = now.hour
    dia_semana = now.weekday()  # 0-6 (Mon-Sun)
    
    session.execute("""
        UPDATE patrones_transaccion
        SET monto_promedio = ((monto_promedio * frecuencia) + %s) / (frecuencia + 1),
            frecuencia = frecuencia + 1
        WHERE usuario_id = %s AND tipo_transaccion = %s AND franja_horaria = %s AND dia_semana = %s
    """, (
        transaction_data['monto'],
        transaction_data['usuario_id'],
        transaction_data['tipo'],
        hora,
        dia_semana
    ))
    
    return str(transaction_id)

def obtener_transacciones_cuenta(account_id, limit=20):
    """
    Get the most recent transactions for an account
    """
    session = get_cassandra_session()
    
    query = """
        SELECT account_id, transaction_id, fecha, monto, tipo, origen, destino, 
               estatus, categoria
        FROM transacciones
        WHERE account_id = %s
        ORDER BY fecha DESC
        LIMIT %s
    """
    
    rows = session.execute(query, (account_id, limit))
    
    transactions = []
    for row in rows:
        transactions.append({
            "account_id": row.account_id,
            "transaction_id": str(row.transaction_id),
            "fecha": row.fecha.strftime("%Y-%m-%d %H:%M:%S"),
            "monto": float(row.monto),
            "tipo": row.tipo,
            "origen": row.origen,
            "destino": row.destino,
            "estatus": row.estatus,
            "categoria": row.categoria
        })
    
    return transactions

def obtener_ultimas_transacciones_usuario(usuario_id, limit=10):
    """
    Get the most recent transactions for a user
    """
    session = get_cassandra_session()
    
    query = """
        SELECT usuario_id, account_id, transaction_id, fecha, monto, tipo, 
               origen, destino, estatus, categoria
        FROM transacciones_por_usuario
        WHERE usuario_id = %s
        ORDER BY fecha DESC
        LIMIT %s
    """
    
    rows = session.execute(query, (usuario_id, limit))
    
    transactions = []
    for row in rows:
        transactions.append({
            "usuario_id": row.usuario_id,
            "account_id": row.account_id,
            "transaction_id": str(row.transaction_id),
            "fecha": row.fecha.strftime("%Y-%m-%d %H:%M:%S"),
            "monto": float(row.monto),
            "tipo": row.tipo,
            "origen": row.origen,
            "destino": row.destino,
            "estatus": row.estatus,
            "categoria": row.categoria
        })
    
    return transactions

# ─────────────────────────────
# FUNCIONES DE DETECCIÓN DE FRAUDE
# ─────────────────────────────

def verificar_velocidad_transacciones(usuario_id):
    """
    Check transaction velocity for fraud detection
    """
    session = get_cassandra_session()
    current_time = datetime.now()
    
    # Check transactions in the last minute
    minute_interval = current_time.strftime('%Y%m%d%H%M')
    
    minute_query = """
        SELECT conteo FROM velocidad_transacciones
        WHERE usuario_id = %s AND intervalo_tiempo = %s AND fecha = %s
    """
    
    minute_row = session.execute(minute_query, (usuario_id, minute_interval, current_time)).one()
    minute_count = minute_row.conteo if minute_row else 0
    
    # Check transactions in the last hour
    hour_interval = current_time.strftime('%Y%m%d%H')
    
    hour_query = """
        SELECT conteo FROM velocidad_transacciones
        WHERE usuario_id = %s AND intervalo_tiempo = %s AND fecha = %s
    """
    
    hour_row = session.execute(hour_query, (usuario_id, hour_interval, current_time)).one()
    hour_count = hour_row.conteo if hour_row else 0
    
    # Return velocity risk assessment
    return {
        "per_minute": minute_count,
        "per_hour": hour_count,
        "is_suspicious": minute_count > 3 or hour_count > 15  # Umbrales configurables
    }

def registrar_alerta_fraude(usuario_id, transaction_id, tipo_alerta, nivel_riesgo, descripcion):
    """
    Register a fraud alert
    """
    session = get_cassandra_session()
    alert_id = TimeUUID.now()
    
    query = """
        INSERT INTO alertas_fraude (
            alert_id, usuario_id, fecha, transaction_id, tipo_alerta, 
            nivel_riesgo, descripcion, estatus
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    session.execute(query, (
        alert_id,
        usuario_id,
        datetime.now(),
        uuid.UUID(transaction_id) if isinstance(transaction_id, str) else transaction_id,
        tipo_alerta,
        nivel_riesgo,
        descripcion,
        "pendiente"
    ))
    
    return str(alert_id)

def obtener_alertas_fraude_usuario(usuario_id, limit=10):
    """
    Get fraud alerts for a user
    """
    session = get_cassandra_session()
    
    query = """
        SELECT alert_id, fecha, transaction_id, tipo_alerta, nivel_riesgo, 
               descripcion, estatus
        FROM alertas_fraude
        WHERE usuario_id = %s
        ORDER BY fecha DESC
        LIMIT %s
    """
    
    rows = session.execute(query, (usuario_id, limit))
    
    alerts = []
    for row in rows:
        alerts.append({
            "alert_id": str(row.alert_id),
            "fecha": row.fecha.strftime("%Y-%m-%d %H:%M:%S"),
            "transaction_id": str(row.transaction_id),
            "tipo_alerta": row.tipo_alerta,
            "nivel_riesgo": row.nivel_riesgo,
            "descripcion": row.descripcion,
            "estatus": row.estatus
        })
    
    return alerts

def actualizar_estatus_alerta(alert_id, nuevo_estatus):
    """
    Update the status of a fraud alert
    """
    session = get_cassandra_session()
    
    # First we need to get the usuario_id and fecha as they are part of the primary key
    query1 = """
        SELECT usuario_id, fecha FROM alertas_fraude
        WHERE alert_id = %s ALLOW FILTERING
    """
    
    row = session.execute(query1, (uuid.UUID(alert_id) if isinstance(alert_id, str) else alert_id,)).one()
    
    if not row:
        return False
    
    # Now we can update the alert
    query2 = """
        UPDATE alertas_fraude
        SET estatus = %s
        WHERE usuario_id = %s AND fecha = %s AND alert_id = %s
    """
    
    session.execute(query2, (
        nuevo_estatus,
        row.usuario_id,
        row.fecha,
        uuid.UUID(alert_id) if isinstance(alert_id, str) else alert_id
    ))
    
    return True

# ─────────────────────────────
# FUNCIONES DE AUDITORÍA DE ADMINISTRADORES
# ─────────────────────────────

def registrar_accion_admin(admin_id, tipo_accion, detalles, ip_address="unknown"):
    """
    Register admin action for audit
    """
    session = get_cassandra_session()
    accion_id = TimeUUID.now()
    
    query = """
        INSERT INTO auditoria_admins (
            admin_id, fecha, accion_id, tipo_accion, detalles, ip_address
        ) VALUES (%s, %s, %s, %s, %s, %s)
    """
    
    session.execute(query, (
        admin_id,
        datetime.now(),
        accion_id,
        tipo_accion,
        detalles,
        ip_address
    ))
    
    return str(accion_id)

def obtener_acciones_admin(admin_id=None, limit=50):
    """
    Get admin actions for audit
    If admin_id is None, get all admin actions
    """
    session = get_cassandra_session()
    
    if admin_id:
        query = """
            SELECT admin_id, fecha, accion_id, tipo_accion, detalles, ip_address
            FROM auditoria_admins
            WHERE admin_id = %s
            ORDER BY fecha DESC
            LIMIT %s
        """
        rows = session.execute(query, (admin_id, limit))
    else:
        # This query requires ALLOW FILTERING which is not recommended for large datasets
        query = """
            SELECT admin_id, fecha, accion_id, tipo_accion, detalles, ip_address
            FROM auditoria_admins
            ORDER BY fecha DESC
            LIMIT %s
            ALLOW FILTERING
        """
        rows = session.execute(query, (limit,))
    
    acciones = []
    for row in rows:
        acciones.append({
            "admin_id": row.admin_id,
            "fecha": row.fecha.strftime("%Y-%m-%d %H:%M:%S"),
            "accion_id": str(row.accion_id),
            "tipo_accion": row.tipo_accion,
            "detalles": row.detalles,
            "ip_address": row.ip_address
        })
    
    return acciones

# ─────────────────────────────
# ANÁLISIS DE PATRONES Y MÉTRICAS
# ─────────────────────────────

def obtener_patrones_usuario(usuario_id):
    """
    Get transaction patterns for a user
    """
    session = get_cassandra_session()
    
    query = """
        SELECT usuario_id, tipo_transaccion, franja_horaria, dia_semana,
               monto_promedio, frecuencia
        FROM patrones_transaccion
        WHERE usuario_id = %s
    """
    
    rows = session.execute(query, (usuario_id,))
    
    patrones = []
    for row in rows:
        patrones.append({
            "usuario_id": row.usuario_id,
            "tipo_transaccion": row.tipo_transaccion,
            "franja_horaria": row.franja_horaria,
            "dia_semana": row.dia_semana,
            "monto_promedio": float(row.monto_promedio),
            "frecuencia": row.frecuencia
        })
    
    return patrones

def obtener_estadisticas_usuario(usuario_id, dias=30):
    """
    Get transaction statistics for a user in the last X days
    """
    session = get_cassandra_session()
    fecha_desde = datetime.now() - timedelta(days=dias)
    
    # Get transactions in the period
    query = """
        SELECT monto, tipo, categoria, fecha
        FROM transacciones_por_usuario
        WHERE usuario_id = %s AND fecha >= %s
        ALLOW FILTERING
    """
    
    rows = session.execute(query, (usuario_id, fecha_desde))
    
    # Process transactions into statistics
    montos_por_categoria = {}
    montos_por_tipo = {}
    montos_por_dia = {}
    total_transacciones = 0
    monto_total = 0
    
    for row in rows:
        total_transacciones += 1
        monto = float(row.monto)
        monto_total += monto
        
        # By category
        categoria = row.categoria
        if categoria in montos_por_categoria:
            montos_por_categoria[categoria] += monto
        else:
            montos_por_categoria[categoria] = monto
        
        # By type
        tipo = row.tipo
        if tipo in montos_por_tipo:
            montos_por_tipo[tipo] += monto
        else:
            montos_por_tipo[tipo] = monto
        
        # By day
        dia = row.fecha.strftime("%Y-%m-%d")
        if dia in montos_por_dia:
            montos_por_dia[dia] += monto
        else:
            montos_por_dia[dia] = monto
    
    # Calculate averages
    monto_promedio = monto_total / total_transacciones if total_transacciones > 0 else 0
    transacciones_por_dia = total_transacciones / dias if dias > 0 else 0
    
    return {
        "total_transacciones": total_transacciones,
        "monto_total": monto_total,
        "monto_promedio": monto_promedio,
        "transacciones_por_dia": transacciones_por_dia,
        "montos_por_categoria": montos_por_categoria,
        "montos_por_tipo": montos_por_tipo,
        "montos_por_dia": montos_por_dia
    }

def obtener_transacciones_periodo(usuario_id, fecha_inicio, fecha_fin):
    """
    Get all transactions for a user in a specific period
    """
    session = get_cassandra_session()
    
    # Convert string dates to datetime if needed
    if isinstance(fecha_inicio, str):
        fecha_inicio = datetime.strptime(fecha_inicio, "%Y-%m-%d")
    if isinstance(fecha_fin, str):
        fecha_fin = datetime.strptime(fecha_fin, "%Y-%m-%d")
    
    # Add time to make fecha_fin inclusive of the entire day
    fecha_fin = fecha_fin.replace(hour=23, minute=59, second=59)
    
    query = """
        SELECT usuario_id, account_id, transaction_id, fecha, monto, tipo,
               origen, destino, estatus, categoria
        FROM transacciones_por_usuario
        WHERE usuario_id = %s AND fecha >= %s AND fecha <= %s
        ALLOW FILTERING
    """
    
    rows = session.execute(query, (usuario_id, fecha_inicio, fecha_fin))
    
    transactions = []
    for row in rows:
        transactions.append({
            "usuario_id": row.usuario_id,
            "account_id": row.account_id,
            "transaction_id": str(row.transaction_id),
            "fecha": row.fecha.strftime("%Y-%m-%d %H:%M:%S"),
            "monto": float(row.monto),
            "tipo": row.tipo,
            "origen": row.origen,
            "destino": row.destino,
            "estatus": row.estatus,
            "categoria": row.categoria
        })
    
    return transactions

# ─────────────────────────────
# INICIALIZACIÓN DE LA BASE DE DATOS
# ─────────────────────────────

# Inicializar tablas si se ejecuta este módulo directamente
if __name__ == "__main__":
    try:
        crear_tablas()
        print("Tablas de Cassandra inicializadas correctamente")
    except Exception as e:
        print(f"Error al inicializar las tablas de Cassandra: {e}")
