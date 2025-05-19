import csv
import uuid
import random
from datetime import datetime

from db.MongoDB.mongo import crear_usuario, crear_cuenta_mongo
from db.Dgraph.dgraph import agregar_usuario, obtener_uid_cuenta, registrar_transaccion_dgraph
from db.Cassandra.cassandra import (
	insertar_transaccion_timestap,
	insertar_transaccion_amount,
	insertar_transaccion_status,
	insertar_transaccion
)

def cargar_usuarios_desde_csv(ruta_csv, num_transacciones=3):
	print(f"[DEBUG] Abriendo archivo CSV: {ruta_csv}")
	usuarios_lista = []
	account_ids = []
	try:
		# Primero, leer todos los usuarios y guardar sus datos y account_ids
		with open(ruta_csv, newline='', encoding='utf-8') as csvfile:
			reader = csv.DictReader(csvfile)
			for row in reader:
				try:
					usuario = {
						"nombre": row["nombre"],
						"email": row["email"],
						"usuario_id": row["usuario_id"],
						"password": row["password"],
						"pais": row["pais"],
						"ubicacion": eval(row["Location"]),  # {'lat': ..., 'lon': ...}
						"created_at": row["created_at"],
						"last_login": row["last_login"]
					}
					usuarios_lista.append(usuario)
				except Exception as e:
					print(f"[ERROR] Fallo al procesar fila del CSV: {e}")

		# Crear usuarios y cuentas en MongoDB y Dgraph, y recolectar account_ids
		for usuario in usuarios_lista:
			try:
				print(f"[DEBUG] Procesando usuario: {usuario.get('usuario_id', 'N/A')}")
				# Insertar usuario en MongoDB
				try:
					crear_usuario(usuario)
					print(f"[DEBUG] Usuario insertado en MongoDB: {usuario['usuario_id']}")
				except Exception as e:
					print(f"[ERROR] Fallo al insertar usuario en MongoDB: {e}")

				# Crear cuenta en MongoDB y obtener account_id
				lat = usuario["ubicacion"]["lat"]
				lon = usuario["ubicacion"]["lon"]
				try:
					account_id = crear_cuenta_mongo(usuario["usuario_id"], lat, lon)
					account_ids.append(account_id)
					usuario["account_id"] = account_id
					print(f"[DEBUG] Cuenta creada en MongoDB: {account_id}")
				except Exception as e:
					print(f"[ERROR] Fallo al crear cuenta en MongoDB: {e}")
					continue

				# Insertar usuario y cuenta en Dgraph
				try:
					agregar_usuario(usuario["email"], account_id)
					print(f"[DEBUG] Usuario agregado en Dgraph: {usuario['email']}")
				except Exception as e:
					print(f"[ERROR] Fallo al agregar usuario en Dgraph: {e}")

			except Exception as e:
				print(f"[ERROR] Fallo al procesar usuario: {e}")

		# Ahora procesar las transacciones
		for usuario in usuarios_lista:
			account_id = usuario.get("account_id")
			if not account_id:
				continue
			lat = usuario["ubicacion"]["lat"]
			lon = usuario["ubicacion"]["lon"]

			# Obtener UID de la cuenta en Dgraph
			try:
				from_uid = obtener_uid_cuenta(account_id)
				print(f"[DEBUG] UID de cuenta origen en Dgraph: {from_uid}")
			except Exception as e:
				print(f"[ERROR] Fallo al obtener UID de cuenta en Dgraph: {e}")
				from_uid = None

			for _ in range(num_transacciones):
				try:
					# Elegir un account_id destino diferente al actual
					posibles_destinos = [aid for aid in account_ids if aid != account_id]
					if not posibles_destinos:
						print("[WARN] No hay cuentas destino disponibles para transacción.")
						continue
					to_account_id = random.choice(posibles_destinos)

					# Obtener lat/lon de la cuenta destino (buscar en usuarios_lista)
					dest_usuario = next((u for u in usuarios_lista if u.get("account_id") == to_account_id), None)
					if dest_usuario:
						to_lat = dest_usuario["ubicacion"]["lat"]
						to_lon = dest_usuario["ubicacion"]["lon"]
					else:
						to_lat = lat + random.uniform(-0.1, 0.1)
						to_lon = lon + random.uniform(-0.1, 0.1)

					# Insertar cuenta destino en Dgraph si no existe (opcional, depende de tu modelo)
					try:
						email_destino = dest_usuario["email"] if dest_usuario else "ficticio_" + str(uuid.uuid4()) + "@mail.com"
						agregar_usuario(email_destino, to_account_id)
						print(f"[DEBUG] Usuario destino agregado en Dgraph: {email_destino}")
					except Exception as e:
						print(f"[ERROR] Fallo al agregar usuario destino en Dgraph: {e}")

					try:
						to_uid = obtener_uid_cuenta(to_account_id)
						print(f"[DEBUG] UID de cuenta destino en Dgraph: {to_uid}")
					except Exception as e:
						print(f"[ERROR] Fallo al obtener UID de cuenta destino en Dgraph: {e}")
						to_uid = None

					monto = round(random.uniform(10, 1000), 2)
					fecha = datetime.now()
					trans_id = uuid.uuid4()

					# Insertar en Cassandra
					try:
						insertar_transaccion_timestap(account_id, to_account_id, monto, fecha, trans_id, lat, lon)
						insertar_transaccion_amount(account_id, to_account_id, monto, fecha, trans_id, lat, lon)
						insertar_transaccion_status(account_id, to_account_id, monto, fecha, trans_id, lat, lon)
						insertar_transaccion(account_id, to_account_id, monto, fecha, trans_id, lat, lon)
						print(f"[DEBUG] Transacción insertada en Cassandra: {trans_id}")
					except Exception as e:
						print(f"[ERROR] Fallo al insertar transacción en Cassandra: {e}")

					# Registrar en Dgraph
					if from_uid and to_uid:
						try:
							registrar_transaccion_dgraph(from_uid, to_uid, trans_id, lat, lon)
							print(f"[DEBUG] Transacción registrada en Dgraph: {trans_id}")
						except Exception as e:
							print(f"[ERROR] Fallo al registrar transacción en Dgraph: {e}")
				except Exception as e:
					print(f"[ERROR] Fallo en la generación de transacción: {e}")
	except Exception as e:
		print(f"[ERROR] Fallo al abrir o leer el archivo CSV: {e}")
	print("Usuarios cargados desde el CSV.")