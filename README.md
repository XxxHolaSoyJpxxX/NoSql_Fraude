# NoSql_Fraude SetUP
python -m pip install virtualenv
python -m venv ./venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt


# Docker Conections
docker run --name dgraph -d -p 8080:8080 -p 9080:9080  dgraph/standalone
docker run -d -p 8000:8000 --name ratel dgraph/ratel:latest
docker run --name casandradb -p 9042:9042 -d cassandra
docker run --name mongodb -d -p 27017:27017 mongo
