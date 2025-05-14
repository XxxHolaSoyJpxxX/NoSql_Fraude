# NoSql_Fraude

# Docker
docker run --name dgraph -d -p 8080:8080 -p 9080:9080  dgraph/standalone
docker run --name casandradb -p 9042:9042 -d cassandra
docker run --name mongodb -d -p 27017:27017 mongo
