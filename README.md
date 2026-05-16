# Alessandra Wide-Column Database Prototype

A Java/Spring Boot prototype of a distributed wide-column database. It demonstrates the core building blocks used by Dynamo/Cassandra-style systems while keeping the implementation approachable for experimentation.

## Features

- **Wide-column data model**: rows are addressed by `table` and `rowKey`, with arbitrary `family:qualifier` columns.
- **MVCC concurrency control**: every cell write is stored as a timestamped immutable version; reads can target the latest version or a historical timestamp, and callers can inspect bounded version history including tombstones.
- **Quorum reads and writes**: coordinators route mutations to the replica set and require configurable read/write acknowledgement counts.
- **Automatic sharding**: a consistent-hash ring maps each row to a replica set across the cluster.
- **Replication**: nodes exchange mutations over gRPC with Protocol Buffers.
- **Persistence**: RocksDB stores versioned cells on disk using lexicographically ordered composite keys.
- **Containerized cluster**: `docker-compose.yml` launches a three-node local cluster.

## Tech stack

Java 17, Spring Boot, gRPC, Protocol Buffers, RocksDB, Docker, Maven, and JUnit 5.

## HTTP API

Write a row:

```bash
curl -X PUT http://localhost:8080/tables/users/rows/u1 \
  -H 'content-type: application/json' \
  -d '{"columns":[{"family":"profile","qualifier":"name","value":"Ada"}]}'
```

Read a row:

```bash
curl 'http://localhost:8080/tables/users/rows/u1?columns=profile:name'
```

Read MVCC version history for a row or selected columns:

```bash
curl 'http://localhost:8080/tables/users/rows/u1/versions?columns=profile:name&fromTimestamp=0&toTimestamp=0&includeTombstones=true&limit=25'
```

Delete a column:

```bash
curl -X DELETE 'http://localhost:8080/tables/users/rows/u1?columns=profile:name'
```

## Running locally

```bash
mvn test
mvn spring-boot:run
```

## Running a cluster

```bash
docker compose up --build
```

The compose file exposes node HTTP APIs at ports `8080`, `8081`, and `8082`. Each node also runs an internal gRPC server on port `9090` inside the Docker network.

## Configuration

Key settings are available as environment variables:

| Variable | Default | Description |
| --- | --- | --- |
| `NODE_ID` | `node-a` | Logical ID of the current node. |
| `HTTP_PORT` | `8080` | Spring Boot HTTP port. |
| `GRPC_PORT` | `9090` | gRPC server port. |
| `STORAGE_PATH` | `data/rocksdb` | RocksDB data directory. |
| `REPLICATION_FACTOR` | `3` | Number of replicas per row shard. |
| `READ_QUORUM` | `2` | Successful replica reads required. |
| `WRITE_QUORUM` | `2` | Successful replica writes required. |
