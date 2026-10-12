# Nimbus — Local S3 Cloud Data Lake

Nimbus is a local cloud data lake project built incrementally using
S3-compatible object storage and distributed data processing.

## Current implementation

- Object storage: MinIO Community Edition, built from pinned source
- Container runtime: Docker Desktop
- Service orchestration: Docker Compose
- Python: 3.12
- S3 client: Python MinIO SDK
- Persistent storage: `data/minio/`
- Credentials: `.env` (local only)

## Services

| Service | Local URL |
|---|---|
| MinIO S3 API | http://127.0.0.1:9000 |
| MinIO Web Console | http://127.0.0.1:9001 |
| Liveness check | http://127.0.0.1:9000/minio/health/live |

## Start and stop

Run these commands from this directory.

Start:
```powershell
docker compose up -d
```

Check status:
```powershell
docker compose ps
```

View logs:
```powershell
docker compose logs --tail 50 minio
```

Stop the service:
```powershell
docker compose down
```

Start again:
```powershell
docker compose up -d
```

The `data/minio/` directory is bind-mounted into the container to preserve
stored objects across container restarts and recreation.

## Current validation

- Linux AMD64 MinIO binary built successfully.
- Docker image starts successfully.
- Liveness endpoint returns HTTP 200.
- Test bucket `nimbus-test` created.
- Test object uploaded and downloaded successfully.
- Downloaded contents matched the original file.
- Bucket and object survived a container restart.

## Important notes

- The current MinIO Community Edition source is an archived/unsupported
  upstream codebase. This is a development setup, not a production deployment.
- The single-drive setup has no storage redundancy.
- Do not commit `.env`, credentials, or stored data.
- The local service is bound to loopback addresses, not exposed to the LAN.

## Current limitations

This checkpoint validates the object-storage layer only. Distributed
processing, Parquet datasets, query execution, and any application interface
must be implemented and tested in their respective project phases.
