# Nimbus — Phase 2 Completion Checkpoint

## 1. Status

Phase 2 implementation and the currently recorded regression checks are
complete. This checkpoint records the state as verified on 11 October 2026.
No Git commit is implied by this document.

## 2. Project location

`P:\Projects\VIT\CCM Project\Nimbus_v1.0.0`

## 3. Implemented components

### Object storage
- MinIO Community Edition built from pinned source.
- Docker Desktop / Docker Compose.
- Persistent MinIO data under `data/minio/`.
- Buckets include `raw-zone`, `refined-zone`, and `nimbus-test`.
- MinIO API and console are available locally on ports 9000 and 9001.

### Spark processing
- PySpark 3.5.9, Java 17, and Hadoop S3A-compatible dependencies.
- Spark execution mode: `local[2]`.
- Production source: `s3a://raw-zone/sales.csv`.
- Pipeline validates the expected columns and data types, excludes invalid
  records, removes duplicate records, and aggregates sales by city.
- Cleaned data and city summary results are written to unique run-specific
  paths in `refined-zone`.
- The pipeline reads the outputs back to verify row counts.

### Backend
- FastAPI and Uvicorn.
- Storage health and object-storage operations.
- `POST /api/processing/run` invokes the Spark pipeline.
- Structured processing results include run ID, counts, output paths,
  execution details, and city summaries.
- A lock prevents simultaneous processing jobs; a busy job is rejected
  with HTTP 409.
- Processing subprocess timeout is 600 seconds.

### Frontend
- React 19 with Vite.
- Overview, Storage Browser, and Processing views.
- Processing action calls the backend API and displays result metrics,
  city summaries, execution details, and output navigation.
- Existing navigation, upload workflow, and theme toggle remain in place.
- Full UI redesign is intentionally deferred until the end of Phase 4.

## 4. Environment locations

- Project: `P:\Projects\VIT\CCM Project\Nimbus_v1.0.0`
- Spark virtual environment: `P:\Tools\NimbusSparkVenv`
- Java 17: `P:\Tools\Java17\jdk-17.0.20.1+1`
- S3A dependencies: `P:\Tools\NimbusS3AJars`
- Backend packages: `P:\Tools\NimbusBackendPackages`
- MinIO Python SDK packages: `P:\Tools\NimbusPythonPackages`
- Spark temporary directory: `P:\Tools\NimbusSparkTemp`
- Docker Desktop WSL disk location: `P:\DockerData\DockerDesktopWSL`

The backend uses the Spark virtual environment's Python interpreter and
a `PYTHONPATH` that includes the backend and MinIO SDK package directories.
Do not copy credentials into this checkpoint.

## 5. Recorded verification results

### A. Service health
- Project directory exists.
- `GET /api/health`: status `ok`, storage `connected`.
- MinIO liveness endpoint: HTTP 200.
- Frontend development server: HTTP 200.

### B. Production processing regression
Recorded run ID: `20261010T184532Z-1f547a3b`

- Status: success
- Spark version: 3.5.9
- Execution mode: `local[2]`
- Source: `s3a://raw-zone/sales.csv`
- Input rows: 8
- Invalid rows excluded: 0
- Duplicate rows removed: 0
- Cleaned rows: 8
- Verified cleaned rows: 8
- Summary rows: 4
- Verified summary rows: 4
- Duration: 11.687 seconds

Output paths:
- `s3a://refined-zone/phase2/sales_cleaned/20261010T184532Z-1f547a3b`
- `s3a://refined-zone/phase2/city_sales_summary/20261010T184532Z-1f547a3b`

Recorded city summary:
| City | Customers | Orders | Total sales | Average sale amount |
|---|---:|---:|---:|---:|
| Bengaluru | 2 | 7 | 3031.25 | 1515.63 |
| Chennai | 2 | 3 | 1080.00 | 540.00 |
| Hyderabad | 1 | 6 | 3400.00 | 3400.00 |
| Vellore | 3 | 11 | 3465.75 | 1155.25 |

### C. Data-quality test
Test files are under `test-data/phase2/`:
- `sales_quality_test.csv`
- `spark_pipeline_quality_test.py`
- `spark_pipeline_missing_input_test.py`

The quality test used a separate test source under
`raw-zone/phase2-tests/`; it did not overwrite `raw-zone/sales.csv`.

Recorded test run ID: `20261010T183439Z-5af74198`

- Input rows: 8
- Invalid rows excluded: 4
- Duplicate rows removed: 1
- Cleaned rows: 3
- Summary rows: 2
- Verified cleaned rows: 3
- Verified summary rows: 2

Recorded aggregate results:
- Chennai: 2 records, 5 orders, total sales 150.00, average 75.00
- Vellore: 1 record, 4 orders, total sales 200.00, average 200.00

### D. Missing-input test
A separate test pointed to a nonexistent object under
`raw-zone/phase2-tests/`. The pipeline returned a structured failure with
`AnalysisException` and `PATH_NOT_FOUND`, as expected.

### E. Concurrency lock
The lock's busy branch was tested directly by acquiring the lock before
calling the processing handler. It returned HTTP 409 with the message
`A Spark processing job is already running.`

Note: this tested the handler's lock branch directly, not simultaneous
HTTP requests through a live transport test client.

### F. Frontend build
`npm run build` succeeded:
- Vite 8.3.4
- 17 modules transformed
- JavaScript bundle: 242.43 kB (74.54 kB gzip)
- CSS bundle: 18.76 kB (4.35 kB gzip)
- `frontend/dist/index.html` exists

The frontend's browser workflow, result display, output navigation, and
theme toggle were also previously tested successfully.

## 6. Known limitations and caveats

- Spark is running locally (`local[2]`), not on a distributed cluster.
- The production source is hard-coded to `raw-zone/sales.csv`.
- Only one Spark processing job may run at a time.
- The recorded API and data-quality tests use a small demonstration dataset;
  they do not establish large-scale performance.
- The FastAPI TestClient attempt was blocked by a missing `httpx2`
  dependency. No dependency was installed just for this test.
- The lock test was performed at handler level, not through concurrent live
  HTTP requests.
- Windows Hadoop utility/native-library and S3A committer warnings may appear.
  Successful runs and output verification are the important checks.
- Phase 3 work remains: Parquet datasets, DuckDB query execution, and
  benchmarking.
- Whole-UI redesign remains deferred until the end of Phase 4.

## 7. Phase 3 handoff

Before implementing Phase 3:
1. Inspect the current pipeline and output schemas rather than recreating
   the existing setup.
2. Preserve the current CSV input path and existing Phase 2 outputs.
3. Add Parquet and DuckDB functionality as a separate, testable increment.
4. Keep new test inputs and outputs in separate test prefixes.
5. Record execution time and result correctness for the planned benchmarks.
6. Avoid unnecessary package installations and check disk requirements
   before downloading anything.

## 8. Safety and workflow constraints

- No Git commits until explicitly requested later.
- Do not redesign the frontend before the end of Phase 4.
- Do not expose `.env` credentials.
- Keep project data and tools on `P:` where practical.
- Do not delete the Phase 2 test scripts or data without checking first.
- Make incremental changes, inspect outputs, and verify each step.
