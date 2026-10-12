# Nimbus Architecture & Design Specification

## 1. Overview

**Nimbus** is a local simulation of an S3-compatible cloud data lake. It models enterprise data lake tiering by enforcing a strict two-zone architecture:
- **Raw Zone (`raw-zone`)**: The immutable landing zone for original, unprocessed files across multiple formats (CSV, JSON, XML, TXT, LOG, PDF, JPG, PNG).
- **Refined Zone (`refined-zone`)**: The processed storage layer containing format-specific extracted records, structured manifests, and derived analytics artifacts (including Snappy-compressed columnar Parquet datasets).

---

## 2. End-to-End System Architecture

```text
                               +-------------------------------------+
                               |          USER / BROWSER             |
                               +-------------------------------------+
                                                  |
                                                  v
                               +-------------------------------------+
                               |   React / Vite Frontend Console     |
                               |   (Storage Browser, Previews, CRUD) |
                               +-------------------------------------+
                                                  |  HTTP REST / Streaming
                                                  v
                               +-------------------------------------+
                               |          FastAPI Backend            |
                               | (CRUD Guards, Extractors, Security) |
                               +-------------------------------------+
                                     /            |            \
                                    /             |             \
            Format Extraction Engine       Storage API           Analytical Services
         (Standard Lib, pypdf, Pillow)   (MinIO Python SDK)      (DuckDB & PySpark)
                   |                              |                      |
                   |                              v                      |
                   |                  +-----------------------+          |
                   |                  | Local MinIO (Docker)  |          |
                   |                  | - S3 API:  :9000      |          |
                   |                  | - Console: :9001      |          |
                   |                  +-----------------------+          |
                   |                              |                      |
                   |              +---------------+---------------+      |
                   |              |                               |      |
                   v              v                               v      v
            +---------------------------+           +---------------------------+
            |         raw-zone          |           |       refined-zone        |
            |  - Original input files   |           |  - Extracted JSON records |
            |  - Immutable landing tier |           |  - Execution manifests    |
            |  - Direct upload permitted|           |  - Cleaned CSV & Parquet  |
            +---------------------------+           +---------------------------+
```

---

## 3. Storage Zone Roles & Isolation Rules

### Raw Zone (`raw-zone`)
1. **Permitted Operations:** Create (upload), Read (list, preview, download), Overwrite (strictly with user authorization), Delete (individual object).
2. **Immutability:** Processing workflows read objects without modifying or truncating the source bytes.
3. **Overwrite Safeguard:** Backend collision detection checks object existence; unconfirmed duplicate uploads trigger `HTTP 409 Conflict`. Overwriting requires explicit `?overwrite=true` query parameter authorization.

### Refined Zone (`refined-zone`)
1. **Permitted Operations:** Read (list, preview, download), Delete (individual object), Create (only via automated processing workflows).
2. **Zone Isolation:** Direct file uploads are strictly blocked at both UI and API levels (`HTTP 400 Bad Request`).
3. **Multi-Format Output Schema:** Processing writes two standardized artifacts per run under `multiformat/<format>/<run_id>/`:
   - `extracted_records.json`: Parsed records or content streams.
   - `manifest.json`: Metadata, record/page count, execution duration (`extraction_duration_ms`, `processing_duration_ms`), and library status.

---

## 4. Processing Engines & Workflow Distinction

Nimbus maintains two strictly separated processing workflows to serve product requirements while supporting empirical data-engineering demonstrations:

### A. Generic Multi-Format Engine (Product Core)
- **Implementation:** `backend/multiformat_service.py`
- **Supported Formats:** **CSV, JSON, XML, TXT, LOG, PDF, JPG, PNG**, and in-process **Parquet** inspection.
- **Source Handling:** Source files in `raw-zone` remain strictly immutable and byte-preserved.
- **Output Hierarchy:** Output is written exclusively under `refined-zone/multiformat/<format>/<run_id>/`:
  - `extracted_records.json`: Parsed records, line streams, or image/document metadata.
  - `manifest.json`: Execution metadata, timing (`extraction_duration_ms`, `processing_duration_ms`), and library status.
- **Extraction Mechanics:**
  - Standard library extractors for JSON, XML, plain text, and structured server logs.
  - `pypdf` integration for digital PDF text streams and document properties (scanned PDFs without OCR are cleanly flagged with `status="partial"`).
  - `Pillow` integration for image dimension decoding, color profiles, and EXIF tags.
  - DuckDB integration for Parquet schema inspection.
- *Crucial Boundary:* Generic CSV multi-format extraction extracts structured JSON records; it does **not** generate Parquet files.

### B. PySpark & DuckDB Columnar Demonstration Workflow
- **Implementation:** `backend/spark_pipeline.py`, `backend/duckdb_service.py`, and `backend/benchmark.py`
- **Execution Mode:** Local parallel execution (`local[2]` master) using local S3A connectors against local MinIO (not a multi-machine cluster).
- **Workflow & Output Paths:**
  1. Reads `s3a://raw-zone/sales.csv`.
  2. Cleans, deduplicates, and aggregates sales transactions.
  3. Writes dual outputs to `refined-zone`:
     - Cleaned CSV parts: `refined-zone/phase2/sales_cleaned/<run_id>/`
     - Snappy Parquet parts: `refined-zone/phase3/sales_cleaned/<run_id>/`
  4. DuckDB queries the Parquet dataset via memory-resident columnar scans.
  5. Empirical benchmarks measure storage size reduction and query speedup across multiple repetitions.
- **Dynamic Run Resolution:** The benchmark engine dynamically sorts and resolves valid runs chronologically (`parse_run_timestamp`), verifying that both CSV and Parquet part files exist for a run before executing comparisons. No static or historical run IDs are required.

---

## 5. Verification Architecture & Test Suites

Testing is organized into distinct categories based on infrastructure prerequisites:

### A. Standalone Unit Tests (No External Services Required)
- **`test-data/phase4/test_multiformat_service.py`**
  - Uses local in-memory fixtures.
  - Validates extraction, edge cases, missing dependency fallback, and temporary workspace cleanup (`P:\Tools\NimbusSparkTemp`) across all 8 supported file formats.

### B. MinIO & Integration Tests (Requires Running MinIO & FastAPI Backend)
- **`test-data/phase4/test_zone_crud.py`**
  - Validates direct upload restriction on `refined-zone` (`HTTP 400`).
  - Verifies overwrite conflict detection (`HTTP 409`) and authorized overwriting (`?overwrite=true`).
  - Verifies exact-byte streaming downloads and single-object deletion guards.
- **`test-data/phase4/test_multiformat_api.py`**
  - Tests `GET /api/objects/preview` and `POST /api/processing/multiformat`.
  - Verifies refined artifact creation, manifest recording, and source object immutability.
- **`test-data/phase4/test_e2e_final_verification.py`**
  - Executes comprehensive end-to-end verification across preview, processing, source immutability, edge cases, and output collision resistance.
- **`test-data/phase3/test_benchmark_engine.py`**
  - Tests multipart part file discovery and verifies that mismatched CSV vs. Parquet row counts trigger strict error rejection.

### C. Benchmark Suites & Prerequisites
- **Small Production Benchmark (8 Rows):**
  - *Prerequisite:* Requires at least one completed PySpark demonstration pipeline run providing matching CSV part files under `refined-zone/phase2/sales_cleaned/<run_id>/` and Parquet part files under `refined-zone/phase3/sales_cleaned/<run_id>/`.
  - *Execution:* Via UI or `POST /api/benchmark/run` (`{"dataset": "small"}`).
- **100,000-Row Synthetic Benchmark:**
  - *Prerequisite:* Requires the high-volume synthetic benchmark fixture. If absent, generate via:
    ```bash
    python test-data/phase4/run_phase4_benchmark.py
    ```
    This script deterministically generates 100k rows (`seed=42`), uploads `raw-zone/phase4-tests/sales_100k.csv`, executes PySpark ETL, outputs partitioned CSV and Parquet parts under `refined-zone/phase4-tests/`, and validates row equivalence.
  - *Execution:* Via UI or `POST /api/benchmark/run` (`{"dataset": "100k"}`).

### D. Latest Verified Empirical Benchmark Results
- **Small Dataset (8 Rows, Run `20261012T134937Z-ff8deb12`):**
  - 8 verified rows, 0 row-level diffs, row equivalence verified `True`.
  - Illustrates columnar metadata overhead on tiny files (CSV: 212 bytes vs Parquet: 1,482 bytes).
  - Query speedups: 1.64x to 2.15x.
- **100,000-Row Dataset (Run `100k_20261012T135524Z_9a50af`):**
  - 100,000 verified rows, 0 row-level diffs, row equivalence verified `True`.
  - Storage reduction: 2.26 MB (CSV) to 0.86 MB (Parquet) $\implies$ **61.99% storage reduction**.
  - Initial scan speedup: **5.07x faster**.
  - Query speedups: **16.5x to 21.5x faster** across analytical aggregations.
