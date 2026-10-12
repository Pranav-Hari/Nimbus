import json
import os
import shutil
import statistics
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

# Setup paths for dependencies and modules
BACKEND_PACKAGES = r"P:\Tools\NimbusBackendPackages"
PYTHON_PACKAGES = r"P:\Tools\NimbusPythonPackages"
SPARK_VENV_SITE_PACKAGES = r"P:\Tools\NimbusSparkVenv\Lib\site-packages"
PROJECT_DIR = Path(r"P:\Projects\VIT\CCM Project\Nimbus_v1.0.0")

for pkg in [BACKEND_PACKAGES, PYTHON_PACKAGES, SPARK_VENV_SITE_PACKAGES, str(PROJECT_DIR)]:
    if pkg not in sys.path:
        sys.path.insert(0, pkg)

import duckdb
from minio import Minio

TEMP_DIR = Path(r"P:\Tools\NimbusSparkTemp")

BENCHMARK_QUERIES = {
    "sales_by_city": {
        "title": "Sales by City",
        "sql": """
            SELECT
                city,
                COUNT(*) AS customer_count,
                ROUND(SUM(amount), 2) AS total_sales,
                ROUND(AVG(amount), 2) AS average_sale_amount
            FROM {table}
            GROUP BY city
            ORDER BY city
        """
    },
    "orders_by_city": {
        "title": "Orders by City",
        "sql": """
            SELECT
                city,
                SUM(orders) AS total_orders,
                ROUND(AVG(orders), 2) AS avg_orders_per_customer,
                MAX(orders) AS max_orders_single_customer
            FROM {table}
            GROUP BY city
            ORDER BY total_orders DESC
        """
    },
    "top_cities_by_sales": {
        "title": "Top Cities by Sales",
        "sql": """
            SELECT
                city,
                ROUND(SUM(amount), 2) AS total_sales,
                COUNT(*) AS customer_count
            FROM {table}
            GROUP BY city
            ORDER BY total_sales DESC
            LIMIT 3
        """
    },
    "high_value_orders": {
        "title": "High-Value Orders (amount >= 1000)",
        "sql": """
            SELECT
                city,
                COUNT(*) AS high_value_customers,
                SUM(orders) AS qualifying_orders,
                ROUND(SUM(amount), 2) AS high_value_sales
            FROM {table}
            WHERE amount >= 1000.0
            GROUP BY city
            ORDER BY high_value_sales DESC
        """
    }
}


def load_minio_credentials():
    env_file = PROJECT_DIR / ".env"
    if not env_file.is_file():
        raise RuntimeError("Project .env file is missing.")

    settings = {}
    for line in env_file.read_text(encoding="utf-8-sig").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        settings[key.strip()] = value.strip().strip('"').strip("'")

    if not settings.get("MINIO_ROOT_USER") or not settings.get("MINIO_ROOT_PASSWORD"):
        raise RuntimeError("Required MinIO credentials are missing from .env.")

    return settings


def get_minio_client(credentials=None):
    if credentials is None:
        credentials = load_minio_credentials()
    return Minio(
        "127.0.0.1:9000",
        access_key=credentials["MINIO_ROOT_USER"],
        secret_key=credentials["MINIO_ROOT_PASSWORD"],
        secure=False,
    )


def list_part_objects(client, bucket, prefix, extension):
    """
    List every part file under a given prefix matching extension.
    Excludes _SUCCESS files, temporary hidden files (.part-...crc), and non-part files.
    Returns (list_of_object_names, total_bytes).
    """
    objs = []
    total_bytes = 0
    for obj in client.list_objects(bucket, prefix=prefix, recursive=True):
        fname = obj.object_name.split("/")[-1]
        if obj.object_name.endswith(extension) and not fname.startswith("."):
            objs.append(obj.object_name)
            total_bytes += obj.size
    return sorted(objs), total_bytes


def parse_run_timestamp(run_id_str):
    """
    Extracts a sorting key from a run ID, supporting ISO-like timestamps
    such as YYYYMMDDTHHMMSSZ-uuid or 100k_YYYYMMDDTHHMMSSZ_uuid.
    """
    try:
        parts = run_id_str.split("_")
        for part in parts:
            ts_cand = part.split("-")[0]
            if len(ts_cand) == 16 and ts_cand.endswith("Z"):
                return datetime.strptime(ts_cand, "%Y%m%dT%H%M%SZ")
    except Exception:
        pass
    return datetime.min


def find_comparable_runs(client):
    """
    Finds run IDs that have both refined-zone CSV outputs (phase2) and
    Parquet outputs (phase3) with valid part files, ordered chronologically.
    """
    csv_runs = set()
    for obj in client.list_objects("refined-zone", prefix="phase2/sales_cleaned/", recursive=True):
        parts = obj.object_name.split("/")
        if len(parts) >= 3 and parts[2] and obj.object_name.endswith(".csv") and not parts[-1].startswith("."):
            csv_runs.add(parts[2])

    parquet_runs = set()
    for obj in client.list_objects("refined-zone", prefix="phase3/sales_cleaned/", recursive=True):
        parts = obj.object_name.split("/")
        if len(parts) >= 3 and parts[2] and obj.object_name.endswith(".parquet") and not parts[-1].startswith("."):
            parquet_runs.add(parts[2])

    common_runs = sorted(csv_runs.intersection(parquet_runs), key=lambda r: (parse_run_timestamp(r), r))
    return common_runs


def find_100k_benchmark_runs(client):
    """
    Finds 100k benchmark run IDs in refined-zone/phase4-tests/ that have both
    valid CSV and Parquet part files, ordered chronologically.
    """
    csv_runs = set()
    for obj in client.list_objects("refined-zone", prefix="phase4-tests/sales_cleaned_csv/", recursive=True):
        parts = obj.object_name.split("/")
        if len(parts) >= 3 and parts[2] and obj.object_name.endswith(".csv") and not parts[-1].startswith("."):
            csv_runs.add(parts[2])

    parquet_runs = set()
    for obj in client.list_objects("refined-zone", prefix="phase4-tests/sales_cleaned_parquet/", recursive=True):
        parts = obj.object_name.split("/")
        if len(parts) >= 3 and parts[2] and obj.object_name.endswith(".parquet") and not parts[-1].startswith("."):
            parquet_runs.add(parts[2])

    common_runs = sorted(csv_runs.intersection(parquet_runs), key=lambda r: (parse_run_timestamp(r), r))
    return common_runs


def run_benchmark(client=None, run_id=None, repetitions=5, csv_prefix=None, parquet_prefix=None):
    """
    Executes a rigorous CSV-versus-Parquet benchmark for a given run ID.
    - Measures storage footprint in bytes.
    - Measures retrieval time from MinIO separately from query execution time.
    - Executes multi-repetition query benchmarks using DuckDB.
    - Proves data equivalence across row counts, summary aggregations, and row-level diffs.
    - Cleans up all temporary directories on P: in a guaranteed finally block.
    """
    if client is None:
        client = get_minio_client()

    if csv_prefix is None or parquet_prefix is None:
        # Determine run ID
        comparable_runs = find_comparable_runs(client)
        if not comparable_runs:
            raise ValueError("No runs found with both CSV (phase2) and Parquet (phase3) cleaned datasets in refined-zone.")

        if run_id is None:
            run_id = comparable_runs[-1]
        elif run_id not in comparable_runs:
            raise ValueError(
                f"Run ID '{run_id}' does not have both CSV and Parquet datasets available. "
                f"Available comparable runs: {comparable_runs}"
            )

        csv_prefix = f"phase2/sales_cleaned/{run_id}/"
        parquet_prefix = f"phase3/sales_cleaned/{run_id}/"
    else:
        if run_id is None:
            run_id = "isolated_benchmark_run"

    csv_objects, csv_total_bytes = list_part_objects(client, "refined-zone", csv_prefix, ".csv")
    parquet_objects, parquet_total_bytes = list_part_objects(client, "refined-zone", parquet_prefix, ".parquet")

    if not csv_objects:
        raise ValueError(f"No CSV part files found under refined-zone/{csv_prefix}")
    if not parquet_objects:
        raise ValueError(f"No Parquet part files found under refined-zone/{parquet_prefix}")

    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    bench_dir = TEMP_DIR / f"nimbus_benchmark_{run_id}_{uuid.uuid4().hex[:8]}"
    bench_dir.mkdir(parents=True, exist_ok=True)
    csv_local_dir = bench_dir / "csv"
    csv_local_dir.mkdir(parents=True, exist_ok=True)
    parquet_local_dir = bench_dir / "parquet"
    parquet_local_dir.mkdir(parents=True, exist_ok=True)

    con = None
    try:
        # 1. Measure Retrieval / Download Time (MinIO to local disk on P:)
        t_csv_dl_start = time.perf_counter()
        csv_local_files = []
        for idx, obj_name in enumerate(csv_objects):
            fname = obj_name.split("/")[-1]
            dest = csv_local_dir / f"{idx}_{fname}"
            client.fget_object("refined-zone", obj_name, str(dest))
            csv_local_files.append(str(dest).replace("\\", "/"))
        csv_retrieval_ms = round((time.perf_counter() - t_csv_dl_start) * 1000, 3)

        t_pq_dl_start = time.perf_counter()
        parquet_local_files = []
        for idx, obj_name in enumerate(parquet_objects):
            fname = obj_name.split("/")[-1]
            dest = parquet_local_dir / f"{idx}_{fname}"
            client.fget_object("refined-zone", obj_name, str(dest))
            parquet_local_files.append(str(dest).replace("\\", "/"))
        parquet_retrieval_ms = round((time.perf_counter() - t_pq_dl_start) * 1000, 3)

        # 2. Connect DuckDB and register views
        con = duckdb.connect(database=":memory:")

        # Register CSV view with explicit schema casting
        con.execute(
            f"""
            CREATE VIEW v_csv AS
            SELECT
                customer_id::INT AS customer_id,
                TRIM(city::VARCHAR) AS city,
                orders::INT AS orders,
                amount::DOUBLE AS amount
            FROM read_csv({csv_local_files!r}, header=true)
            """
        )

        # Register Parquet view
        con.execute(
            f"""
            CREATE VIEW v_parquet AS
            SELECT
                customer_id::INT AS customer_id,
                TRIM(city::VARCHAR) AS city,
                orders::INT AS orders,
                amount::DOUBLE AS amount
            FROM read_parquet({parquet_local_files!r})
            """
        )

        # 3. Equivalence and Verification Checks
        # A. Row counts
        t_csv_scan = time.perf_counter()
        csv_count = con.execute("SELECT COUNT(*) FROM v_csv").fetchone()[0]
        csv_initial_scan_ms = round((time.perf_counter() - t_csv_scan) * 1000, 3)

        t_pq_scan = time.perf_counter()
        parquet_count = con.execute("SELECT COUNT(*) FROM v_parquet").fetchone()[0]
        parquet_initial_scan_ms = round((time.perf_counter() - t_pq_scan) * 1000, 3)

        if csv_count != parquet_count:
            raise ValueError(
                f"Row count mismatch between CSV ({csv_count}) and Parquet ({parquet_count}) for run {run_id}"
            )

        # B. Overall aggregate check
        csv_agg = con.execute(
            """
            SELECT
                COUNT(*) AS cnt,
                SUM(orders) AS tot_orders,
                ROUND(SUM(amount), 2) AS tot_sales,
                ROUND(AVG(amount), 2) AS avg_amount,
                COUNT(DISTINCT city) AS dist_cities
            FROM v_csv
            """
        ).fetchone()

        pq_agg = con.execute(
            """
            SELECT
                COUNT(*) AS cnt,
                SUM(orders) AS tot_orders,
                ROUND(SUM(amount), 2) AS tot_sales,
                ROUND(AVG(amount), 2) AS avg_amount,
                COUNT(DISTINCT city) AS dist_cities
            FROM v_parquet
            """
        ).fetchone()

        if csv_agg != pq_agg:
            raise ValueError(f"Dataset aggregate mismatch: CSV={csv_agg} vs Parquet={pq_agg}")

        # C. Full dataset diff check
        diff_csv_minus_pq = con.execute(
            "SELECT customer_id, city, orders, amount FROM v_csv EXCEPT SELECT customer_id, city, orders, amount FROM v_parquet"
        ).fetchall()
        diff_pq_minus_csv = con.execute(
            "SELECT customer_id, city, orders, amount FROM v_parquet EXCEPT SELECT customer_id, city, orders, amount FROM v_csv"
        ).fetchall()

        if diff_csv_minus_pq or diff_pq_minus_csv:
            raise ValueError(
                f"Row-level value mismatch found! In CSV only: {diff_csv_minus_pq}, In Parquet only: {diff_pq_minus_csv}"
            )

        equivalence_verified = True

        # 4. Storage Comparison
        size_reduction_bytes = csv_total_bytes - parquet_total_bytes
        size_reduction_pct = (
            round((size_reduction_bytes / csv_total_bytes) * 100, 2)
            if csv_total_bytes > 0
            else 0.0
        )

        # 5. Multi-Repetition Query Benchmarks
        query_benchmarks = {}
        for q_key, q_info in BENCHMARK_QUERIES.items():
            raw_sql = q_info["sql"]

            # Benchmark on CSV view
            csv_sql = raw_sql.format(table="v_csv")
            csv_reps_ms = []
            csv_result_rows = None
            for _ in range(repetitions):
                t0 = time.perf_counter()
                cur = con.execute(csv_sql)
                csv_result_rows = cur.fetchall()
                elapsed = round((time.perf_counter() - t0) * 1000, 3)
                csv_reps_ms.append(elapsed)

            # Benchmark on Parquet view
            pq_sql = raw_sql.format(table="v_parquet")
            pq_reps_ms = []
            pq_result_rows = None
            for _ in range(repetitions):
                t0 = time.perf_counter()
                cur = con.execute(pq_sql)
                pq_result_rows = cur.fetchall()
                elapsed = round((time.perf_counter() - t0) * 1000, 3)
                pq_reps_ms.append(elapsed)

            # Verify query results match identically
            if csv_result_rows != pq_result_rows:
                raise ValueError(f"Query '{q_key}' result mismatch between CSV and Parquet")

            query_benchmarks[q_key] = {
                "title": q_info["title"],
                "row_count": len(pq_result_rows),
                "csv_timings_ms": {
                    "repetitions": csv_reps_ms,
                    "median_ms": round(statistics.median(csv_reps_ms), 3),
                    "mean_ms": round(statistics.mean(csv_reps_ms), 3),
                    "min_ms": round(min(csv_reps_ms), 3),
                    "max_ms": round(max(csv_reps_ms), 3),
                },
                "parquet_timings_ms": {
                    "repetitions": pq_reps_ms,
                    "median_ms": round(statistics.median(pq_reps_ms), 3),
                    "mean_ms": round(statistics.mean(pq_reps_ms), 3),
                    "min_ms": round(min(pq_reps_ms), 3),
                    "max_ms": round(max(pq_reps_ms), 3),
                },
                "speedup_ratio": round(
                    statistics.median(csv_reps_ms) / statistics.median(pq_reps_ms), 2
                ) if statistics.median(pq_reps_ms) > 0 else 1.0,
            }

        return {
            "status": "success",
            "run_id": run_id,
            "benchmark_timestamp": datetime.now(timezone.utc).isoformat(),
            "engine": f"DuckDB {duckdb.__version__}",
            "verified_row_count": csv_count,
            "equivalence_verified": equivalence_verified,
            "verification_summary": {
                "row_count": csv_count,
                "total_orders": csv_agg[1],
                "total_sales": csv_agg[2],
                "average_amount": csv_agg[3],
                "distinct_cities": csv_agg[4],
                "row_level_diffs": 0,
            },
            "storage_metrics": {
                "csv": {
                    "prefix": f"s3a://refined-zone/{csv_prefix}",
                    "part_files_count": len(csv_objects),
                    "part_files": [obj.split("/")[-1] for obj in csv_objects],
                    "total_bytes": csv_total_bytes,
                },
                "parquet": {
                    "prefix": f"s3a://refined-zone/{parquet_prefix}",
                    "part_files_count": len(parquet_objects),
                    "part_files": [obj.split("/")[-1] for obj in parquet_objects],
                    "total_bytes": parquet_total_bytes,
                },
                "size_reduction_bytes": size_reduction_bytes,
                "size_reduction_pct": size_reduction_pct,
            },
            "retrieval_metrics": {
                "timer_definition": "Time taken by MinIO client to download all part files from object storage to local NVMe SSD on drive P:.",
                "csv_retrieval_ms": csv_retrieval_ms,
                "parquet_retrieval_ms": parquet_retrieval_ms,
                "speedup_ratio": round(csv_retrieval_ms / parquet_retrieval_ms, 2) if parquet_retrieval_ms > 0 else 1.0,
            },
            "scan_metrics": {
                "timer_definition": "Time taken by DuckDB to scan and count rows (SELECT COUNT(*)) across all downloaded part files.",
                "csv_initial_scan_ms": csv_initial_scan_ms,
                "parquet_initial_scan_ms": parquet_initial_scan_ms,
                "speedup_ratio": round(csv_initial_scan_ms / parquet_initial_scan_ms, 2) if parquet_initial_scan_ms > 0 else 1.0,
            },
            "query_benchmarks": {
                "repetitions_per_query": repetitions,
                "timer_definition": "Time taken by DuckDB in-memory engine to execute and fetch rows for each SQL query against local part files.",
                "queries": query_benchmarks,
            },
        }

    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass
        if bench_dir.exists():
            try:
                shutil.rmtree(bench_dir, ignore_errors=True)
            except Exception:
                pass


def get_available_datasets(client=None):
    """
    Returns metadata on available benchmark datasets, discovering active runs dynamically.
    """
    if client is None:
        try:
            client = get_minio_client()
        except Exception:
            pass

    avail_100k = find_100k_benchmark_runs(client) if client else []
    avail_small = find_comparable_runs(client) if client else []

    desc_100k = (
        f"Deterministic 100k-row sales dataset across 8 cities (Ready: {avail_100k[-1]})."
        if avail_100k
        else "Deterministic 100k-row sales dataset across 8 cities (Not generated yet — run python test-data/phase4/run_phase4_benchmark.py)."
    )

    desc_small = (
        f"Production sales sample from raw-zone/sales.csv (Ready: {avail_small[-1]})."
        if avail_small
        else "Production sales sample from raw-zone/sales.csv (Requires running PySpark sales pipeline)."
    )

    datasets = [
        {
            "id": "100k",
            "name": "100,000-Row Synthetic Benchmark",
            "run_id": avail_100k[-1] if avail_100k else None,
            "available": bool(avail_100k),
            "available_runs": avail_100k,
            "description": desc_100k,
            "category": "High-Volume Test",
        },
        {
            "id": "small",
            "name": "Small Production Sample",
            "run_id": avail_small[-1] if avail_small else None,
            "available": bool(avail_small),
            "available_runs": avail_small,
            "description": desc_small,
            "category": "Production Sample",
        },
    ]
    return datasets


def execute_benchmark_for_dataset(client, dataset_id="100k", run_id=None, repetitions=5):
    """
    Executes benchmark for either '100k' (isolated Phase 4 dataset)
    or 'small' / 'production' (Phase 2/3 production dataset).
    Discovers available runs dynamically without relying on stale hardcoded run IDs.
    """
    if dataset_id in {"100k", "synthetic_100k", "synthetic"}:
        available_runs = find_100k_benchmark_runs(client)
        if not available_runs:
            raise ValueError(
                "No 100,000-row synthetic benchmark datasets found in refined-zone. "
                "Please generate the 100k benchmark fixture first by running: "
                "python test-data/phase4/run_phase4_benchmark.py"
            )
        if run_id:
            if run_id not in available_runs:
                raise ValueError(
                    f"Specified 100k run ID '{run_id}' does not have complete CSV and Parquet parts available in refined-zone/phase4-tests/. "
                    f"Available runs: {available_runs}"
                )
            target_run = run_id
        else:
            target_run = available_runs[-1]

        csv_prefix = f"phase4-tests/sales_cleaned_csv/{target_run}/"
        pq_prefix = f"phase4-tests/sales_cleaned_parquet/{target_run}/"
        label = "100,000-Row Synthetic Benchmark"
        ds_id = "100k"

    elif dataset_id in {"small", "production_small", "production"}:
        comparable_runs = find_comparable_runs(client)
        if not comparable_runs:
            raise ValueError(
                "No completed PySpark sales pipeline runs found in refined-zone. "
                "Please run the PySpark sales pipeline demonstration first."
            )
        if run_id:
            if run_id not in comparable_runs:
                raise ValueError(
                    f"Specified run ID '{run_id}' does not have both CSV (phase2) and Parquet (phase3) datasets in refined-zone. "
                    f"Available comparable runs: {comparable_runs}"
                )
            target_run = run_id
        else:
            target_run = comparable_runs[-1]

        csv_prefix = f"phase2/sales_cleaned/{target_run}/"
        pq_prefix = f"phase3/sales_cleaned/{target_run}/"
        label = "Small Production Sample"
        ds_id = "small"
    else:
        raise ValueError(f"Unknown dataset '{dataset_id}'. Choose '100k' or 'small'.")

    result = run_benchmark(
        client=client,
        run_id=target_run,
        repetitions=repetitions,
        csv_prefix=csv_prefix,
        parquet_prefix=pq_prefix,
    )
    result["dataset_id"] = ds_id
    result["dataset_name"] = label
    return result


def main():
    print("=" * 70)
    print("Nimbus Phase 4 - Small-Data CSV vs Parquet Benchmark Engine")
    print("=" * 70)

    try:
        result = run_benchmark()
        print("\nBENCHMARK EXECUTION SUMMARY:")
        print(f"Run ID:                {result['run_id']}")
        print(f"Engine:                {result['engine']}")
        print(f"Verified Rows:         {result['verified_row_count']}")
        print(f"Equivalence Verified:  {result['equivalence_verified']}")
        print(f"Aggregates Match:      Total Sales = {result['verification_summary']['total_sales']}, Total Orders = {result['verification_summary']['total_orders']}")

        sm = result["storage_metrics"]
        print("\nSTORAGE FOOTPRINT:")
        print(f"CSV Total Bytes:       {sm['csv']['total_bytes']} bytes ({sm['csv']['part_files_count']} part file(s))")
        print(f"Parquet Total Bytes:   {sm['parquet']['total_bytes']} bytes ({sm['parquet']['part_files_count']} part file(s))")
        print(f"Storage Reduction:     {sm['size_reduction_bytes']} bytes ({sm['size_reduction_pct']}%)")

        rm = result["retrieval_metrics"]
        print("\nRETRIEVAL METRICS (MinIO -> P: Drive):")
        print(f"CSV Retrieval:         {rm['csv_retrieval_ms']} ms")
        print(f"Parquet Retrieval:     {rm['parquet_retrieval_ms']} ms")

        sc = result["scan_metrics"]
        print("\nINITIAL SCAN METRICS:")
        print(f"CSV Initial Scan:      {sc['csv_initial_scan_ms']} ms")
        print(f"Parquet Initial Scan:  {sc['parquet_initial_scan_ms']} ms")

        qb = result["query_benchmarks"]
        print(f"\nQUERY BENCHMARKS ({qb['repetitions_per_query']} repetitions each, showing median):")
        for q_key, q_val in qb["queries"].items():
            csv_med = q_val["csv_timings_ms"]["median_ms"]
            pq_med = q_val["parquet_timings_ms"]["median_ms"]
            speedup = q_val["speedup_ratio"]
            print(f"- {q_val['title']:<35} | CSV: {csv_med:>6.3f} ms | Parquet: {pq_med:>6.3f} ms | Ratio: {speedup}x")

        print("\n" + "=" * 70)
        print("FULL BENCHMARK JSON RESULT:")
        print(json.dumps(result, indent=2))
        print("=" * 70)

    except Exception as exc:
        print(f"\nERROR: Benchmark failed: {type(exc).__name__}: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
