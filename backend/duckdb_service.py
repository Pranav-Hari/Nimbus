import os
import sys
import time
from pathlib import Path

SPARK_VENV_SITE_PACKAGES = r"P:\Tools\NimbusSparkVenv\Lib\site-packages"
if SPARK_VENV_SITE_PACKAGES not in sys.path:
    sys.path.insert(0, SPARK_VENV_SITE_PACKAGES)

import duckdb

TEMP_DIR = Path(r"P:\Tools\NimbusSparkTemp")
PREDEFINED_QUERIES = {
    "sales_by_city": {
        "title": "Sales by City",
        "sql": """
            SELECT
                city,
                COUNT(*) AS customer_count,
                ROUND(SUM(amount), 2) AS total_sales,
                ROUND(AVG(amount), 2) AS average_sale_amount
            FROM read_parquet(?)
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
            FROM read_parquet(?)
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
            FROM read_parquet(?)
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
            FROM read_parquet(?)
            WHERE amount >= 1000.0
            GROUP BY city
            ORDER BY high_value_sales DESC
        """
    }
}


def get_latest_parquet_run(client):
    runs = set()
    for obj in client.list_objects("refined-zone", prefix="phase3/sales_cleaned/", recursive=True):
        parts = obj.object_name.split("/")
        if len(parts) >= 3 and parts[2]:
            runs.add(parts[2])
    if not runs:
        return None
    return sorted(runs)[-1]


def get_parquet_objects(client, run_id):
    prefix = f"phase3/sales_cleaned/{run_id}/"
    parquet_objs = []
    for obj in client.list_objects("refined-zone", prefix=prefix, recursive=True):
        filename = obj.object_name.split("/")[-1]
        if obj.object_name.endswith(".parquet") and not filename.startswith("."):
            parquet_objs.append(obj.object_name)
    return sorted(parquet_objs)


def run_analytical_queries(client, run_id=None, query_key=None):
    import shutil
    import uuid

    TEMP_DIR.mkdir(parents=True, exist_ok=True)

    if not run_id:
        run_id = get_latest_parquet_run(client)
        if not run_id:
            raise ValueError("No Parquet dataset runs found in refined-zone/phase3/sales_cleaned/.")

    parquet_objects = get_parquet_objects(client, run_id)
    if not parquet_objects:
        raise ValueError(f"No Parquet part files found for run ID: {run_id}")

    unique_run_dir = TEMP_DIR / f"duckdb_run_{run_id}_{uuid.uuid4().hex[:8]}"
    unique_run_dir.mkdir(parents=True, exist_ok=True)

    con = None
    try:
        t_dl_start = time.perf_counter()
        downloaded_files = []
        total_file_size = 0

        for obj_name in parquet_objects:
            fname = obj_name.split("/")[-1]
            dest_file = unique_run_dir / fname
            client.fget_object("refined-zone", obj_name, str(dest_file))
            total_file_size += dest_file.stat().st_size
            downloaded_files.append(str(dest_file).replace("\\", "/"))

        temp_download_ms = round((time.perf_counter() - t_dl_start) * 1000, 3)

        con = duckdb.connect(database=":memory:")

        queries_to_run = {}
        if query_key:
            if query_key not in PREDEFINED_QUERIES:
                raise ValueError(f"Unknown query key: {query_key}. Available: {list(PREDEFINED_QUERIES.keys())}")
            queries_to_run[query_key] = PREDEFINED_QUERIES[query_key]
        else:
            queries_to_run = PREDEFINED_QUERIES

        results = {}
        for key, q_info in queries_to_run.items():
            t0 = time.perf_counter()
            query_sql = q_info["sql"]
            cursor = con.execute(query_sql, [downloaded_files])
            columns = [desc[0] for desc in cursor.description]
            raw_rows = cursor.fetchall()
            elapsed_ms = round((time.perf_counter() - t0) * 1000, 3)

            row_dicts = [dict(zip(columns, row)) for row in raw_rows]

            results[key] = {
                "title": q_info["title"],
                "columns": columns,
                "rows": row_dicts,
                "row_count": len(row_dicts),
                "execution_time_ms": elapsed_ms,
            }

        return {
            "status": "success",
            "run_id": run_id,
            "engine": f"DuckDB {duckdb.__version__}",
            "source_parquet_prefix": f"s3a://refined-zone/phase3/sales_cleaned/{run_id}/",
            "part_files_count": len(downloaded_files),
            "file_size_bytes": total_file_size,
            "download_time_ms": temp_download_ms,
            "queries": results,
        }

    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass
        if unique_run_dir.exists():
            try:
                shutil.rmtree(unique_run_dir, ignore_errors=True)
            except Exception:
                pass
