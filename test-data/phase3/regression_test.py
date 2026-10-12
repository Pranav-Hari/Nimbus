import sys

BACKEND_PACKAGES = r"P:\Tools\NimbusBackendPackages"
PYTHON_PACKAGES = r"P:\Tools\NimbusPythonPackages"
SPARK_VENV_SITE_PACKAGES = r"P:\Tools\NimbusSparkVenv\Lib\site-packages"

for pkg in [BACKEND_PACKAGES, PYTHON_PACKAGES, SPARK_VENV_SITE_PACKAGES, r"P:\Projects\VIT\CCM Project\Nimbus_v1.0.0"]:
    if pkg not in sys.path:
        sys.path.insert(0, pkg)

import json
import os
import shutil
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from minio import Minio

PROJECT_DIR = Path(r"P:\Projects\VIT\CCM Project\Nimbus_v1.0.0")
JAR_DIR = Path(r"P:\Tools\NimbusS3AJars")
TEMP_DIR = Path(r"P:\Tools\NimbusSparkTemp")
SPARK_VENV_SITE_PACKAGES = r"P:\Tools\NimbusSparkVenv\Lib\site-packages"
if SPARK_VENV_SITE_PACKAGES not in sys.path:
    sys.path.insert(0, SPARK_VENV_SITE_PACKAGES)

import duckdb

def log(msg):
    print(f"[{datetime.now(timezone.utc).isoformat()}] {msg}", flush=True)

def load_credentials():
    env_file = PROJECT_DIR / ".env"
    settings = {}
    for line in env_file.read_text(encoding="utf-8-sig").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        settings[key.strip()] = value.strip().strip('"').strip("'")
    return settings

def get_minio_client(credentials):
    return Minio(
        "127.0.0.1:9000",
        access_key=credentials["MINIO_ROOT_USER"],
        secret_key=credentials["MINIO_ROOT_PASSWORD"],
        secure=False,
    )

def test_1_spark_pipeline_isolated_csv_vs_parquet():
    log("=== TEST 1: Spark Pipeline Isolated CSV vs Parquet Verification ===")
    credentials = load_credentials()
    client = get_minio_client(credentials)

    test_csv = PROJECT_DIR / "test-data" / "phase2" / "sales_quality_test.csv"
    if not test_csv.is_file():
        raise FileNotFoundError(f"Test CSV not found: {test_csv}")

    # Upload test CSV to isolated test prefix in raw-zone
    remote_test_key = "phase3-tests/sales_quality_test.csv"
    client.fput_object("raw-zone", remote_test_key, str(test_csv))
    log(f"Uploaded isolated test input to raw-zone/{remote_test_key}")

    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("JAVA_HOME", r"P:\Tools\Java17\jdk-17.0.20.1+1")
    os.environ["PATH"] = os.environ["JAVA_HOME"] + r"\bin;" + os.environ.get("PATH", "")
    os.environ["TMP"] = str(TEMP_DIR)
    os.environ["TEMP"] = str(TEMP_DIR)
    TEMP_DIR.mkdir(parents=True, exist_ok=True)

    hadoop_aws = JAR_DIR / "hadoop-aws-3.3.4.jar"
    aws_bundle = JAR_DIR / "aws-java-sdk-bundle-1.11.1026.jar"
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        f'--driver-class-path "{hadoop_aws};{aws_bundle}" pyspark-shell'
    )

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    test_run_id = f"test_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:6]}"
    spark = None
    try:
        spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("Nimbus-Phase3-Test")
            .config("spark.ui.enabled", "false")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.access.key", credentials["MINIO_ROOT_USER"])
            .config("spark.hadoop.fs.s3a.secret.key", credentials["MINIO_ROOT_PASSWORD"])
            .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
            .config("spark.hadoop.fs.s3a.fast.upload", "true")
            .config("spark.hadoop.fs.s3a.fast.upload.buffer", "array")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        source = f"s3a://raw-zone/{remote_test_key}"
        raw = spark.read.option("header", "true").option("inferSchema", "true").csv(source)
        typed = raw.select(
            F.col("customer_id").cast("int").alias("customer_id"),
            F.trim(F.col("city")).alias("city"),
            F.col("orders").cast("int").alias("orders"),
            F.col("amount").cast("double").alias("amount"),
        )
        valid_condition = (
            F.col("customer_id").isNotNull()
            & F.col("city").isNotNull()
            & (F.length(F.col("city")) > 0)
            & F.col("orders").isNotNull()
            & F.col("amount").isNotNull()
            & (F.col("orders") >= 0)
            & (F.col("amount") >= 0)
        )
        valid = typed.filter(valid_condition)
        deduplicated = valid.dropDuplicates(["customer_id", "city", "orders", "amount"])

        city_summary = (
            deduplicated
            .groupBy("city")
            .agg(
                F.count("*").alias("customer_count"),
                F.sum("orders").alias("total_orders"),
                F.round(F.sum("amount"), 2).alias("total_sales"),
                F.round(F.avg("amount"), 2).alias("average_sale_amount"),
            )
            .orderBy("city")
        )

        # Isolated output paths
        csv_cleaned_path = f"s3a://refined-zone/phase3-tests/sales_cleaned/{test_run_id}"
        csv_summary_path = f"s3a://refined-zone/phase3-tests/city_sales_summary/{test_run_id}"
        pq_cleaned_path = f"s3a://refined-zone/phase3-tests/sales_cleaned_pq/{test_run_id}"
        pq_summary_path = f"s3a://refined-zone/phase3-tests/city_sales_summary_pq/{test_run_id}"

        deduplicated.write.mode("errorifexists").option("header", "true").csv(csv_cleaned_path)
        city_summary.write.mode("errorifexists").option("header", "true").csv(csv_summary_path)
        deduplicated.write.mode("errorifexists").parquet(pq_cleaned_path)
        city_summary.write.mode("errorifexists").parquet(pq_summary_path)

        # Read back
        cleaned_check = spark.read.option("header", "true").csv(csv_cleaned_path)
        summary_check = spark.read.option("header", "true").csv(csv_summary_path)
        parquet_cleaned_check = spark.read.parquet(pq_cleaned_path)
        parquet_summary_check = spark.read.parquet(pq_summary_path)

        # Strengthened Verification
        csv_cleaned_rows = [
            row.asDict()
            for row in cleaned_check.select(
                F.col("customer_id").cast("int"),
                F.col("city"),
                F.col("orders").cast("int"),
                F.col("amount").cast("double"),
            ).orderBy("customer_id", "city", "orders", "amount").collect()
        ]
        pq_cleaned_rows = [
            row.asDict()
            for row in parquet_cleaned_check.select(
                F.col("customer_id").cast("int"),
                F.col("city"),
                F.col("orders").cast("int"),
                F.col("amount").cast("double"),
            ).orderBy("customer_id", "city", "orders", "amount").collect()
        ]

        assert len(csv_cleaned_rows) == len(pq_cleaned_rows), "Cleaned record count mismatch"
        for idx, (c_row, p_row) in enumerate(zip(csv_cleaned_rows, pq_cleaned_rows)):
            assert int(c_row["customer_id"]) == int(p_row["customer_id"]), f"customer_id mismatch at {idx}"
            assert str(c_row["city"]).strip() == str(p_row["city"]).strip(), f"city mismatch at {idx}"
            assert int(c_row["orders"]) == int(p_row["orders"]), f"orders mismatch at {idx}"
            assert round(float(c_row["amount"]), 2) == round(float(p_row["amount"]), 2), f"amount mismatch at {idx}"

        log(f"PASS: Verified all {len(csv_cleaned_rows)} cleaned records match identically between CSV and Parquet.")

        # Summary verification
        summary_data = [row.asDict() for row in summary_check.orderBy("city").collect()]
        pq_summary_data = [row.asDict() for row in parquet_summary_check.orderBy("city").collect()]
        assert len(summary_data) == len(pq_summary_data), "Summary count mismatch"
        for c_row, p_row in zip(summary_data, pq_summary_data):
            assert c_row["city"] == p_row["city"]
            assert int(c_row["customer_count"]) == int(p_row["customer_count"])
            assert int(c_row["total_orders"]) == int(p_row["total_orders"])
            assert round(float(c_row["total_sales"]), 2) == round(float(p_row["total_sales"]), 2)
            assert round(float(c_row["average_sale_amount"]), 2) == round(float(p_row["average_sale_amount"]), 2)

        log(f"PASS: Verified all {len(summary_data)} summary aggregate rows match identically.")
        return test_run_id

    finally:
        if spark is not None:
            spark.stop()

def test_2_duckdb_multipart_and_temp_cleanup():
    log("=== TEST 2: DuckDB Multi-Part Parquet Processing & Temp Cleanup ===")
    credentials = load_credentials()
    client = get_minio_client(credentials)

    # 1. Create a synthetic 3-part Parquet dataset under an isolated test prefix
    test_run_id = f"multipart_{uuid.uuid4().hex[:8]}"
    prefix = f"phase3/sales_cleaned/{test_run_id}/"

    scratch_dir = TEMP_DIR / f"test_create_multipart_{uuid.uuid4().hex[:6]}"
    scratch_dir.mkdir(parents=True, exist_ok=True)

    try:
        con = duckdb.connect(database=":memory:")
        # Write part 1: Chennai (2 rows)
        part1 = scratch_dir / "part-00000.parquet"
        con.execute(f"COPY (SELECT 101 AS customer_id, 'Chennai' AS city, 2 AS orders, 1200.0 AS amount UNION ALL SELECT 102, 'Chennai', 3, 300.0) TO '{str(part1).replace('\\', '/')}' (FORMAT PARQUET)")

        # Write part 2: Vellore (2 rows)
        part2 = scratch_dir / "part-00001.parquet"
        con.execute(f"COPY (SELECT 103 AS customer_id, 'Vellore' AS city, 4 AS orders, 1500.0 AS amount UNION ALL SELECT 104, 'Vellore', 1, 200.0) TO '{str(part2).replace('\\', '/')}' (FORMAT PARQUET)")

        # Write part 3: Bengaluru (2 rows)
        part3 = scratch_dir / "part-00002.parquet"
        con.execute(f"COPY (SELECT 105 AS customer_id, 'Bengaluru' AS city, 5 AS orders, 2500.0 AS amount UNION ALL SELECT 106, 'Bengaluru', 2, 400.0) TO '{str(part3).replace('\\', '/')}' (FORMAT PARQUET)")
        con.close()

        # Upload all 3 parts to MinIO refined-zone
        client.fput_object("refined-zone", f"{prefix}part-00000.parquet", str(part1))
        client.fput_object("refined-zone", f"{prefix}part-00001.parquet", str(part2))
        client.fput_object("refined-zone", f"{prefix}part-00002.parquet", str(part3))
        log(f"Uploaded 3 part files to refined-zone/{prefix}")

        # Import duckdb_service
        from backend.duckdb_service import run_analytical_queries

        # Record temp directory state before
        temp_dirs_before = set(TEMP_DIR.glob("duckdb_run_*"))

        # Run analytical queries targeting this multi-part run
        result = run_analytical_queries(client, run_id=test_run_id)

        # Record temp directory state after
        temp_dirs_after = set(TEMP_DIR.glob("duckdb_run_*"))

        assert result["status"] == "success", f"Query status failed: {result}"
        assert result["part_files_count"] == 3, f"Expected 3 part files, got {result['part_files_count']}"
        assert result["run_id"] == test_run_id

        # Verify multi-part queries returned all rows across all 3 files
        sales_q = result["queries"]["sales_by_city"]
        assert sales_q["row_count"] == 3, f"Expected 3 cities, got {sales_q['row_count']}"
        cities = {r["city"] for r in sales_q["rows"]}
        assert cities == {"Bengaluru", "Chennai", "Vellore"}, f"Unexpected cities: {cities}"

        # Total sales across all 3 parts = (1200+300) + (1500+200) + (2500+400) = 1500 + 1700 + 2900 = 6100.0
        total_sales_sum = sum(r["total_sales"] for r in sales_q["rows"])
        assert total_sales_sum == 6100.0, f"Expected 6100.0 total sales, got {total_sales_sum}"

        # High-value orders query (amount >= 1000): should have 1 customer each from Bengaluru, Chennai, Vellore
        high_val_q = result["queries"]["high_value_orders"]
        assert high_val_q["row_count"] == 3
        high_val_total = sum(r["high_value_sales"] for r in high_val_q["rows"])
        assert high_val_total == 1200.0 + 1500.0 + 2500.0, f"Expected 5200.0 high-value sales, got {high_val_total}"

        # Verify reliable temp cleanup: no new duckdb_run_ directory remained
        leftover = temp_dirs_after - temp_dirs_before
        assert len(leftover) == 0, f"Leaked temporary directories: {leftover}"

        log(f"PASS: Multi-part Parquet (3 parts, 6 rows across 3 cities) correctly aggregated. Total sales: {total_sales_sum}. Temp directory reliably cleaned up.")

    finally:
        shutil.rmtree(scratch_dir, ignore_errors=True)
        # Clean up synthetic test objects from MinIO
        for obj in client.list_objects("refined-zone", prefix=prefix, recursive=True):
            client.remove_object("refined-zone", obj.object_name)

def test_3_duckdb_production_dataset_read():
    log("=== TEST 3: DuckDB Query Execution Over Latest Production Parquet ===")
    credentials = load_credentials()
    client = get_minio_client(credentials)

    from backend.duckdb_service import run_analytical_queries, get_latest_parquet_run

    latest_run = get_latest_parquet_run(client)
    log(f"Latest production run found in refined-zone: {latest_run}")
    assert latest_run is not None, "No production Parquet run found"

    temp_dirs_before = set(TEMP_DIR.glob("duckdb_run_*"))
    result = run_analytical_queries(client, run_id=latest_run)
    temp_dirs_after = set(TEMP_DIR.glob("duckdb_run_*"))

    assert result["status"] == "success"
    assert len(temp_dirs_after - temp_dirs_before) == 0, "Temp directory leaked"

    for q_key, q_data in result["queries"].items():
        log(f"Query '{q_key}' [{q_data['title']}]: {q_data['row_count']} rows in {q_data['execution_time_ms']} ms")
        assert q_data["row_count"] > 0, f"Query {q_key} returned 0 rows"
        assert len(q_data["columns"]) > 0

    log(f"PASS: All 4 analytical queries executed successfully on production run {latest_run}. Engine: {result['engine']}. Download time: {result['download_time_ms']} ms.")

def main():
    log("Starting Phase 3 Safe Regression Test Suite...")
    test_1_spark_pipeline_isolated_csv_vs_parquet()
    test_2_duckdb_multipart_and_temp_cleanup()
    test_3_duckdb_production_dataset_read()
    log("ALL REGRESSION TESTS PASSED SUCCESSFULLY!")

if __name__ == "__main__":
    main()
