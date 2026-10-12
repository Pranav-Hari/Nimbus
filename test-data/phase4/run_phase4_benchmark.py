import json
import os
import random
import shutil
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

# Setup paths for dependencies and packages
BACKEND_PACKAGES = r"P:\Tools\NimbusBackendPackages"
PYTHON_PACKAGES = r"P:\Tools\NimbusPythonPackages"
SPARK_VENV_SITE_PACKAGES = r"P:\Tools\NimbusSparkVenv\Lib\site-packages"
PROJECT_DIR = Path(r"P:\Projects\VIT\CCM Project\Nimbus_v1.0.0")
JAR_DIR = Path(r"P:\Tools\NimbusS3AJars")
TEMP_DIR = Path(r"P:\Tools\NimbusSparkTemp")

for pkg in [BACKEND_PACKAGES, PYTHON_PACKAGES, SPARK_VENV_SITE_PACKAGES, str(PROJECT_DIR)]:
    if pkg not in sys.path:
        sys.path.insert(0, pkg)

from minio import Minio
from backend.benchmark import load_minio_credentials, get_minio_client, run_benchmark


def log(msg):
    print(f"[{datetime.now(timezone.utc).isoformat()}] {msg}", flush=True)


def generate_synthetic_dataset(output_path, num_rows=100000, seed=42):
    """
    Generates a deterministic 100,000-row synthetic sales dataset.
    - Seed: 42 (reproducible across any machine).
    - Columns: customer_id, city, orders, amount
    - Realistic variation across 8 Indian tech/commercial hubs.
    - Varied customer order frequency and realistic order amounts.
    """
    log(f"Generating deterministic synthetic dataset ({num_rows:,} rows, seed={seed})...")
    rng = random.Random(seed)

    cities = [
        "Bengaluru",
        "Chennai",
        "Hyderabad",
        "Mumbai",
        "Pune",
        "Delhi",
        "Vellore",
        "Kolkata",
    ]
    city_weights = [0.22, 0.18, 0.16, 0.14, 0.10, 0.08, 0.07, 0.05]

    t0 = time.perf_counter()
    with open(output_path, "w", encoding="utf-8", newline="\n") as f:
        f.write("customer_id,city,orders,amount\n")

        batch = []
        for i in range(1, num_rows + 1):
            # Customer ID between 1001 and 18000
            cust_id = rng.randint(1001, 18000)

            # City chosen with population/commercial weighting
            city = rng.choices(cities, weights=city_weights, k=1)[0]

            # Orders between 1 and 12 (skewed towards 1-4)
            if rng.random() < 0.65:
                orders = rng.randint(1, 3)
            elif rng.random() < 0.90:
                orders = rng.randint(4, 7)
            else:
                orders = rng.randint(8, 12)

            # Amount: varying from 35.0 to 4500.0, with ~25% high-value orders (>= 1000.0)
            if rng.random() < 0.25:
                # High-value order
                amount = round(rng.uniform(1000.0, 4500.0), 2)
            else:
                amount = round(rng.uniform(35.0, 999.0), 2)

            batch.append(f"{cust_id},{city},{orders},{amount}\n")
            if len(batch) >= 10000:
                f.writelines(batch)
                batch.clear()

        if batch:
            f.writelines(batch)

    elapsed = round(time.perf_counter() - t0, 3)
    file_bytes = output_path.stat().st_size
    log(f"Generated {num_rows:,} rows in {elapsed}s. File size: {file_bytes:,} bytes ({file_bytes / (1024*1024):.2f} MB).")
    return file_bytes


def run_spark_processing_100k(run_id, source_s3_path, client):
    """
    Executes PySpark pipeline over the 100k synthetic dataset,
    writing isolated CSV and Parquet outputs to refined-zone/phase4-tests/.
    """
    log(f"Starting PySpark pipeline execution for run {run_id}...")
    credentials = load_minio_credentials()

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

    spark = None
    try:
        t_spark_start = time.perf_counter()
        spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("Nimbus-Phase4-100k-Benchmark")
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

        log(f"Reading source CSV from {source_s3_path}...")
        raw = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(source_s3_path)
        )

        typed = raw.select(
            F.col("customer_id").cast("long").alias("customer_id"),
            F.trim(F.col("city").cast("string")).alias("city"),
            F.col("orders").cast("long").alias("orders"),
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

        input_rows = raw.count()
        cleaned_rows = deduplicated.count()
        log(f"Input rows: {input_rows:,} | Cleaned rows: {cleaned_rows:,}")

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

        # Isolated Phase 4 test destination paths
        csv_cleaned_path = f"s3a://refined-zone/phase4-tests/sales_cleaned_csv/{run_id}"
        csv_summary_path = f"s3a://refined-zone/phase4-tests/city_sales_summary_csv/{run_id}"
        pq_cleaned_path = f"s3a://refined-zone/phase4-tests/sales_cleaned_parquet/{run_id}"
        pq_summary_path = f"s3a://refined-zone/phase4-tests/city_sales_summary_parquet/{run_id}"

        log(f"Writing CSV outputs to {csv_cleaned_path}...")
        deduplicated.write.mode("errorifexists").option("header", "true").csv(csv_cleaned_path)
        city_summary.write.mode("errorifexists").option("header", "true").csv(csv_summary_path)

        log(f"Writing Parquet outputs to {pq_cleaned_path}...")
        deduplicated.write.mode("errorifexists").parquet(pq_cleaned_path)
        city_summary.write.mode("errorifexists").parquet(pq_summary_path)

        # Read back and verify row counts
        csv_check = spark.read.option("header", "true").csv(csv_cleaned_path)
        pq_check = spark.read.parquet(pq_cleaned_path)
        csv_count = csv_check.count()
        pq_count = pq_check.count()

        if csv_count != cleaned_rows or pq_count != cleaned_rows:
            raise RuntimeError(
                f"Count mismatch on read back: expected {cleaned_rows}, CSV got {csv_count}, Parquet got {pq_count}"
            )

        log(f"PySpark processing finished in {round(time.perf_counter() - t_spark_start, 2)}s. Cleaned: {cleaned_rows:,} rows.")
        return cleaned_rows

    finally:
        if spark is not None:
            spark.stop()


def main():
    print("=" * 75)
    print("Nimbus Phase 4, Step 2: 100,000-Row CSV vs Parquet Benchmark")
    print("=" * 75)

    client = get_minio_client()
    timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_id = f"100k_{timestamp_str}_{uuid.uuid4().hex[:6]}"

    temp_csv_file = TEMP_DIR / f"synthetic_sales_100k_{uuid.uuid4().hex[:6]}.csv"
    TEMP_DIR.mkdir(parents=True, exist_ok=True)

    try:
        existing_run_id = sys.argv[1] if len(sys.argv) > 1 else None
        if existing_run_id:
            run_id = existing_run_id
            log(f"Running benchmark on existing dataset run: {run_id}")
        else:
            # Step 1: Generate synthetic 100,000-row CSV
            file_bytes = generate_synthetic_dataset(temp_csv_file, num_rows=100000, seed=42)

            # Step 2: Upload to isolated MinIO path
            s3_raw_key = "phase4-tests/sales_100k.csv"
            log(f"Uploading synthetic CSV to raw-zone/{s3_raw_key}...")
            client.fput_object("raw-zone", s3_raw_key, str(temp_csv_file))
            log("Upload complete. Removing local temporary CSV...")
            temp_csv_file.unlink(missing_ok=True)

            # Step 3: Run through PySpark ETL pipeline
            source_s3 = f"s3a://raw-zone/{s3_raw_key}"
            cleaned_rows = run_spark_processing_100k(run_id, source_s3, client)

        # Step 4: Run rigorous benchmark engine
        log("Executing benchmark engine over the 100k dataset...")
        csv_prefix = f"phase4-tests/sales_cleaned_csv/{run_id}/"
        pq_prefix = f"phase4-tests/sales_cleaned_parquet/{run_id}/"

        result = run_benchmark(
            client=client,
            run_id=run_id,
            repetitions=5,
            csv_prefix=csv_prefix,
            parquet_prefix=pq_prefix,
        )

        print("\n" + "=" * 75)
        print("100,000-ROW BENCHMARK EXECUTION RESULTS")
        print("=" * 75)
        print(f"Run ID:                {result['run_id']}")
        print(f"Engine:                {result['engine']}")
        print(f"Verified Rows:         {result['verified_row_count']:,}")
        print(f"Equivalence Verified:  {result['equivalence_verified']} (Row-level diffs: {result['verification_summary']['row_level_diffs']})")
        print(f"Aggregates Match:      Total Sales = INR {result['verification_summary']['total_sales']:,.2f} | Total Orders = {result['verification_summary']['total_orders']:,} | Cities = {result['verification_summary']['distinct_cities']}")

        sm = result["storage_metrics"]
        print("\n" + "-" * 75)
        print("STORAGE FOOTPRINT (MinIO Object Storage):")
        print(f"CSV Total Storage:     {sm['csv']['total_bytes']:,} bytes ({sm['csv']['total_bytes'] / (1024*1024):.2f} MB across {sm['csv']['part_files_count']} part file(s))")
        print(f"Parquet Total Storage: {sm['parquet']['total_bytes']:,} bytes ({sm['parquet']['total_bytes'] / (1024*1024):.2f} MB across {sm['parquet']['part_files_count']} part file(s))")
        print(f"Storage Reduction:     {sm['size_reduction_bytes']:,} bytes saved ({sm['size_reduction_pct']}% reduction!)")

        rm = result["retrieval_metrics"]
        print("\n" + "-" * 75)
        print("RETRIEVAL TIME (MinIO HTTP S3 API -> Local NVMe on P:):")
        print(f"CSV Retrieval:         {rm['csv_retrieval_ms']:.2f} ms")
        print(f"Parquet Retrieval:     {rm['parquet_retrieval_ms']:.2f} ms ({round(rm['csv_retrieval_ms'] / rm['parquet_retrieval_ms'], 2)}x faster retrieval)")

        sc = result["scan_metrics"]
        print("\n" + "-" * 75)
        print("INITIAL SCAN TIME (SELECT COUNT(*)):")
        print(f"CSV Initial Scan:      {sc['csv_initial_scan_ms']:.2f} ms")
        print(f"Parquet Initial Scan:  {sc['parquet_initial_scan_ms']:.2f} ms ({round(sc['csv_initial_scan_ms'] / sc['parquet_initial_scan_ms'], 2)}x faster scan)")

        qb = result["query_benchmarks"]
        print("\n" + "-" * 75)
        print(f"QUERY BENCHMARK COMPARISON ({qb['repetitions_per_query']} repetitions each, showing median):")
        for q_key, q_val in qb["queries"].items():
            csv_med = q_val["csv_timings_ms"]["median_ms"]
            pq_med = q_val["parquet_timings_ms"]["median_ms"]
            speedup = q_val["speedup_ratio"]
            print(f"- {q_val['title']:<35} | CSV: {csv_med:>7.2f} ms | Parquet: {pq_med:>7.2f} ms | Speedup: {speedup:>5.2f}x")

        print("\n" + "=" * 75)
        print("DETAILED JSON METRICS:")
        print(json.dumps(result, indent=2))
        print("=" * 75)

    finally:
        if temp_csv_file.exists():
            temp_csv_file.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
