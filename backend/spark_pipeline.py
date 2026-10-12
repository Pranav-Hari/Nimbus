import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
JAR_DIR = Path(r"P:\Tools\NimbusS3AJars")
TEMP_DIR = Path(r"P:\Tools\NimbusSparkTemp")

START_TIME = time.perf_counter()
RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]


def log(message):
    print(f"[{datetime.now(timezone.utc).isoformat()}] {message}", flush=True)


def load_credentials():
    env_file = PROJECT_DIR / ".env"
    if not env_file.is_file():
        raise RuntimeError("Project .env file is missing.")

    settings = {}
    for line in env_file.read_text(encoding="utf-8-sig").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key in {"MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD"}:
            settings[key] = value

    if not settings.get("MINIO_ROOT_USER") or not settings.get("MINIO_ROOT_PASSWORD"):
        raise RuntimeError("Required MinIO credentials are missing from .env.")

    return settings


def main():
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("JAVA_HOME", r"P:\Tools\Java17\jdk-17.0.20.1+1")
    os.environ["PATH"] = os.environ["JAVA_HOME"] + r"\bin;" + os.environ.get("PATH", "")
    os.environ["TMP"] = str(TEMP_DIR)
    os.environ["TEMP"] = str(TEMP_DIR)
    TEMP_DIR.mkdir(parents=True, exist_ok=True)

    hadoop_aws = JAR_DIR / "hadoop-aws-3.3.4.jar"
    aws_bundle = JAR_DIR / "aws-java-sdk-bundle-1.11.1026.jar"
    if not hadoop_aws.is_file() or not aws_bundle.is_file():
        raise RuntimeError("Required S3A connector JARs were not found in P:\\Tools\\NimbusS3AJars.")

    # Load connector JARs on the driver classpath. Avoid spark.jars here:
    # on this Windows local-mode setup, Spark's JAR distribution triggered
    # a winutils.exe dependency before the job could start.
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        f'--driver-class-path "{hadoop_aws};{aws_bundle}" pyspark-shell'
    )

    credentials = load_credentials()

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = None
    try:
        log(f"Starting Nimbus Spark pipeline. Run ID: {RUN_ID}")

        spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("Nimbus-Phase2-Sales-Pipeline")
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

        source = "s3a://raw-zone/sales.csv"
        log(f"Reading source: {source}")

        raw = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(source)
        )

        required = {"customer_id", "city", "orders", "amount"}
        missing = required - set(raw.columns)
        if missing:
            raise ValueError(f"Source CSV is missing required columns: {sorted(missing)}")

        # Normalize the expected types explicitly instead of relying only on inference.
        typed = raw.select(
            F.col("customer_id").cast("long").alias("customer_id"),
            F.trim(F.col("city").cast("string")).alias("city"),
            F.col("orders").cast("long").alias("orders"),
            F.col("amount").cast("double").alias("amount"),
        )

        input_rows = raw.count()
        log(f"Input rows: {input_rows}")

        # Reject rows with missing required values, blank cities, failed casts,
        # negative order counts, or negative amounts.
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
        valid_rows = valid.count()
        invalid_rows = input_rows - valid_rows

        deduplicated = valid.dropDuplicates(
            ["customer_id", "city", "orders", "amount"]
        )
        cleaned_rows = deduplicated.count()
        duplicates_removed = valid_rows - cleaned_rows

        log(f"Invalid rows excluded: {invalid_rows}")
        log(f"Duplicate rows removed: {duplicates_removed}")
        log(f"Cleaned rows: {cleaned_rows}")

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

        summary_rows = city_summary.count()

        # Phase 2: CSV outputs
        cleaned_path = f"s3a://refined-zone/phase2/sales_cleaned/{RUN_ID}"
        summary_path = f"s3a://refined-zone/phase2/city_sales_summary/{RUN_ID}"

        log(f"Writing cleaned records (CSV) to {cleaned_path}")
        deduplicated.write.mode("errorifexists").option("header", "true").csv(cleaned_path)

        log(f"Writing city summary (CSV) to {summary_path}")
        city_summary.write.mode("errorifexists").option("header", "true").csv(summary_path)

        # Phase 3: Parquet columnar outputs
        parquet_cleaned_path = f"s3a://refined-zone/phase3/sales_cleaned/{RUN_ID}"
        parquet_summary_path = f"s3a://refined-zone/phase3/city_sales_summary/{RUN_ID}"

        log(f"Writing cleaned records (Parquet) to {parquet_cleaned_path}")
        deduplicated.write.mode("errorifexists").parquet(parquet_cleaned_path)

        log(f"Writing city summary (Parquet) to {parquet_summary_path}")
        city_summary.write.mode("errorifexists").parquet(parquet_summary_path)

        # Read the outputs back from object storage to verify they are usable.
        cleaned_check = (
            spark.read.option("header", "true").csv(cleaned_path)
        )
        summary_check = (
            spark.read.option("header", "true").csv(summary_path)
        )

        verified_cleaned_rows = cleaned_check.count()
        verified_summary_rows = summary_check.count()

        if verified_cleaned_rows != cleaned_rows:
            raise RuntimeError(
                f"Cleaned output count mismatch: expected {cleaned_rows}, "
                f"read back {verified_cleaned_rows}."
            )
        if verified_summary_rows != summary_rows:
            raise RuntimeError(
                f"Summary output count mismatch: expected {summary_rows}, "
                f"read back {verified_summary_rows}."
            )

        # Verify Parquet outputs
        parquet_cleaned_check = spark.read.parquet(parquet_cleaned_path)
        parquet_summary_check = spark.read.parquet(parquet_summary_path)

        parquet_verified_cleaned_rows = parquet_cleaned_check.count()
        parquet_verified_summary_rows = parquet_summary_check.count()

        if parquet_verified_cleaned_rows != cleaned_rows:
            raise RuntimeError(
                f"Parquet cleaned count mismatch: expected {cleaned_rows}, "
                f"read back {parquet_verified_cleaned_rows}."
            )
        if parquet_verified_summary_rows != summary_rows:
            raise RuntimeError(
                f"Parquet summary count mismatch: expected {summary_rows}, "
                f"read back {parquet_verified_summary_rows}."
            )

        # Collect aggregate outputs for validation and reporting.
        summary_data = [
            row.asDict()
            for row in summary_check.orderBy("city").collect()
        ]
        parquet_summary_data = [
            row.asDict()
            for row in parquet_summary_check.orderBy("city").collect()
        ]

        # Verify all individual cleaned records and values between CSV and Parquet
        csv_cleaned_rows = [
            row.asDict()
            for row in cleaned_check.select(
                F.col("customer_id").cast("int"),
                F.col("city"),
                F.col("orders").cast("int"),
                F.col("amount").cast("double"),
            )
            .orderBy("customer_id", "city", "orders", "amount")
            .collect()
        ]
        parquet_cleaned_rows = [
            row.asDict()
            for row in parquet_cleaned_check.select(
                F.col("customer_id").cast("int"),
                F.col("city"),
                F.col("orders").cast("int"),
                F.col("amount").cast("double"),
            )
            .orderBy("customer_id", "city", "orders", "amount")
            .collect()
        ]

        if len(csv_cleaned_rows) != len(parquet_cleaned_rows):
            raise RuntimeError(
                f"Cleaned record count mismatch between CSV ({len(csv_cleaned_rows)}) and Parquet ({len(parquet_cleaned_rows)})"
            )

        for idx, (c_row, p_row) in enumerate(zip(csv_cleaned_rows, parquet_cleaned_rows)):
            if int(c_row["customer_id"]) != int(p_row["customer_id"]):
                raise RuntimeError(
                    f"Cleaned record customer_id mismatch at index {idx}: CSV {c_row['customer_id']} vs Parquet {p_row['customer_id']}"
                )
            if str(c_row["city"]).strip() != str(p_row["city"]).strip():
                raise RuntimeError(
                    f"Cleaned record city mismatch at index {idx}: CSV {c_row['city']} vs Parquet {p_row['city']}"
                )
            if int(c_row["orders"]) != int(p_row["orders"]):
                raise RuntimeError(
                    f"Cleaned record orders mismatch at index {idx}: CSV {c_row['orders']} vs Parquet {p_row['orders']}"
                )
            if round(float(c_row["amount"]), 2) != round(float(p_row["amount"]), 2):
                raise RuntimeError(
                    f"Cleaned record amount mismatch at index {idx}: CSV {c_row['amount']} vs Parquet {p_row['amount']}"
                )

        log(f"Verified all {len(csv_cleaned_rows)} cleaned records and values match identically between CSV and Parquet.")

        # Verify city summary values between CSV and Parquet match exactly
        if len(summary_data) != len(parquet_summary_data):
            raise RuntimeError("Summary row count mismatch between CSV and Parquet")

        for csv_row, pq_row in zip(summary_data, parquet_summary_data):
            if csv_row["city"] != pq_row["city"]:
                raise RuntimeError(f"City mismatch between CSV and Parquet: {csv_row['city']} vs {pq_row['city']}")
            if int(csv_row["customer_count"]) != int(pq_row["customer_count"]):
                raise RuntimeError(f"Customer count mismatch for {csv_row['city']}: {csv_row['customer_count']} vs {pq_row['customer_count']}")
            if int(csv_row["total_orders"]) != int(pq_row["total_orders"]):
                raise RuntimeError(f"Total orders mismatch for {csv_row['city']}: {csv_row['total_orders']} vs {pq_row['total_orders']}")
            if round(float(csv_row["total_sales"]), 2) != round(float(pq_row["total_sales"]), 2):
                raise RuntimeError(f"Total sales mismatch for {csv_row['city']}: {csv_row['total_sales']} vs {pq_row['total_sales']}")
            if round(float(csv_row["average_sale_amount"]), 2) != round(float(pq_row["average_sale_amount"]), 2):
                raise RuntimeError(f"Average sale mismatch for {csv_row['city']}: {csv_row['average_sale_amount']} vs {pq_row['average_sale_amount']}")

        result = {
            "status": "success",
            "run_id": RUN_ID,
            "spark_version": spark.version,
            "execution_mode": "local[2]",
            "source": source,
            "input_rows": input_rows,
            "invalid_rows_excluded": invalid_rows,
            "duplicate_rows_removed": duplicates_removed,
            "cleaned_rows": cleaned_rows,
            "summary_rows": summary_rows,
            "verified_cleaned_rows": verified_cleaned_rows,
            "verified_summary_rows": verified_summary_rows,
            "cleaned_output": cleaned_path,
            "summary_output": summary_path,
            "parquet_cleaned_output": parquet_cleaned_path,
            "parquet_summary_output": parquet_summary_path,
            "parquet_verified_cleaned_rows": parquet_verified_cleaned_rows,
            "parquet_verified_summary_rows": parquet_verified_summary_rows,
            "city_summary": summary_data,
            "duration_seconds": round(time.perf_counter() - START_TIME, 3),
        }

        print("NIMBUS_RESULT_JSON=" + json.dumps(result, default=str), flush=True)
        log("Pipeline completed: CSV and Parquet outputs written and verified successfully.")

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log(f"Pipeline failed: {type(exc).__name__}: {exc}")
        print(
            "NIMBUS_RESULT_JSON="
            + json.dumps({
                "status": "failed",
                "run_id": RUN_ID,
                "error_type": type(exc).__name__,
                "error": str(exc),
                "duration_seconds": round(time.perf_counter() - START_TIME, 3),
            }),
            flush=True,
        )
        sys.exit(1)
