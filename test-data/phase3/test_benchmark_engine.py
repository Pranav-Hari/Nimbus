import os
import shutil
import sys
import uuid
from pathlib import Path

BACKEND_PACKAGES = r"P:\Tools\NimbusBackendPackages"
PYTHON_PACKAGES = r"P:\Tools\NimbusPythonPackages"
SPARK_VENV_SITE_PACKAGES = r"P:\Tools\NimbusSparkVenv\Lib\site-packages"
PROJECT_DIR = Path(r"P:\Projects\VIT\CCM Project\Nimbus_v1.0.0")

for pkg in [BACKEND_PACKAGES, PYTHON_PACKAGES, SPARK_VENV_SITE_PACKAGES, str(PROJECT_DIR)]:
    if pkg not in sys.path:
        sys.path.insert(0, pkg)

import duckdb
from backend.benchmark import get_minio_client, run_benchmark, list_part_objects, TEMP_DIR


def test_multipart_and_rejection():
    print("Testing benchmark engine multi-part handling and error rejection...")
    client = get_minio_client()

    # 1. Multi-part synthetic test run
    test_run_id = f"bench_mp_{uuid.uuid4().hex[:6]}"
    csv_prefix = f"phase2/sales_cleaned/{test_run_id}/"
    pq_prefix = f"phase3/sales_cleaned/{test_run_id}/"

    scratch = TEMP_DIR / f"bench_test_{uuid.uuid4().hex[:6]}"
    scratch.mkdir(parents=True, exist_ok=True)

    try:
        con = duckdb.connect()
        # CSV Part 1 (2 rows)
        c1 = scratch / "part-00000.csv"
        c1.write_text("customer_id,city,orders,amount\n101,Chennai,2,100.0\n102,Vellore,4,200.0\n", encoding="utf-8")
        # CSV Part 2 (2 rows)
        c2 = scratch / "part-00001.csv"
        c2.write_text("customer_id,city,orders,amount\n103,Chennai,1,150.0\n104,Bengaluru,5,500.0\n", encoding="utf-8")

        # Parquet Part 1 (2 rows)
        p1 = scratch / "part-00000.parquet"
        con.execute(f"COPY (SELECT 101 AS customer_id, 'Chennai' AS city, 2 AS orders, 100.0 AS amount UNION ALL SELECT 102, 'Vellore', 4, 200.0) TO '{str(p1).replace('\\', '/')}' (FORMAT PARQUET)")
        # Parquet Part 2 (2 rows)
        p2 = scratch / "part-00001.parquet"
        con.execute(f"COPY (SELECT 103 AS customer_id, 'Chennai' AS city, 1 AS orders, 150.0 AS amount UNION ALL SELECT 104, 'Bengaluru', 5, 500.0) TO '{str(p2).replace('\\', '/')}' (FORMAT PARQUET)")
        con.close()

        # Upload both parts
        client.fput_object("refined-zone", f"{csv_prefix}part-00000.csv", str(c1))
        client.fput_object("refined-zone", f"{csv_prefix}part-00001.csv", str(c2))
        client.fput_object("refined-zone", f"{pq_prefix}part-00000.parquet", str(p1))
        client.fput_object("refined-zone", f"{pq_prefix}part-00001.parquet", str(p2))

        # Check part discovery
        csv_objs, csv_bytes = list_part_objects(client, "refined-zone", csv_prefix, ".csv")
        pq_objs, pq_bytes = list_part_objects(client, "refined-zone", pq_prefix, ".parquet")
        assert len(csv_objs) == 2, f"Expected 2 CSV parts, got {len(csv_objs)}"
        assert len(pq_objs) == 2, f"Expected 2 PQ parts, got {len(pq_objs)}"

        # Run benchmark on this multi-part run
        res = run_benchmark(client=client, run_id=test_run_id, repetitions=3)
        assert res["status"] == "success"
        assert res["verified_row_count"] == 4
        assert res["storage_metrics"]["csv"]["part_files_count"] == 2
        assert res["storage_metrics"]["parquet"]["part_files_count"] == 2
        assert res["verification_summary"]["total_sales"] == 950.0
        assert res["verification_summary"]["total_orders"] == 12
        print("PASS: Multi-part dataset (2 CSV parts + 2 Parquet parts) successfully benchmarked and verified.")

        # Test Rejection of Mismatched Datasets
        # Upload an extra row to CSV part 3
        c3 = scratch / "part-00002.csv"
        c3.write_text("customer_id,city,orders,amount\n999,Mismatched,9,999.0\n", encoding="utf-8")
        client.fput_object("refined-zone", f"{csv_prefix}part-00002.csv", str(c3))

        rejected = False
        try:
            run_benchmark(client=client, run_id=test_run_id)
        except ValueError as err:
            rejected = True
            print(f"PASS: Correctly rejected mismatched dataset: {err}")

        assert rejected, "Benchmark engine should have rejected mismatched dataset!"

    finally:
        shutil.rmtree(scratch, ignore_errors=True)
        # Clean up test objects from MinIO
        for prefix in [csv_prefix, pq_prefix]:
            for obj in client.list_objects("refined-zone", prefix=prefix, recursive=True):
                client.remove_object("refined-zone", obj.object_name)

    # Verify no leaked temporary directories in TEMP_DIR
    leaked = list(TEMP_DIR.glob(f"nimbus_benchmark_{test_run_id}_*"))
    assert len(leaked) == 0, f"Leaked temporary benchmark directories: {leaked}"
    print("PASS: All temporary benchmark directories cleanly removed.")
    print("ALL BENCHMARK ENGINE TESTS PASSED!")


if __name__ == "__main__":
    test_multipart_and_rejection()
