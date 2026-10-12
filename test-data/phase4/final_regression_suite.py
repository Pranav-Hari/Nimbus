import json
import os
import sys
import time
import urllib.request
import urllib.error
from pathlib import Path

PROJECT_DIR = Path(r"P:\Projects\VIT\CCM Project\Nimbus_v1.0.0")
TEMP_DIR = Path(r"P:\Tools\NimbusSparkTemp")
BACKEND_PACKAGES = r"P:\Tools\NimbusBackendPackages"
PYTHON_PACKAGES = r"P:\Tools\NimbusPythonPackages"
SPARK_VENV_SITE_PACKAGES = r"P:\Tools\NimbusSparkVenv\Lib\site-packages"

for pkg in [BACKEND_PACKAGES, PYTHON_PACKAGES, SPARK_VENV_SITE_PACKAGES, str(PROJECT_DIR)]:
    if pkg not in sys.path:
        sys.path.insert(0, pkg)

def log(msg):
    print(f"[{time.strftime('%X')}] {msg}", flush=True)

def http_get(url):
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode("utf-8"))

def http_post(url, payload=None):
    data = json.dumps(payload or {}).encode("utf-8") if payload else b"{}"
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode("utf-8"))

def run_tests():
    log("======================================================================")
    log("NIMBUS PHASE 1-4 COMPREHENSIVE REGRESSION TEST SUITE")
    log("======================================================================")
    results = {}

    # TEST 1: MinIO Health & Storage
    log("\n--- TEST 1: MinIO Health & Storage API ---")
    try:
        health = http_get("http://127.0.0.1:8000/api/health")
        assert health.get("status") == "ok", f"Health status: {health}"
        assert health.get("storage") == "connected", f"Storage status: {health}"

        buckets = http_get("http://127.0.0.1:8000/api/buckets")
        bucket_names = [b["name"] for b in buckets]
        assert "raw-zone" in bucket_names and "refined-zone" in bucket_names

        raw_objs = http_get("http://127.0.0.1:8000/api/buckets/raw-zone/objects")
        refined_objs = http_get("http://127.0.0.1:8000/api/buckets/refined-zone/objects")
        log(f"PASS: MinIO connected. raw-zone has {len(raw_objs)} objects, refined-zone has {len(refined_objs)} objects.")
        results["test1_minio_storage"] = "PASS"
    except Exception as exc:
        log(f"FAIL in TEST 1: {exc}")
        results["test1_minio_storage"] = f"FAIL: {exc}"

    # TEST 2: Existing Phase 2 & Phase 3 Production Outputs
    log("\n--- TEST 2: Phase 2 & 3 Production Dataset Integrity ---")
    try:
        from backend.benchmark import get_minio_client, list_part_objects
        client = get_minio_client()
        prod_run = "20261010T192309Z-45947a40"

        csv_objs, csv_bytes = list_part_objects(client, "refined-zone", f"phase2/sales_cleaned/{prod_run}/", ".csv")
        pq_objs, pq_bytes = list_part_objects(client, "refined-zone", f"phase3/sales_cleaned/{prod_run}/", ".parquet")

        assert len(csv_objs) > 0, "No CSV parts found in production run"
        assert len(pq_objs) > 0, "No Parquet parts found in production run"
        log(f"PASS: Production run {prod_run} verified intact (CSV: {len(csv_objs)} parts, {csv_bytes} B; Parquet: {len(pq_objs)} parts, {pq_bytes} B).")
        results["test2_production_outputs"] = "PASS"
    except Exception as exc:
        log(f"FAIL in TEST 2: {exc}")
        results["test2_production_outputs"] = f"FAIL: {exc}"

    # TEST 3: Phase 3 DuckDB Columnar Analytics API
    log("\n--- TEST 3: Phase 3 DuckDB Analytics Queries (4 Predefined Queries) ---")
    try:
        queries = ["sales_by_city", "orders_by_city", "top_cities_by_sales", "high_value_orders"]
        for q in queries:
            t0 = time.perf_counter()
            resp = http_post(f"http://127.0.0.1:8000/api/analytics/duckdb?query={q}")
            elapsed = round((time.perf_counter() - t0) * 1000, 2)
            assert resp.get("status") == "success"
            q_res = resp["queries"][q]
            assert q_res["row_count"] > 0
            log(f"PASS: Query '{q}' returned {q_res['row_count']} rows in {q_res['execution_time_ms']} ms (API latency: {elapsed} ms).")
        results["test3_duckdb_analytics"] = "PASS"
    except Exception as exc:
        log(f"FAIL in TEST 3: {exc}")
        results["test3_duckdb_analytics"] = f"FAIL: {exc}"

    # TEST 4: Phase 4 Benchmark API - Small Production Run
    log("\n--- TEST 4: Phase 4 Benchmark API (Small Production Dataset) ---")
    try:
        t0 = time.perf_counter()
        resp_small = http_post("http://127.0.0.1:8000/api/benchmark/run", {"dataset": "small", "repetitions": 5})
        elapsed = round((time.perf_counter() - t0) * 1000, 2)
        assert resp_small["status"] == "success"
        assert resp_small["verified_row_count"] == 8
        assert resp_small["equivalence_verified"] is True
        assert len(resp_small["query_benchmarks"]["queries"]) == 4
        log(f"PASS: Small dataset benchmark verified (Rows: 8, Equivalence: True, API elapsed: {elapsed} ms).")
        results["test4_benchmark_small"] = "PASS"
    except Exception as exc:
        log(f"FAIL in TEST 4: {exc}")
        results["test4_benchmark_small"] = f"FAIL: {exc}"

    # TEST 5: Phase 4 Benchmark API - 100,000-Row Dataset
    log("\n--- TEST 5: Phase 4 Benchmark API (100,000-Row Dataset) ---")
    try:
        t0 = time.perf_counter()
        resp_100k = http_post("http://127.0.0.1:8000/api/benchmark/run", {"dataset": "100k", "repetitions": 5})
        elapsed = round((time.perf_counter() - t0) * 1000, 2)
        assert resp_100k["status"] == "success"
        assert resp_100k["verified_row_count"] == 100000
        assert resp_100k["equivalence_verified"] is True
        assert resp_100k["storage_metrics"]["size_reduction_pct"] > 50.0
        log(f"PASS: 100k dataset benchmark verified (Rows: 100,000, Storage reduction: {resp_100k['storage_metrics']['size_reduction_pct']}%, API elapsed: {elapsed} ms).")
        results["test5_benchmark_100k"] = "PASS"
    except Exception as exc:
        log(f"FAIL in TEST 5: {exc}")
        results["test5_benchmark_100k"] = f"FAIL: {exc}"

    # TEST 6: Temporary Directory Cleanup Validation
    log("\n--- TEST 6: Temporary Directory Cleanup on P: Drive ---")
    try:
        leaked_bench = list(TEMP_DIR.glob("nimbus_benchmark_*"))
        leaked_duck = list(TEMP_DIR.glob("duckdb_run_*"))
        total_leaks = len(leaked_bench) + len(leaked_duck)
        assert total_leaks == 0, f"Leaked temporary directories: {leaked_bench + leaked_duck}"
        log(f"PASS: P:\\Tools\\NimbusSparkTemp is clean. 0 leaked temporary directories.")
        results["test6_temp_cleanup"] = "PASS"
    except Exception as exc:
        log(f"FAIL in TEST 6: {exc}")
        results["test6_temp_cleanup"] = f"FAIL: {exc}"

    # TEST 7: Benchmark Dataset Availability Endpoint
    log("\n--- TEST 7: Benchmark Datasets Metadata Endpoint ---")
    try:
        datasets_resp = http_get("http://127.0.0.1:8000/api/benchmark/datasets")
        assert "datasets" in datasets_resp and len(datasets_resp["datasets"]) >= 2
        log(f"PASS: Available benchmark datasets listed: {[d['id'] for d in datasets_resp['datasets']]}")
        results["test7_datasets_endpoint"] = "PASS"
    except Exception as exc:
        log(f"FAIL in TEST 7: {exc}")
        results["test7_datasets_endpoint"] = f"FAIL: {exc}"

    log("\n======================================================================")
    log("FINAL REGRESSION TEST SUMMARY:")
    for k, v in results.items():
        log(f"  {k:<30}: {v}")
    all_passed = all(v == "PASS" for v in results.values())
    log(f"OVERALL RESULT: {'ALL TESTS PASSED' if all_passed else 'SOME TESTS FAILED'}")
    log("======================================================================")
    return all_passed

if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
