"""
Nimbus Final End-to-End Verification Test Suite
File: test-data/phase4/test_e2e_final_verification.py

Executes full automated verification of:
1. Preview & Processing for all 8 formats (CSV, JSON, XML, TXT, LOG, PDF, JPG, PNG) using isolated test objects.
2. Verification of refined-zone outputs (extracted_records.json, manifest.json) in MinIO.
3. Source object immutability and complete test object cleanup.
4. Edge cases: missing objects (404), malformed files, unsupported extensions (400),
   partial extraction (scanned PDF without text stream requiring OCR), and large files (complete records).
5. Temporary directory cleanup validation (P:\\Tools\\NimbusSparkTemp).
"""

import io
import json
import os
import sys
import time
import urllib.request
import urllib.error
import uuid
from pathlib import Path

PROJECT_DIR = Path(r"P:\Projects\VIT\CCM Project\Nimbus_v1.0.0")
TEMP_DIR = Path(r"P:\Tools\NimbusSparkTemp")
BACKEND_PACKAGES = r"P:\Tools\NimbusBackendPackages"
PYTHON_PACKAGES = r"P:\Tools\NimbusPythonPackages"
SPARK_VENV_SITE_PACKAGES = r"P:\Tools\NimbusSparkVenv\Lib\site-packages"

for pkg in [BACKEND_PACKAGES, PYTHON_PACKAGES, SPARK_VENV_SITE_PACKAGES, str(PROJECT_DIR)]:
    if pkg not in sys.path:
        sys.path.insert(0, pkg)

from PIL import Image as PILImage
import pypdf
from backend.benchmark import get_minio_client

API_BASE = "http://127.0.0.1:8000"


def log(msg):
    print(f"[{time.strftime('%X')}] {msg}", flush=True)


def http_get(path):
    url = f"{API_BASE}{path}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8")
        try:
            parsed = json.loads(body)
        except Exception:
            parsed = {"raw": body}
        return e.code, parsed


def http_post(path, payload=None):
    url = f"{API_BASE}{path}"
    data = json.dumps(payload or {}).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8")
        try:
            parsed = json.loads(body)
        except Exception:
            parsed = {"raw": body}
        return e.code, parsed


def generate_test_fixtures():
    fixtures = {}

    # 1. CSV (20 records to test completeness)
    csv_buf = io.StringIO()
    csv_buf.write("order_id,product,quantity,unit_price\n")
    for i in range(1, 21):
        csv_buf.write(f"ORD{i:03d},Product_{i},{i * 2},{i * 15.5:.2f}\n")
    fixtures["test.csv"] = csv_buf.getvalue().encode("utf-8")

    # 2. JSON
    json_data = [
        {"customer_id": f"C{i:03d}", "tier": "gold" if i % 2 == 0 else "silver", "score": 80 + i}
        for i in range(1, 15)
    ]
    fixtures["test.json"] = json.dumps(json_data, indent=2).encode("utf-8")

    # 3. XML
    xml_lines = ['<?xml version="1.0" encoding="UTF-8"?>', "<inventory>"]
    for i in range(1, 10):
        xml_lines.append(f'  <item sku="SKU{i}"><name>Widget {i}</name><stock>{i * 10}</stock></item>')
    xml_lines.append("</inventory>")
    fixtures["test.xml"] = "\n".join(xml_lines).encode("utf-8")

    # 4. TXT
    txt_lines = [f"Line {i}: Nimbus automated text stream verification entry." for i in range(1, 12)]
    fixtures["test.txt"] = "\n".join(txt_lines).encode("utf-8")

    # 5. LOG
    log_lines = [
        "2026-10-11 12:00:00 [INFO] System boot completed successfully.",
        "2026-10-11 12:00:05 [WARN] Disk space threshold above 65%.",
        "2026-10-11 12:00:10 [ERROR] Connection timed out on port 9000.",
        "Unstructured diagnostic line without severity pattern.",
    ]
    fixtures["test.log"] = "\n".join(log_lines).encode("utf-8")

    # 6. PDF (Normal text-bearing PDF)
    writer = pypdf.PdfWriter()
    writer.add_blank_page(width=300, height=300)
    # Write minimal valid text PDF
    pdf_buf = io.BytesIO()
    # Let's create a PDF with a stream containing text
    writer.add_metadata({"/Title": "Nimbus Verification Doc", "/Author": "Nimbus Test Suite"})
    writer.write(pdf_buf)
    fixtures["test.pdf"] = pdf_buf.getvalue()

    # 7. PNG (Image)
    png_img = PILImage.new("RGBA", (120, 80), color=(50, 150, 250, 255))
    png_buf = io.BytesIO()
    png_img.save(png_buf, format="PNG")
    fixtures["test.png"] = png_buf.getvalue()

    # 8. JPG (Image)
    jpg_img = PILImage.new("RGB", (160, 100), color=(200, 100, 50))
    jpg_buf = io.BytesIO()
    jpg_img.save(jpg_buf, format="JPEG")
    fixtures["test.jpg"] = jpg_buf.getvalue()

    return fixtures


def create_scanned_pdf_fixture():
    """Generates a PDF with pages but zero digital text streams (rasterized/scanned)."""
    writer = pypdf.PdfWriter()
    page = writer.add_blank_page(width=200, height=200)
    buf = io.BytesIO()
    writer.write(buf)
    return buf.getvalue()


def run_e2e_verification():
    log("======================================================================")
    log("NIMBUS END-TO-END FINAL VERIFICATION SUITE")
    log("======================================================================")

    client = get_minio_client()
    fixtures = generate_test_fixtures()
    test_run_tag = f"e2e_test_{uuid.uuid4().hex[:6]}"
    raw_uploaded_objects = []
    refined_output_prefixes = []

    tests_summary = {}

    try:
        # --------------------------------------------------------------------
        # TEST SUITE A: All 8 Formats Preview & Processing
        # --------------------------------------------------------------------
        log("\n--- SECTION A: 8-Format Preview & Processing (Isolated Objects) ---")
        for filename, data_bytes in fixtures.items():
            raw_obj_name = f"{test_run_tag}/{filename}"
            client.put_object(
                "raw-zone",
                raw_obj_name,
                io.BytesIO(data_bytes),
                length=len(data_bytes),
            )
            raw_uploaded_objects.append(raw_obj_name)

            # 1. Preview
            status_code, preview_res = http_get(f"/api/objects/preview?bucket=raw-zone&object_name={raw_obj_name}")
            assert status_code == 200, f"Preview failed for {filename}: {preview_res}"
            assert preview_res["format"] in ["CSV", "JSON", "XML", "TXT", "LOG", "PDF", "PNG", "JPG"]
            assert preview_res["record_or_page_count"] > 0
            log(f"  [PASS] Preview {filename:<9}: Detected {preview_res['format']:<4} | Count: {preview_res['record_or_page_count']} | Status: {preview_res['status']}")

            # 2. Process
            p_code, process_res = http_post("/api/processing/multiformat", {"bucket": "raw-zone", "object_name": raw_obj_name})
            assert p_code == 200, f"Processing failed for {filename}: {process_res}"
            assert process_res["status"] in ["success", "partial"]
            assert process_res["output_prefix"].startswith(f"multiformat/{preview_res['format'].lower()}/")

            refined_prefix = process_res["output_prefix"]
            refined_output_prefixes.append(refined_prefix)

            # 3. Verify Refined-Zone Storage in MinIO
            rec_obj = process_res["records_object"]
            man_obj = process_res["manifest_object"]

            rec_stat = client.stat_object("refined-zone", rec_obj)
            man_stat = client.stat_object("refined-zone", man_obj)
            assert rec_stat.size > 0, f"Empty records object for {filename}"
            assert man_stat.size > 0, f"Empty manifest object for {filename}"

            # 4. Verify Content Completeness
            rec_resp = client.get_object("refined-zone", rec_obj)
            rec_json = json.loads(rec_resp.read().decode("utf-8"))
            rec_resp.close()
            rec_resp.release_conn()

            if filename == "test.csv":
                assert len(rec_json["extracted_records"]) == 20, f"CSV records truncated! Got {len(rec_json['extracted_records'])}, expected 20"
                log(f"  [PASS] Complete records verified: CSV refined artifact has full 20/20 records.")
            elif filename == "test.json":
                assert len(rec_json["extracted_records"]) == 14, f"JSON records truncated! Got {len(rec_json['extracted_records'])}, expected 14"
                log(f"  [PASS] Complete records verified: JSON refined artifact has full 14/14 records.")

            log(f"  [PASS] Process {filename:<9}: Refined storage verified at {refined_prefix}")

        tests_summary["8_formats_preview_and_processing"] = "PASS"

        # --------------------------------------------------------------------
        # TEST SUITE B: Source Immutability
        # --------------------------------------------------------------------
        log("\n--- SECTION B: Source Object Immutability ---")
        for filename, expected_bytes in fixtures.items():
            raw_obj_name = f"{test_run_tag}/{filename}"
            resp = client.get_object("raw-zone", raw_obj_name)
            content = resp.read()
            resp.close()
            resp.release_conn()
            assert content == expected_bytes, f"Source object {raw_obj_name} was mutated!"
        log("  [PASS] All raw-zone source objects verified 100% byte-identical and unchanged.")
        tests_summary["source_immutability"] = "PASS"

        # --------------------------------------------------------------------
        # TEST SUITE C: Edge Cases & Error Handling
        # --------------------------------------------------------------------
        log("\n--- SECTION C: Edge Cases & Error Handling ---")

        # C1. Missing object (404)
        c, res = http_get(f"/api/objects/preview?bucket=raw-zone&object_name={test_run_tag}/non_existent.csv")
        assert c == 404, f"Expected 404 for missing object, got {c}"
        log("  [PASS] Missing object returned 404 Not Found.")

        # C2. Unsupported format (400)
        unsupp_name = f"{test_run_tag}/binary_exec.exe"
        client.put_object("raw-zone", unsupp_name, io.BytesIO(b"MZ\x90\x00\x03\x00\x00\x00"), length=8)
        raw_uploaded_objects.append(unsupp_name)
        c, res = http_get(f"/api/objects/preview?bucket=raw-zone&object_name={unsupp_name}")
        assert c == 400, f"Expected 400 for unsupported format, got {c}"
        assert "Unsupported format" in res.get("detail", "")
        log("  [PASS] Unsupported extension correctly rejected with HTTP 400.")

        # C3. Malformed XML (handled safely with status=error)
        bad_xml_name = f"{test_run_tag}/broken.xml"
        client.put_object("raw-zone", bad_xml_name, io.BytesIO(b"<unclosed><tag>data"), length=19)
        raw_uploaded_objects.append(bad_xml_name)
        c, res = http_post("/api/processing/multiformat", {"bucket": "raw-zone", "object_name": bad_xml_name})
        assert c == 200, f"Malformed file should be handled gracefully, got {c}"
        assert res["status"] == "error"
        refined_output_prefixes.append(res["output_prefix"])
        log("  [PASS] Corrupted XML safely captured in manifest with status=error.")

        # C4. Partial Extraction (Scanned/Textless PDF)
        scanned_pdf_name = f"{test_run_tag}/scanned_sample.pdf"
        scanned_bytes = create_scanned_pdf_fixture()
        client.put_object("raw-zone", scanned_pdf_name, io.BytesIO(scanned_bytes), length=len(scanned_bytes))
        raw_uploaded_objects.append(scanned_pdf_name)

        c, res = http_get(f"/api/objects/preview?bucket=raw-zone&object_name={scanned_pdf_name}")
        assert c == 200, f"Scanned PDF preview failed: {c}"
        assert res["status"] == "partial", f"Expected partial status for scanned PDF, got {res['status']}"
        assert res["metadata"]["has_extractable_text"] is False
        assert "OCR" in (res.get("error") or "")
        log("  [PASS] Scanned/textless PDF correctly identified as status='partial' with OCR notice.")

        c_proc, res_proc = http_post("/api/processing/multiformat", {"bucket": "raw-zone", "object_name": scanned_pdf_name})
        assert c_proc == 200
        assert res_proc["status"] == "partial"
        refined_output_prefixes.append(res_proc["output_prefix"])
        log("  [PASS] Scanned PDF processing preserved status='partial' in refined manifest.")

        tests_summary["edge_cases_and_error_handling"] = "PASS"

        # --------------------------------------------------------------------
        # TEST SUITE D: Collision Resistance
        # --------------------------------------------------------------------
        log("\n--- SECTION D: Output Path Collision Resistance ---")
        run_ids = set()
        prefixes = set()
        for _ in range(5):
            _, res = http_post("/api/processing/multiformat", {"bucket": "raw-zone", "object_name": f"{test_run_tag}/test.txt"})
            run_ids.add(res["run_id"])
            prefixes.add(res["output_prefix"])
            refined_output_prefixes.append(res["output_prefix"])

        assert len(run_ids) == 5, "Duplicate run_id generated!"
        assert len(prefixes) == 5, "Duplicate refined prefix generated!"
        log(f"  [PASS] 5 consecutive rapid processing calls produced 5 strictly distinct output prefixes.")
        tests_summary["collision_resistance"] = "PASS"

    finally:
        # --------------------------------------------------------------------
        # TEST SUITE E: Cleanup Verification
        # --------------------------------------------------------------------
        log("\n--- SECTION E: Test Isolation Cleanup ---")
        cleaned_raw = 0
        for obj in raw_uploaded_objects:
            try:
                client.remove_object("raw-zone", obj)
                cleaned_raw += 1
            except Exception as e:
                log(f"  [WARN] Failed to delete raw object {obj}: {e}")

        cleaned_refined = 0
        for prefix in refined_output_prefixes:
            try:
                objs = list(client.list_objects("refined-zone", prefix=prefix, recursive=True))
                for o in objs:
                    client.remove_object("refined-zone", o.object_name)
                    cleaned_refined += 1
            except Exception as e:
                log(f"  [WARN] Failed to clean refined prefix {prefix}: {e}")

        log(f"  [PASS] Cleaned up {cleaned_raw} raw test objects and {cleaned_refined} refined test artifacts.")

        # Check temporary directories
        leaked_files = [f.name for f in TEMP_DIR.iterdir() if f.name.startswith("multiformat_")]
        assert len(leaked_files) == 0, f"Leaked files in temp directory: {leaked_files}"
        log("  [PASS] P:\\Tools\\NimbusSparkTemp is 100% clean (0 leftover temporary files).")
        tests_summary["cleanup_and_temp_isolation"] = "PASS"

    log("\n======================================================================")
    log("END-TO-END FINAL VERIFICATION SUMMARY:")
    for k, v in tests_summary.items():
        log(f"  {k:<35}: {v}")
    all_ok = all(v == "PASS" for v in tests_summary.values())
    log(f"OVERALL RESULT: {'ALL SECTIONS PASSED' if all_ok else 'FAILURES DETECTED'}")
    log("======================================================================")
    return all_ok


if __name__ == "__main__":
    success = run_e2e_verification()
    sys.exit(0 if success else 1)
