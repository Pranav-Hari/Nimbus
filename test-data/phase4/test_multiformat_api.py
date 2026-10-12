"""
Nimbus Multi-Format API Integration Test Suite
Location: test-data/phase4/test_multiformat_api.py

Validates GET /api/objects/preview and POST /api/processing/multiformat endpoints:
- Supported formats: CSV, JSON, XML, TXT, LOG, PDF, JPG, PNG
- Validation errors: missing inputs, invalid buckets, unsupported formats
- Missing objects: 404 handling
- Processing failures: safe error trapping and manifest logging
- Isolation: uses isolated test objects under a unique test prefix,
  verifies source objects are NEVER modified/deleted,
  and cleans up all test outputs without touching production data.
"""

import io
import json
import os
import struct
import sys
import time
import urllib.request
import urllib.error
import uuid
from pathlib import Path

# Package paths
PROJECT_DIR = Path(r"P:\Projects\VIT\CCM Project\Nimbus_v1.0.0")
TEMP_DIR = Path(r"P:\Tools\NimbusSparkTemp")

for p in [
    r"P:\Tools\NimbusBackendPackages",
    r"P:\Tools\NimbusPythonPackages",
    r"P:\Tools\NimbusSparkVenv\Lib\site-packages",
    str(PROJECT_DIR),
]:
    if os.path.isdir(p) and p not in sys.path:
        sys.path.insert(0, p)

from minio import Minio
from PIL import Image as PILImage

BASE_URL = "http://127.0.0.1:8000"


def load_env():
    env_file = PROJECT_DIR / ".env"
    creds = {}
    for line in env_file.read_text(encoding="utf-8-sig").splitlines():
        if "=" in line and not line.strip().startswith("#"):
            k, v = line.strip().split("=", 1)
            creds[k.strip()] = v.strip().strip('"').strip("'")
    return creds


def get_minio_client():
    creds = load_env()
    return Minio(
        "127.0.0.1:9000",
        access_key=creds["MINIO_ROOT_USER"],
        secret_key=creds["MINIO_ROOT_PASSWORD"],
        secure=False,
    )


def api_get(path: str) -> tuple[int, dict]:
    url = f"{BASE_URL}{path}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as err:
        try:
            body = json.loads(err.read().decode("utf-8"))
        except Exception:
            body = {"detail": err.reason}
        return err.code, body


def api_post(path: str, payload: dict) -> tuple[int, dict]:
    url = f"{BASE_URL}{path}"
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as err:
        try:
            body = json.loads(err.read().decode("utf-8"))
        except Exception:
            body = {"detail": err.reason}
        return err.code, body


# Fixture generators
def generate_fixtures() -> dict[str, bytes]:
    # 1. CSV
    csv_bytes = b"id,name,role\n1,Alice,Engineer\n2,Bob,Architect\n"
    # 2. JSON
    json_bytes = json.dumps([{"item": "Server A", "load": 42}, {"item": "Server B", "load": 65}]).encode("utf-8")
    # 3. XML
    xml_bytes = b'<system><node id="1"><name>Compute-1</name></node><node id="2"><name>Storage-1</name></node></system>'
    # 4. TXT
    txt_bytes = b"Line 1: Status OK\nLine 2: Worker active\n"
    # 5. LOG
    log_bytes = b"2026-10-11 12:00:00 [INFO] System initialized\n2026-10-11 12:01:00 [WARN] Memory high\nRaw alert\n"
    # 6. PDF
    pdf_bytes = (
        b"%PDF-1.4\n"
        b"1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n"
        b"2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n"
        b"3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>\nendobj\n"
        b"4 0 obj\n<< /Length 38 >>\nstream\nBT\n/F1 12 Tf\n72 712 Td\n(Nimbus API Test) Tj\nET\nendstream\nendobj\n"
        b"5 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>\nendobj\n"
        b"xref\n0 6\n0000000000 65535 f \n0000000009 00000 n \n0000000058 00000 n \n0000000115 00000 n \n0000000227 00000 n \n0000000315 00000 n \n"
        b"trailer\n<< /Size 6 /Root 1 0 R >>\nstartxref\n388\n%%EOF"
    )
    # 7. PNG (using Pillow)
    img_png = PILImage.new("RGBA", (100, 50), color=(10, 20, 30, 255))
    buf_png = io.BytesIO()
    img_png.save(buf_png, format="PNG")
    png_bytes = buf_png.getvalue()
    # 8. JPG (using Pillow)
    img_jpg = PILImage.new("RGB", (80, 60), color=(200, 100, 50))
    buf_jpg = io.BytesIO()
    img_jpg.save(buf_jpg, format="JPEG")
    jpg_bytes = buf_jpg.getvalue()

    return {
        "test.csv": csv_bytes,
        "test.json": json_bytes,
        "test.xml": xml_bytes,
        "test.txt": txt_bytes,
        "test.log": log_bytes,
        "test.pdf": pdf_bytes,
        "test.png": png_bytes,
        "test.jpg": jpg_bytes,
    }


def test_preview_endpoint():
    print("=== Testing GET /api/objects/preview ===")
    client = get_minio_client()
    fixtures = generate_fixtures()
    test_prefix = f"test_api_preview_{uuid.uuid4().hex[:6]}/"

    uploaded_keys = []
    try:
        # Upload isolated fixtures to raw-zone
        for fname, data in fixtures.items():
            key = f"{test_prefix}{fname}"
            client.put_object("raw-zone", key, io.BytesIO(data), length=len(data))
            uploaded_keys.append(key)

        # 1. Test missing object_name query parameter -> 400
        code, body = api_get("/api/objects/preview?bucket=raw-zone")
        assert code == 400, f"Expected 400 for missing object_name, got {code}"
        assert "object_name" in body.get("detail", "").lower()
        print("  [PASS] Missing object_name parameter correctly rejected (400)")

        # 2. Test invalid bucket -> 400
        code, body = api_get(f"/api/objects/preview?bucket=invalid-bucket&object_name={uploaded_keys[0]}")
        assert code == 400, f"Expected 400 for invalid bucket, got {code}"
        print("  [PASS] Invalid bucket rejected (400)")

        # 3. Test non-existent object -> 404
        code, body = api_get(f"/api/objects/preview?bucket=raw-zone&object_name={test_prefix}does_not_exist.csv")
        assert code == 404, f"Expected 404 for missing object, got {code}"
        assert "not found" in body.get("detail", "").lower()
        print("  [PASS] Non-existent object returned 404 Not Found")

        # 4. Test unsupported format -> 400
        unsupported_key = f"{test_prefix}archive.zip"
        client.put_object("raw-zone", unsupported_key, io.BytesIO(b"PK\x03\x04unsupported"), length=15)
        uploaded_keys.append(unsupported_key)

        code, body = api_get(f"/api/objects/preview?bucket=raw-zone&object_name={unsupported_key}")
        assert code == 400, f"Expected 400 for unsupported format, got {code}"
        assert "unsupported format" in body.get("detail", "").lower()
        print("  [PASS] Unsupported format rejected (400)")

        # 5. Test all 8 supported formats
        expected_formats = {
            "test.csv": "CSV",
            "test.json": "JSON",
            "test.xml": "XML",
            "test.txt": "TXT",
            "test.log": "LOG",
            "test.pdf": "PDF",
            "test.png": "PNG",
            "test.jpg": "JPG",
        }

        for fname, expected_fmt in expected_formats.items():
            key = f"{test_prefix}{fname}"
            code, body = api_get(f"/api/objects/preview?bucket=raw-zone&object_name={key}")
            assert code == 200, f"Failed preview for {fname}, code {code}: {body}"
            assert body["format"] == expected_fmt, f"Expected {expected_fmt}, got {body['format']}"
            assert body["status"] in ("success", "partial")
            assert body["record_or_page_count"] >= 1
            assert "extracted_preview" in body
            assert "metadata" in body
            print(f"  [PASS] Successfully previewed {expected_fmt} (count: {body['record_or_page_count']})")

    finally:
        # Clean up test objects from raw-zone
        for key in uploaded_keys:
            try:
                client.remove_object("raw-zone", key)
            except Exception:
                pass


def test_processing_endpoint():
    print("\n=== Testing POST /api/processing/multiformat ===")
    client = get_minio_client()
    fixtures = generate_fixtures()
    test_prefix = f"test_api_proc_{uuid.uuid4().hex[:6]}/"

    uploaded_keys = []
    refined_prefixes_to_clean = []

    try:
        for fname, data in fixtures.items():
            key = f"{test_prefix}{fname}"
            client.put_object("raw-zone", key, io.BytesIO(data), length=len(data))
            uploaded_keys.append(key)

        # 1. Missing object_name in payload -> 400
        code, body = api_post("/api/processing/multiformat", {"bucket": "raw-zone"})
        assert code == 400, f"Expected 400 for missing object_name, got {code}"
        print("  [PASS] Missing object_name payload rejected (400)")

        # 2. Invalid bucket (not raw-zone) -> 400
        code, body = api_post("/api/processing/multiformat", {"object_name": uploaded_keys[0], "bucket": "refined-zone"})
        assert code == 400, f"Expected 400 for non-raw-zone bucket, got {code}"
        assert "raw-zone" in body.get("detail", "").lower()
        print("  [PASS] Non-raw-zone bucket rejected (400)")

        # 3. Missing object in raw-zone -> 404
        code, body = api_post("/api/processing/multiformat", {"object_name": f"{test_prefix}missing.json"})
        assert code == 404, f"Expected 404 for missing object, got {code}"
        print("  [PASS] Missing source object returned 404")

        # 4. Unsupported format -> 400
        unsupported_key = f"{test_prefix}data.tar"
        client.put_object("raw-zone", unsupported_key, io.BytesIO(b"tar-archive-bytes"), length=17)
        uploaded_keys.append(unsupported_key)

        code, body = api_post("/api/processing/multiformat", {"object_name": unsupported_key})
        assert code == 400, f"Expected 400 for unsupported format, got {code}"
        print("  [PASS] Unsupported format rejected during processing (400)")

        # 5. Process JSON object
        json_key = f"{test_prefix}test.json"
        code, body = api_post("/api/processing/multiformat", {"object_name": json_key})
        assert code == 200, f"Processing JSON failed: {body}"
        assert body["status"] == "success"
        assert body["format"] == "JSON"
        assert body["record_or_page_count"] == 2
        assert "run_id" in body
        assert "records_object" in body
        assert "manifest_object" in body
        refined_prefixes_to_clean.append(body["output_prefix"])

        # Verify that refined-zone actually contains the records and manifest
        rec_resp = client.get_object("refined-zone", body["records_object"])
        rec_content = json.loads(rec_resp.read().decode("utf-8"))
        rec_resp.close()
        rec_resp.release_conn()

        assert rec_content["source_name"] == json_key
        assert rec_content["record_or_page_count"] == 2
        assert len(rec_content["extracted_records"]) == 2
        print("  [PASS] Processed JSON: verified extracted_records.json in refined-zone")

        man_resp = client.get_object("refined-zone", body["manifest_object"])
        man_content = json.loads(man_resp.read().decode("utf-8"))
        man_resp.close()
        man_resp.release_conn()

        assert man_content["run_id"] == body["run_id"]
        assert man_content["status"] == "success"
        assert man_content["format"] == "JSON"
        assert man_content["output_location"]["records_object"] == body["records_object"]
        print("  [PASS] Processed JSON: verified manifest.json in refined-zone")

        # 6. Verify source object in raw-zone was NOT modified or deleted
        src_stat = client.stat_object("raw-zone", json_key)
        assert src_stat.size == len(fixtures["test.json"]), "Source object was unexpectedly modified!"
        print("  [PASS] Verified raw-zone source object preserved intact")

        # 7. Process LOG file (with timestamp and severity extraction)
        log_key = f"{test_prefix}test.log"
        code, body = api_post("/api/processing/multiformat", {"object_name": log_key})
        assert code == 200, f"Processing LOG failed: {body}"
        assert body["format"] == "LOG"
        assert body["status"] == "success"
        refined_prefixes_to_clean.append(body["output_prefix"])

        rec_resp = client.get_object("refined-zone", body["records_object"])
        rec_content = json.loads(rec_resp.read().decode("utf-8"))
        rec_resp.close()
        rec_resp.release_conn()
        assert rec_content["record_or_page_count"] == 3
        assert rec_content["metadata"]["matched_lines"] == 2
        assert rec_content["metadata"]["unmatched_lines"] == 1
        print("  [PASS] Processed LOG: structured records with timestamps and severity stored")

        # 8. Failure case / Corrupted data handling
        corrupt_key = f"{test_prefix}corrupt.xml"
        client.put_object("raw-zone", corrupt_key, io.BytesIO(b"<unclosed>xml content"), length=21)
        uploaded_keys.append(corrupt_key)

        code, body = api_post("/api/processing/multiformat", {"object_name": corrupt_key})
        assert code == 200, f"Processing corrupted XML should return structured result, got {code}: {body}"
        assert body["status"] == "error"
        assert body["error"] is not None
        refined_prefixes_to_clean.append(body["output_prefix"])

        # Verify manifest stored even for error status
        man_resp = client.get_object("refined-zone", body["manifest_object"])
        man_content = json.loads(man_resp.read().decode("utf-8"))
        man_resp.close()
        man_resp.release_conn()
        assert man_content["status"] == "error"
        assert "error" in man_content and man_content["error"] is not None
        print("  [PASS] Corrupted XML safely handled and recorded in manifest.json with status=error")

    finally:
        # Clean up raw-zone test keys
        for key in uploaded_keys:
            try:
                client.remove_object("raw-zone", key)
            except Exception:
                pass

        # Clean up refined-zone test prefixes
        for pfx in refined_prefixes_to_clean:
            try:
                for obj in client.list_objects("refined-zone", prefix=pfx, recursive=True):
                    client.remove_object("refined-zone", obj.object_name)
            except Exception:
                pass


def test_temp_cleanliness():
    print("\n=== Checking Temp Directory Cleanliness ===")
    if TEMP_DIR.exists():
        leftovers = [
            f for f in os.listdir(TEMP_DIR)
            if "multiformat" in f or "unit_test" in f or "test_api" in f
        ]
        assert len(leftovers) == 0, f"Found leftover temp files: {leftovers}"
    print("  [PASS] P:\\Tools\\NimbusSparkTemp is clean (0 leftover files)")


def run_all_api_tests():
    print("=" * 70)
    print("NIMBUS MULTI-FORMAT STEP 3 - API INTEGRATION TEST SUITE")
    print(f"API Target: {BASE_URL}")
    print("=" * 70)

    test_preview_endpoint()
    test_processing_endpoint()
    test_temp_cleanliness()

    print("\n" + "=" * 70)
    print("ALL MULTI-FORMAT API INTEGRATION TESTS PASSED SUCCESSFULLY!")
    print("=" * 70)


if __name__ == "__main__":
    run_all_api_tests()
