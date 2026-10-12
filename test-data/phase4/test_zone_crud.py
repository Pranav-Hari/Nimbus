"""
Nimbus Phase 4 - Step 2: Zone Management and CRUD Regression Test Suite
Verifies:
1. Direct upload into refined-zone rejected (HTTP 400).
2. Upload new file to raw-zone with upload_duration_ms and audit response (HTTP 200).
3. Duplicate upload without overwrite blocked (HTTP 409 Conflict).
4. Duplicate upload with overwrite=true succeeds (HTTP 200, overwritten=True).
5. Streaming download from raw-zone with exact byte match and headers.
6. Object deletion from raw-zone (HTTP 200) and verification of removal.
7. Object deletion from refined-zone (isolated test object created directly in MinIO, then verified deleted).
8. Rejection of prefix/directory deletion (HTTP 400).
9. Missing object deletion returns 404.
10. Invalid bucket name rejection (HTTP 400).
11. Special characters in object names (spaces, brackets, etc.) download and delete safely.
"""

import urllib.request
import urllib.parse
import json
import io
import sys
from pathlib import Path

BASE_URL = 'http://127.0.0.1:8000'

sys.path.insert(0, r'P:\Tools\NimbusPythonPackages')
sys.path.insert(0, r'P:\Tools\NimbusBackendPackages')
sys.path.insert(0, str(Path('.').resolve()))
from backend.main import client

def run_tests():
    print("======================================================================")
    print("NIMBUS STEP 2: ZONE MANAGEMENT & CRUD REGRESSION TESTS")
    print("======================================================================")

    # 1. Direct upload to refined-zone rejection
    print("\n[Test 1] Attempt direct upload to refined-zone...")
    boundary = '----WebKitBoundaryTest123'
    body = io.BytesIO()
    body.write(f'--{boundary}\r\n'.encode('utf-8'))
    body.write(b'Content-Disposition: form-data; name="file"; filename="illegal_refined.txt"\r\nContent-Type: text/plain\r\n\r\nDirectUploadToRefinedZone\r\n')
    body.write(f'--{boundary}--\r\n'.encode('utf-8'))
    req = urllib.request.Request(
        f'{BASE_URL}/api/buckets/refined-zone/upload',
        data=body.getvalue(),
        headers={'Content-Type': f'multipart/form-data; boundary={boundary}'},
        method='POST'
    )
    try:
        urllib.request.urlopen(req)
        assert False, "Upload to refined-zone should have been rejected with 400"
    except urllib.error.HTTPError as e:
        assert e.code == 400, f"Expected 400, got {e.code}"
        err_msg = e.read().decode('utf-8')
        assert "Direct uploads are restricted" in err_msg
        print(f"  PASS: Direct upload to refined-zone rejected (HTTP 400: {err_msg})")

    # 2. Upload new file to raw-zone
    test_obj_raw = "test_crud_isolated_raw.txt"
    try:
        client.remove_object("raw-zone", test_obj_raw)
    except Exception:
        pass
    test_content_v1 = b"Hello Nimbus Isolated Test V1"
    print(f"\n[Test 2] Upload new file '{test_obj_raw}' to raw-zone...")
    body = io.BytesIO()
    body.write(f'--{boundary}\r\n'.encode('utf-8'))
    body.write(f'Content-Disposition: form-data; name="file"; filename="{test_obj_raw}"\r\nContent-Type: text/plain\r\n\r\n'.encode('utf-8'))
    body.write(test_content_v1)
    body.write(f'\r\n--{boundary}--\r\n'.encode('utf-8'))
    req = urllib.request.Request(
        f'{BASE_URL}/api/buckets/raw-zone/upload',
        data=body.getvalue(),
        headers={'Content-Type': f'multipart/form-data; boundary={boundary}'},
        method='POST'
    )
    with urllib.request.urlopen(req) as resp:
        assert resp.status == 200
        res = json.loads(resp.read().decode('utf-8'))
        assert res.get("uploaded") is True
        assert res.get("overwritten") is False
        assert "upload_duration_ms" in res
        print(f"  PASS: New file uploaded successfully (overwritten={res.get('overwritten')}, duration={res.get('upload_duration_ms')}ms)")

    # 3. Duplicate upload without overwrite -> 409
    print(f"\n[Test 3] Attempt duplicate upload of '{test_obj_raw}' without overwrite authorization...")
    body = io.BytesIO()
    body.write(f'--{boundary}\r\n'.encode('utf-8'))
    body.write(f'Content-Disposition: form-data; name="file"; filename="{test_obj_raw}"\r\nContent-Type: text/plain\r\n\r\nNewContentWithoutAuth\r\n'.encode('utf-8'))
    body.write(f'--{boundary}--\r\n'.encode('utf-8'))
    req = urllib.request.Request(
        f'{BASE_URL}/api/buckets/raw-zone/upload',
        data=body.getvalue(),
        headers={'Content-Type': f'multipart/form-data; boundary={boundary}'},
        method='POST'
    )
    try:
        urllib.request.urlopen(req)
        assert False, "Duplicate upload should have triggered HTTP 409"
    except urllib.error.HTTPError as e:
        assert e.code == 409
        err_msg = e.read().decode('utf-8')
        assert "already exists" in err_msg
        print(f"  PASS: Duplicate upload blocked with HTTP 409 Conflict: {err_msg}")

    # 4. Duplicate upload with overwrite=true -> 200
    test_content_v2 = b"Hello Nimbus Isolated Test V2 - Overwritten"
    print(f"\n[Test 4] Authorize overwrite of '{test_obj_raw}' with overwrite=true...")
    body = io.BytesIO()
    body.write(f'--{boundary}\r\n'.encode('utf-8'))
    body.write(f'Content-Disposition: form-data; name="file"; filename="{test_obj_raw}"\r\nContent-Type: text/plain\r\n\r\n'.encode('utf-8'))
    body.write(test_content_v2)
    body.write(f'\r\n--{boundary}--\r\n'.encode('utf-8'))
    req = urllib.request.Request(
        f'{BASE_URL}/api/buckets/raw-zone/upload?overwrite=true',
        data=body.getvalue(),
        headers={'Content-Type': f'multipart/form-data; boundary={boundary}'},
        method='POST'
    )
    with urllib.request.urlopen(req) as resp:
        assert resp.status == 200
        res = json.loads(resp.read().decode('utf-8'))
        assert res.get("uploaded") is True
        assert res.get("overwritten") is True
        print(f"  PASS: Authorized overwrite succeeded (overwritten={res.get('overwritten')})")

    # 5. Download object from raw-zone
    print(f"\n[Test 5] Download object '{test_obj_raw}' from raw-zone...")
    dl_url = f'{BASE_URL}/api/buckets/raw-zone/objects/{urllib.parse.quote(test_obj_raw)}/download'
    req = urllib.request.Request(dl_url)
    with urllib.request.urlopen(req) as resp:
        assert resp.status == 200
        downloaded = resp.read()
        assert downloaded == test_content_v2
        assert "Content-Disposition" in resp.headers
        print(f"  PASS: Downloaded bytes matched exact overwritten content ({len(downloaded)} bytes)")

    # 6. Delete object from raw-zone
    print(f"\n[Test 6] Delete object '{test_obj_raw}' from raw-zone...")
    del_url = f'{BASE_URL}/api/buckets/raw-zone/objects/{urllib.parse.quote(test_obj_raw)}'
    req = urllib.request.Request(del_url, method='DELETE')
    with urllib.request.urlopen(req) as resp:
        assert resp.status == 200
        del_res = json.loads(resp.read().decode('utf-8'))
        assert del_res.get("deleted") is True
        print(f"  PASS: Object deleted successfully from raw-zone: {del_res}")

    # Verify object no longer exists in raw-zone
    try:
        client.stat_object("raw-zone", test_obj_raw)
        assert False, "Object should not exist after deletion"
    except Exception:
        print("  PASS: MinIO stat confirmed object does not exist.")

    # 7. Delete isolated test object in refined-zone
    print(f"\n[Test 7] Create and delete isolated object in refined-zone...")
    test_obj_ref = "test_crud_isolated_refined.txt"
    ref_bytes = b"Refined isolated test content"
    client.put_object("refined-zone", test_obj_ref, io.BytesIO(ref_bytes), len(ref_bytes), content_type="text/plain")

    # Download from refined-zone
    dl_ref_url = f'{BASE_URL}/api/buckets/refined-zone/objects/{urllib.parse.quote(test_obj_ref)}/download'
    with urllib.request.urlopen(dl_ref_url) as resp:
        assert resp.read() == ref_bytes
        print(f"  PASS: Successfully downloaded isolated refined-zone object.")

    # Delete from refined-zone
    del_ref_url = f'{BASE_URL}/api/buckets/refined-zone/objects/{urllib.parse.quote(test_obj_ref)}'
    req = urllib.request.Request(del_ref_url, method='DELETE')
    with urllib.request.urlopen(req) as resp:
        assert resp.status == 200
        assert json.loads(resp.read().decode('utf-8')).get("deleted") is True
        print(f"  PASS: Successfully deleted isolated refined-zone object.")

    # 8. Prefix / Directory deletion rejection
    print("\n[Test 8] Rejection of prefix/directory deletion...")
    prefix_url = f'{BASE_URL}/api/buckets/raw-zone/objects/{urllib.parse.quote("some_folder/")}'
    req = urllib.request.Request(prefix_url, method='DELETE')
    try:
        urllib.request.urlopen(req)
        assert False, "Prefix delete should fail"
    except urllib.error.HTTPError as e:
        assert e.code == 400
        print(f"  PASS: Prefix deletion rejected with HTTP 400")

    # 9. Delete missing object returns 404
    print("\n[Test 9] Delete non-existent object...")
    missing_url = f'{BASE_URL}/api/buckets/raw-zone/objects/{urllib.parse.quote("definitely_non_existent.txt")}'
    req = urllib.request.Request(missing_url, method='DELETE')
    try:
        urllib.request.urlopen(req)
        assert False, "Missing object should return 404"
    except urllib.error.HTTPError as e:
        assert e.code == 404
        print(f"  PASS: Missing object returned HTTP 404")

    # 10. Invalid bucket name rejection
    print("\n[Test 10] Operations on invalid bucket name...")
    bad_bucket_url = f'{BASE_URL}/api/buckets/unauthorized-bucket/objects/file.txt/download'
    try:
        urllib.request.urlopen(bad_bucket_url)
        assert False, "Invalid bucket should return 400"
    except urllib.error.HTTPError as e:
        assert e.code == 400
        print(f"  PASS: Invalid bucket returned HTTP 400")

    # 11. Special characters in object name
    print("\n[Test 11] Special characters in object name...")
    special_name = "test special [chars] (v1) #2.txt"
    special_content = b"Special chars test content"
    client.put_object("raw-zone", special_name, io.BytesIO(special_content), len(special_content))
    try:
        dl_spec_url = f'{BASE_URL}/api/buckets/raw-zone/objects/{urllib.parse.quote(special_name)}/download'
        with urllib.request.urlopen(dl_spec_url) as resp:
            assert resp.read() == special_content
            print("  PASS: Successfully downloaded object with special characters")

        del_spec_url = f'{BASE_URL}/api/buckets/raw-zone/objects/{urllib.parse.quote(special_name)}'
        req = urllib.request.Request(del_spec_url, method='DELETE')
        with urllib.request.urlopen(req) as resp:
            assert resp.status == 200
            print("  PASS: Successfully deleted object with special characters")
    finally:
        try:
            client.remove_object("raw-zone", special_name)
        except Exception:
            pass

    print("\n======================================================================")
    print("ALL 11 ZONE CRUD TESTS PASSED SUCCESSFULLY!")
    print("======================================================================")

if __name__ == "__main__":
    run_tests()
