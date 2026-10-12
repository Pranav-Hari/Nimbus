"""
Nimbus Multi-Format Extraction Unit Tests
Location: test-data/phase4/test_multiformat_service.py

Validates format-specific extraction, previewing, and metadata parsing
across all 8 supported formats: CSV, JSON, XML, TXT, LOG, PDF, JPG, PNG.
Tests local fixtures only. Does NOT upload to MinIO or modify production data.
Guarantees cleanup under P:\\Tools\\NimbusSparkTemp.
"""

import io
import os
import shutil
import struct
import sys
import uuid
from pathlib import Path

PROJECT_DIR = Path(r"P:\Projects\VIT\CCM Project\Nimbus_v1.0.0")
TEMP_DIR = Path(r"P:\Tools\NimbusSparkTemp")

if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

import backend.multiformat_service as ms
from backend.multiformat_service import (
    process_multi_format_content,
    extract_csv,
    extract_json,
    extract_xml,
    extract_txt,
    extract_log,
    extract_pdf,
    extract_image,
    create_temp_workspace,
    get_dependency_status,
    SUPPORTED_FORMATS,
)

# External libraries for fixture generation and assertion
from PIL import Image as PILImage
import pypdf


def assert_standard_schema(result: dict, expected_format: str):
    """Asserts that the extraction result conforms strictly to the normalized schema."""
    assert isinstance(result, dict), "Result must be a dictionary"
    for key in [
        "source_name",
        "format",
        "status",
        "record_or_page_count",
        "extracted_preview",
        "metadata",
        "error",
        "dependency_status",
    ]:
        assert key in result, f"Missing key in result schema: {key}"
    assert result["format"] == expected_format, f"Expected format {expected_format}, got {result['format']}"
    assert result["status"] in ("success", "partial", "error"), f"Unexpected status: {result['status']}"
    assert isinstance(result["metadata"], dict), "Metadata must be a dictionary"
    assert isinstance(result["record_or_page_count"], int), "record_or_page_count must be an integer"
    assert isinstance(result["dependency_status"], dict), "dependency_status must be a dictionary"


def test_format_csv():
    print("-> Testing CSV extraction...")
    csv_sample_path = PROJECT_DIR / "samples" / "input" / "sales.csv"
    assert csv_sample_path.exists(), f"Sample CSV not found at {csv_sample_path}"

    result = process_multi_format_content(csv_sample_path)
    assert_standard_schema(result, "CSV")
    assert result["status"] == "success"
    assert result["record_or_page_count"] == 8, f"Expected 8 rows, got {result['record_or_page_count']}"
    assert len(result["extracted_preview"]) == 8
    assert result["metadata"]["column_count"] == 4
    assert result["metadata"]["columns"] == ["customer_id", "city", "orders", "amount"]
    assert result["metadata"]["delimiter"] == ","
    assert result["extracted_preview"][0]["city"] == "Vellore"
    assert result["extracted_preview"][0]["amount"] == "1250.50"
    print("   [PASS] CSV extraction passed.")


def test_format_json():
    print("-> Testing JSON extraction (sample object & synthetic array)...")
    json_sample_path = PROJECT_DIR / "samples" / "input" / "metadata.json"
    assert json_sample_path.exists(), f"Sample JSON not found at {json_sample_path}"

    # 1. Existing sample JSON (object with potential BOM)
    res_obj = process_multi_format_content(json_sample_path)
    assert_standard_schema(res_obj, "JSON")
    assert res_obj["status"] == "success"
    assert res_obj["metadata"]["root_type"] == "object"
    assert res_obj["metadata"]["key_count"] == 4
    assert "dataset" in res_obj["metadata"]["top_level_keys"]
    assert res_obj["extracted_preview"]["dataset"] == "sales_sample"

    # 2. JSON array
    json_array_bytes = b'[{"id": 1, "name": "Item A"}, {"id": 2, "name": "Item B"}]'
    res_arr = process_multi_format_content(json_array_bytes, filename="items.json")
    assert_standard_schema(res_arr, "JSON")
    assert res_arr["status"] == "success"
    assert res_arr["record_or_page_count"] == 2
    assert res_arr["metadata"]["root_type"] == "array"
    assert res_arr["metadata"]["item_count"] == 2
    assert res_arr["metadata"]["sample_keys"] == ["id", "name"]
    assert len(res_arr["extracted_preview"]) == 2
    print("   [PASS] JSON extraction passed.")


def test_format_xml():
    print("-> Testing XML extraction...")
    xml_data = (
        b'<?xml version="1.0" encoding="UTF-8"?>\n'
        b'<catalog dataset="sales_catalog" version="1.0">\n'
        b'    <item id="101">\n'
        b'        <name>Industrial Widget</name>\n'
        b'        <price>125.50</price>\n'
        b'    </item>\n'
        b'    <item id="102">\n'
        b'        <name>Precision Bearing</name>\n'
        b'        <price>45.00</price>\n'
        b'    </item>\n'
        b'    <item id="103">\n'
        b'        <name>Hydraulic Seal</name>\n'
        b'        <price>18.25</price>\n'
        b'    </item>\n'
        b'</catalog>\n'
    )

    result = process_multi_format_content(xml_data, filename="catalog.xml")
    assert_standard_schema(result, "XML")
    assert result["status"] == "success"
    assert result["record_or_page_count"] == 3
    assert result["metadata"]["root_tag"] == "catalog"
    assert result["metadata"]["root_attributes"]["dataset"] == "sales_catalog"
    assert result["metadata"]["direct_child_count"] == 3
    assert result["metadata"]["distinct_child_tags"] == ["item"]
    assert len(result["extracted_preview"]) == 3
    assert result["extracted_preview"][0]["attributes"]["id"] == "101"
    assert result["extracted_preview"][0]["fields"]["name"] == "Industrial Widget"
    assert result["extracted_preview"][0]["fields"]["price"] == "125.50"
    print("   [PASS] XML extraction passed.")


def test_format_txt():
    print("-> Testing TXT extraction...")
    txt_data = (
        b"Nimbus Cloud System\n"
        b"Architecture: Hybrid Lakehouse\n"
        b"Storage Engine: MinIO S3\n"
        b"\n"
        b"Execution Engine: Apache Spark & DuckDB\n"
    )

    result = process_multi_format_content(txt_data, filename="system_info.txt")
    assert_standard_schema(result, "TXT")
    assert result["status"] == "success"
    assert result["record_or_page_count"] == 5
    assert result["metadata"]["line_count"] == 5
    assert result["metadata"]["non_empty_line_count"] == 4
    assert result["metadata"]["word_count"] > 10
    assert result["metadata"]["encoding"] == "utf-8"
    assert result["extracted_preview"][0]["line_number"] == 1
    assert result["extracted_preview"][0]["text"] == "Nimbus Cloud System"
    print("   [PASS] TXT extraction passed.")


def test_format_log():
    print("-> Testing LOG extraction (unmatched lines + structured patterns)...")
    # 1. Existing sample log (unstructured lines)
    log_sample_path = PROJECT_DIR / "samples" / "input" / "sample.log"
    assert log_sample_path.exists(), f"Sample LOG not found at {log_sample_path}"

    res_raw = process_multi_format_content(log_sample_path)
    assert_standard_schema(res_raw, "LOG")
    assert res_raw["status"] == "success"
    assert res_raw["metadata"]["total_lines"] == 4
    assert res_raw["metadata"]["unmatched_lines"] == 4
    assert res_raw["metadata"]["matched_lines"] == 0
    assert res_raw["extracted_preview"][0]["matched"] is False
    assert res_raw["extracted_preview"][0]["message"] == "Nimbus sample log"

    # 2. Structured log lines with various timestamps and severities
    structured_log = (
        b"2026-10-11 02:40:00 [INFO] MinIO bucket health check OK\n"
        b"[2026-10-11T02:41:15.120Z] WARN: High disk latency detected\n"
        b"2026-10-11 02:42:00 ERROR Failed to acquire lock on partition\n"
        b"[DEBUG] 2026-10-11 02:43:00 Buffer flushed to disk\n"
        b"Arbitrary unformatted diagnostic notice\n"
    )

    res_struct = process_multi_format_content(structured_log, filename="server.log")
    assert_standard_schema(res_struct, "LOG")
    assert res_struct["status"] == "success"
    assert res_struct["metadata"]["total_lines"] == 5
    assert res_struct["metadata"]["matched_lines"] == 4
    assert res_struct["metadata"]["unmatched_lines"] == 1
    assert res_struct["metadata"]["severity_counts"]["INFO"] == 1
    assert res_struct["metadata"]["severity_counts"]["WARN"] == 1
    assert res_struct["metadata"]["severity_counts"]["ERROR"] == 1
    assert res_struct["metadata"]["severity_counts"]["DEBUG"] == 1
    # Check parsed records
    assert res_struct["extracted_preview"][0]["matched"] is True
    assert res_struct["extracted_preview"][0]["severity"] == "INFO"
    assert res_struct["extracted_preview"][0]["timestamp"] == "2026-10-11 02:40:00"
    assert res_struct["extracted_preview"][0]["message"] == "MinIO bucket health check OK"

    assert res_struct["extracted_preview"][1]["matched"] is True
    assert res_struct["extracted_preview"][1]["severity"] == "WARN"
    assert res_struct["extracted_preview"][1]["timestamp"] == "2026-10-11T02:41:15.120Z"

    assert res_struct["extracted_preview"][4]["matched"] is False
    assert res_struct["extracted_preview"][4]["message"] == "Arbitrary unformatted diagnostic notice"
    print("   [PASS] LOG extraction passed.")


def test_format_pdf():
    print("-> Testing PDF extraction with actual page text and document metadata (pypdf)...")
    # Build valid PDF with stream content and metadata
    pdf_content = (
        b"%PDF-1.4\n"
        b"1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n"
        b"2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n"
        b"3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>\nendobj\n"
        b"4 0 obj\n<< /Length 55 >>\nstream\nBT\n/F1 12 Tf\n72 712 Td\n(Nimbus Analytics System Validation) Tj\nET\nendstream\nendobj\n"
        b"5 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>\nendobj\n"
        b"6 0 obj\n<< /Title (Nimbus Architecture Guide) /Author (VIT CCM Team) >>\nendobj\n"
        b"xref\n0 7\n0000000000 65535 f \n0000000009 00000 n \n0000000058 00000 n \n0000000115 00000 n \n0000000227 00000 n \n0000000332 00000 n \n0000000405 00000 n \n"
        b"trailer\n<< /Size 7 /Root 1 0 R /Info 6 0 R >>\nstartxref\n485\n%%EOF"
    )

    result = process_multi_format_content(pdf_content, filename="nimbus_guide.pdf")
    assert_standard_schema(result, "PDF")
    assert result["status"] == "success"
    assert result["record_or_page_count"] == 1
    assert result["metadata"]["pypdf_used"] is True
    assert result["metadata"]["pdf_version"] == "1.4"
    assert result["metadata"]["document_info"]["Title"] == "Nimbus Architecture Guide"
    assert result["metadata"]["document_info"]["Author"] == "VIT CCM Team"
    # Verify page text preview extraction
    assert len(result["extracted_preview"]) == 1
    assert "Nimbus Analytics System Validation" in result["extracted_preview"][0]["text_preview"]
    print("   [PASS] PDF page-text and metadata extraction passed.")


def test_format_png():
    print("-> Testing PNG extraction and image decoding (Pillow)...")
    # Create an RGBA image fixture in memory
    img = PILImage.new("RGBA", (640, 480), color=(255, 128, 0, 200))
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    png_bytes = buf.getvalue()

    result = process_multi_format_content(png_bytes, filename="ui_diagram.png")
    assert_standard_schema(result, "PNG")
    assert result["status"] == "success"
    assert result["record_or_page_count"] == 1
    assert result["metadata"]["pillow_used"] is True
    assert result["metadata"]["width"] == 640
    assert result["metadata"]["height"] == 480
    assert result["metadata"]["mode"] == "RGBA"
    assert result["metadata"]["format"] == "PNG"
    assert result["extracted_preview"][0]["aspect_ratio"] == 1.333
    print("   [PASS] PNG image decoding and dimension validation passed.")


def test_format_jpg():
    print("-> Testing JPG extraction, image decoding, and EXIF metadata (Pillow)...")
    # Create an RGB image fixture with EXIF metadata
    img = PILImage.new("RGB", (320, 240), color=(50, 100, 150))
    exif = img.getexif()
    exif[0x010E] = "Nimbus Step 2 Test Image"  # ImageDescription
    exif[0x010F] = "Nimbus Sensor Core"        # Make
    exif[0x0110] = "NimbusCam v1.0"            # Model

    buf = io.BytesIO()
    img.save(buf, format="JPEG", exif=exif)
    jpg_bytes = buf.getvalue()

    result = process_multi_format_content(jpg_bytes, filename="inspection.jpg")
    assert_standard_schema(result, "JPG")
    assert result["status"] == "success"
    assert result["record_or_page_count"] == 1
    assert result["metadata"]["pillow_used"] is True
    assert result["metadata"]["width"] == 320
    assert result["metadata"]["height"] == 240
    assert result["metadata"]["mode"] == "RGB"
    assert result["metadata"]["format"] == "JPEG"
    assert result["extracted_preview"][0]["aspect_ratio"] == 1.333

    # Verify extracted EXIF tags
    exif_data = result["metadata"]["exif"]
    assert exif_data.get("Make") == "Nimbus Sensor Core", f"Expected 'Nimbus Sensor Core', got {exif_data.get('Make')}"
    assert exif_data.get("Model") == "NimbusCam v1.0", f"Expected 'NimbusCam v1.0', got {exif_data.get('Model')}"
    assert exif_data.get("ImageDescription") == "Nimbus Step 2 Test Image"
    print("   [PASS] JPG image decoding and EXIF metadata validation passed.")


def test_fallback_when_dependencies_missing():
    print("-> Testing fallback behavior and clear messages when dependencies are absent...")
    # 1. Simulate pypdf missing
    orig_pypdf_flag = ms.PYPDF_AVAILABLE
    ms.PYPDF_AVAILABLE = False
    try:
        minimal_pdf = b"%PDF-1.4\n1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n3 0 obj\n<< /Type /Page /Parent 2 0 R >>\nendobj\nxref\n0 4\n0000000000 65535 f \ntrailer\n<< /Size 4 /Root 1 0 R >>\nstartxref\n100\n%%EOF"
        pdf_res = process_multi_format_content(minimal_pdf, filename="fallback.pdf")
        assert pdf_res["status"] == "partial"
        assert pdf_res["metadata"]["pypdf_used"] is False
        assert pdf_res["metadata"]["dependency_required"] == "pypdf"
        assert "pypdf package is not installed" in pdf_res["error"]
        assert pdf_res["metadata"]["pdf_version"] == "1.4"
        assert pdf_res["record_or_page_count"] == 1
    finally:
        ms.PYPDF_AVAILABLE = orig_pypdf_flag

    # 2. Simulate Pillow missing for PNG
    orig_pillow_flag = ms.PILLOW_AVAILABLE
    ms.PILLOW_AVAILABLE = False
    try:
        png_bytes = (
            b"\x89PNG\r\n\x1a\n"
            b"\x00\x00\x00\rIHDR"
            + struct.pack(">IIBBBBB", 800, 600, 8, 2, 0, 0, 0)
            + b"\x00\x00\x00\x00"
        )
        png_res = process_multi_format_content(png_bytes, filename="fallback.png")
        assert png_res["status"] == "partial"
        assert png_res["metadata"]["pillow_used"] is False
        assert png_res["metadata"]["dependency_required"] == "Pillow"
        assert "Pillow package is not installed" in png_res["error"]
        assert png_res["metadata"]["width"] == 800
        assert png_res["metadata"]["height"] == 600
        assert png_res["metadata"]["color_mode"] == "RGB"
    finally:
        ms.PILLOW_AVAILABLE = orig_pillow_flag

    print("   [PASS] Missing dependency fallback modes verified.")


def test_temp_cleanup_guarantee():
    print("-> Testing temporary workspace cleanup guarantee under P:\\Tools\\NimbusSparkTemp...")
    workspace, cleanup = create_temp_workspace(prefix="unit_test_cleanup_")
    assert workspace.exists(), f"Temp workspace {workspace} was not created"
    assert str(TEMP_DIR).lower() in str(workspace).lower(), f"Workspace {workspace} must be under {TEMP_DIR}"

    # Create dummy files
    dummy_file = workspace / "test_scratch.bin"
    dummy_file.write_bytes(b"temporary content to be deleted")
    assert dummy_file.exists()

    # Run cleanup
    cleanup()
    assert not workspace.exists(), f"Temp workspace {workspace} was not cleaned up!"
    print("   [PASS] Temporary workspace cleanup verified.")


def test_error_handling_and_unsupported_formats():
    print("-> Testing error trapping and unsupported format handling...")
    # 1. Unsupported format
    unsupported = process_multi_format_content(b"some binary data", filename="archive.zip")
    assert unsupported["status"] == "error"
    assert "Unsupported format" in unsupported["error"]

    # 2. Malformed JSON
    bad_json = process_multi_format_content(b"{key_without_quotes: 123", filename="broken.json")
    assert bad_json["status"] == "error"
    assert "Invalid JSON" in bad_json["error"] or "Extraction failure" in bad_json["error"]

    # 3. Malformed XML
    bad_xml = process_multi_format_content(b"<open_tag>without closing", filename="broken.xml")
    assert bad_xml["status"] == "error"
    assert "XML parse error" in bad_xml["error"] or "Extraction failure" in bad_xml["error"]

    # 4. Truncated / malformed PDF
    bad_pdf = process_multi_format_content(b"%PDF-truncated-binary-garbage-here", filename="corrupted.pdf")
    assert bad_pdf["status"] == "error"
    assert "failed to extract" in bad_pdf["error"].lower() or "pdf" in bad_pdf["error"].lower()

    # 5. Truncated / malformed PNG
    bad_png = process_multi_format_content(b"\x89PNG\r\n\x1a\ncorrupted", filename="corrupted.png")
    assert bad_png["status"] == "error"
    assert "failed" in bad_png["error"].lower() or "image" in bad_png["error"].lower()

    print("   [PASS] Error trapping passed.")


def run_all_tests():
    print("=" * 70)
    print("NIMBUS MULTI-FORMAT EXTRACTION SERVICE - STEP 2 UNIT TEST SUITE")
    print(f"Project Root: {PROJECT_DIR}")
    print(f"Temp Root:    {TEMP_DIR}")
    print(f"Dependency Status: {get_dependency_status()}")
    print("=" * 70)

    test_format_csv()
    test_format_json()
    test_format_xml()
    test_format_txt()
    test_format_log()
    test_format_pdf()
    test_format_png()
    test_format_jpg()
    test_fallback_when_dependencies_missing()
    test_temp_cleanup_guarantee()
    test_error_handling_and_unsupported_formats()

    print("=" * 70)
    print("ALL MULTI-FORMAT STEP 2 UNIT TESTS COMPLETED SUCCESSFULLY!")
    print("=" * 70)


if __name__ == "__main__":
    run_all_tests()
