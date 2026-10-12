"""
Nimbus Multi-Format Extraction Service
Module: backend/multiformat_service.py

Handles format-specific extraction, metadata parsing, and record previewing
for 8 supported formats: CSV, JSON, XML, TXT, LOG, PDF, JPG, and PNG.

Design Principles:
- Uses standard library built-ins for JSON, XML, TXT, LOG, and CSV.
- Supports optional pypdf (for PDF text/metadata) and Pillow (for Image metadata).
- If pypdf or Pillow are not installed, provides pure-Python fallback extraction
  for headers, dimensions, and page counts, returning status="partial" with clear
  dependency notes, without failing or crashing.
- Guarantees temporary file cleanup strictly under P:\\Tools\\NimbusSparkTemp.
- Does not modify existing PySpark sales pipeline or production buckets.
"""

import csv
import io
import json
import os
import sys
import re
import shutil
import struct
import tempfile
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
import xml.etree.ElementTree as ET

# Supported formats
SUPPORTED_FORMATS = {"CSV", "JSON", "XML", "TXT", "LOG", "PDF", "JPG", "PNG", "PARQUET"}

# Configurable temp base directory
DEFAULT_TEMP_DIR = Path(r"P:\Tools\NimbusSparkTemp")

# Dependency detection
SPARK_VENV_SITE_PACKAGES = r"P:\Tools\NimbusSparkVenv\Lib\site-packages"
if os.path.isdir(SPARK_VENV_SITE_PACKAGES) and SPARK_VENV_SITE_PACKAGES not in sys.path:
    sys.path.insert(0, SPARK_VENV_SITE_PACKAGES)

try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    duckdb = None
    DUCKDB_AVAILABLE = False

try:
    import pypdf
    PYPDF_AVAILABLE = True
except ImportError:
    pypdf = None
    PYPDF_AVAILABLE = False

try:
    from PIL import Image as PILImage
    from PIL import ExifTags
    PILLOW_AVAILABLE = True
except ImportError:
    PILImage = None
    ExifTags = None
    PILLOW_AVAILABLE = False


def get_dependency_status() -> Dict[str, bool]:
    """Returns the availability of external extraction libraries."""
    return {
        "pypdf_available": PYPDF_AVAILABLE,
        "pillow_available": PILLOW_AVAILABLE,
        "duckdb_available": DUCKDB_AVAILABLE,
    }


def ensure_temp_dir(base_dir: Optional[Path] = None) -> Path:
    """Ensures base temporary directory exists and returns it."""
    target_dir = base_dir or DEFAULT_TEMP_DIR
    target_dir.mkdir(parents=True, exist_ok=True)
    return target_dir


def create_temp_workspace(prefix: str = "multiformat_") -> Tuple[Path, Any]:
    """
    Creates an isolated temporary directory under P:\\Tools\\NimbusSparkTemp.
    Returns (path, cleanup_callable).
    """
    base_dir = ensure_temp_dir()
    unique_path = base_dir / f"{prefix}{uuid.uuid4().hex[:12]}"
    unique_path.mkdir(parents=True, exist_ok=True)

    def cleanup():
        if unique_path.exists():
            shutil.rmtree(unique_path, ignore_errors=True)

    return unique_path, cleanup


def detect_format(filename: Optional[str] = None, data_bytes: Optional[bytes] = None) -> Optional[str]:
    """
    Detects file format from filename extension or magic bytes.
    """
    if filename:
        ext = Path(filename).suffix.lower()
        mapping = {
            ".csv": "CSV",
            ".json": "JSON",
            ".xml": "XML",
            ".txt": "TXT",
            ".log": "LOG",
            ".pdf": "PDF",
            ".jpg": "JPG",
            ".jpeg": "JPG",
            ".png": "PNG",
            ".parquet": "PARQUET",
        }
        if ext in mapping:
            return mapping[ext]

    if data_bytes and len(data_bytes) >= 4:
        if data_bytes.startswith(b"PAR1") or data_bytes[:4] == b"PAR1":
            return "PARQUET"
        if data_bytes.startswith(b"%PDF"):
            return "PDF"
        if data_bytes.startswith(b"\x89PNG\r\n\x1a\n"):
            return "PNG"
        if data_bytes.startswith(b"\xff\xd8\xff"):
            return "JPG"
        # Check text-based magic signatures
        stripped = data_bytes[:1024].strip()
        if stripped.startswith(b"\xef\xbb\xbf"):
            stripped = stripped[3:].strip()
        if stripped.startswith(b"<?xml") or (stripped.startswith(b"<") and b">" in stripped):
            return "XML"
        if stripped.startswith(b"{") or stripped.startswith(b"["):
            try:
                json.loads(stripped.decode("utf-8-sig", errors="ignore"))
                return "JSON"
            except Exception:
                pass

    return None


def read_input_bytes(
    file_input: Union[str, Path, bytes, io.BytesIO],
    filename: Optional[str] = None,
) -> Tuple[bytes, str]:
    """
    Normalizes file input to raw bytes and resolves filename.
    """
    if isinstance(file_input, (str, Path)):
        p = Path(file_input)
        resolved_name = filename or p.name
        if not p.is_file():
            raise FileNotFoundError(f"Input file not found: {p}")
        with open(p, "rb") as f:
            data = f.read()
        return data, resolved_name
    elif isinstance(file_input, bytes):
        return file_input, (filename or "unnamed_input")
    elif isinstance(file_input, (io.BytesIO, io.BufferedReader)):
        data = file_input.read()
        return data, (filename or "unnamed_input")
    else:
        raise ValueError(f"Unsupported file_input type: {type(file_input)}")


def build_result(
    source_name: str,
    format_name: str,
    status: str,
    record_or_page_count: int,
    extracted_preview: Any,
    metadata: Dict[str, Any],
    error: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Builds a consistent multi-format processing result dictionary.
    """
    return {
        "source_name": source_name,
        "format": format_name,
        "status": status,
        "record_or_page_count": record_or_page_count,
        "extracted_preview": extracted_preview,
        "metadata": metadata,
        "error": error,
        "dependency_status": get_dependency_status(),
    }


# ============================================================================
# FORMAT EXTRACTORS
# ============================================================================

def extract_csv(data_bytes: bytes, filename: str, preview_limit: int = 50) -> Dict[str, Any]:
    """
    Extracts tabular preview and metadata from CSV data using built-in csv module.
    """
    try:
        text = data_bytes.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = data_bytes.decode("latin-1", errors="replace")

    lines = [line for line in text.splitlines() if line.strip()]
    if not lines:
        return build_result(
            source_name=filename,
            format_name="CSV",
            status="success",
            record_or_page_count=0,
            extracted_preview=[],
            metadata={"row_count": 0, "column_count": 0, "columns": [], "delimiter": ","},
        )

    # Sniff delimiter
    sample = "\n".join(lines[:10])
    delimiter = ","
    try:
        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(sample)
        delimiter = dialect.delimiter
    except Exception:
        delimiter = ","

    reader = csv.reader(lines, delimiter=delimiter)
    all_rows = list(reader)
    if not all_rows:
        return build_result(
            source_name=filename,
            format_name="CSV",
            status="success",
            record_or_page_count=0,
            extracted_preview=[],
            metadata={"row_count": 0, "column_count": 0, "columns": [], "delimiter": delimiter},
        )

    headers = [h.strip() for h in all_rows[0]]
    data_rows = all_rows[1:]
    target_rows = data_rows if (preview_limit is None or preview_limit <= 0) else data_rows[:preview_limit]
    preview_records = []
    for r in target_rows:
        row_dict = {}
        for col_idx, col_name in enumerate(headers):
            val = r[col_idx] if col_idx < len(r) else None
            row_dict[col_name] = val
        preview_records.append(row_dict)

    metadata = {
        "columns": headers,
        "column_count": len(headers),
        "total_rows": len(data_rows),
        "delimiter": delimiter,
        "size_bytes": len(data_bytes),
    }

    return build_result(
        source_name=filename,
        format_name="CSV",
        status="success",
        record_or_page_count=len(data_rows),
        extracted_preview=preview_records,
        metadata=metadata,
    )


def extract_json(data_bytes: bytes, filename: str, preview_limit: int = 50) -> Dict[str, Any]:
    """
    Extracts structured records and schema metadata from JSON data.
    Supports JSON arrays, single JSON objects, and newline-delimited JSON.
    """
    try:
        text = data_bytes.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = data_bytes.decode("latin-1", errors="replace")

    text_stripped = text.strip()
    if not text_stripped:
        return build_result(
            source_name=filename,
            format_name="JSON",
            status="success",
            record_or_page_count=0,
            extracted_preview=[],
            metadata={"root_type": "empty", "size_bytes": 0},
        )

    # Try standard JSON
    try:
        parsed = json.loads(text_stripped)
        if isinstance(parsed, list):
            preview = parsed if (preview_limit is None or preview_limit <= 0) else parsed[:preview_limit]
            metadata = {
                "root_type": "array",
                "item_count": len(parsed),
                "element_type": type(parsed[0]).__name__ if parsed else "unknown",
                "size_bytes": len(data_bytes),
            }
            if parsed and isinstance(parsed[0], dict):
                metadata["sample_keys"] = sorted(list(parsed[0].keys()))
            return build_result(
                source_name=filename,
                format_name="JSON",
                status="success",
                record_or_page_count=len(parsed),
                extracted_preview=preview,
                metadata=metadata,
            )
        elif isinstance(parsed, dict):
            all_keys = list(parsed.keys())
            target_keys = all_keys if (preview_limit is None or preview_limit <= 0) else all_keys[:preview_limit]
            preview = {k: parsed[k] for k in target_keys}
            metadata = {
                "root_type": "object",
                "top_level_keys": sorted(list(parsed.keys())),
                "key_count": len(parsed),
                "size_bytes": len(data_bytes),
            }
            return build_result(
                source_name=filename,
                format_name="JSON",
                status="success",
                record_or_page_count=len(parsed),
                extracted_preview=preview,
                metadata=metadata,
            )
        else:
            return build_result(
                source_name=filename,
                format_name="JSON",
                status="success",
                record_or_page_count=1,
                extracted_preview=parsed,
                metadata={"root_type": type(parsed).__name__, "size_bytes": len(data_bytes)},
            )
    except json.JSONDecodeError:
        # Check if Newline-Delimited JSON (NDJSON)
        lines = [l for l in text_stripped.splitlines() if l.strip()]
        ndjson_records = []
        is_ndjson = True
        for l in lines:
            try:
                ndjson_records.append(json.loads(l))
            except Exception:
                is_ndjson = False
                break

        if is_ndjson and ndjson_records:
            metadata = {
                "root_type": "ndjson",
                "item_count": len(ndjson_records),
                "size_bytes": len(data_bytes),
            }
            if isinstance(ndjson_records[0], dict):
                metadata["sample_keys"] = sorted(list(ndjson_records[0].keys()))
            preview_ndjson = ndjson_records if (preview_limit is None or preview_limit <= 0) else ndjson_records[:preview_limit]
            return build_result(
                source_name=filename,
                format_name="JSON",
                status="success",
                record_or_page_count=len(ndjson_records),
                extracted_preview=preview_ndjson,
                metadata=metadata,
            )

        raise ValueError(f"Invalid JSON content in {filename}")


def extract_xml(data_bytes: bytes, filename: str, preview_limit: int = 50) -> Dict[str, Any]:
    """
    Extracts XML hierarchy, child records, and tag metadata using xml.etree.ElementTree.
    """
    try:
        root = ET.fromstring(data_bytes)
    except ET.ParseError as e:
        raise ValueError(f"XML parse error in {filename}: {str(e)}")

    child_elements = list(root)
    distinct_child_tags = sorted(list({child.tag for child in child_elements}))
    target_children = child_elements if (preview_limit is None or preview_limit <= 0) else child_elements[:preview_limit]

    preview_records = []
    for child in target_children:
        rec = {
            "tag": child.tag,
            "attributes": dict(child.attrib),
        }
        sub_elements = {sub.tag: (sub.text or "").strip() for sub in child}
        if sub_elements:
            rec["fields"] = sub_elements
        else:
            rec["text"] = (child.text or "").strip()
        preview_records.append(rec)

    metadata = {
        "root_tag": root.tag,
        "root_attributes": dict(root.attrib),
        "direct_child_count": len(child_elements),
        "distinct_child_tags": distinct_child_tags,
        "size_bytes": len(data_bytes),
    }

    return build_result(
        source_name=filename,
        format_name="XML",
        status="success",
        record_or_page_count=len(child_elements),
        extracted_preview=preview_records,
        metadata=metadata,
    )


def extract_txt(data_bytes: bytes, filename: str, preview_limit: int = 50) -> Dict[str, Any]:
    """
    Extracts line records, word counts, and textual metadata from plain text files.
    """
    try:
        text = data_bytes.decode("utf-8-sig")
        encoding = "utf-8"
    except UnicodeDecodeError:
        text = data_bytes.decode("latin-1", errors="replace")
        encoding = "latin-1"

    raw_lines = text.splitlines()
    preview_records = []
    non_empty_count = 0
    total_words = 0

    for idx, line in enumerate(raw_lines, start=1):
        stripped = line.strip()
        if stripped:
            non_empty_count += 1
            total_words += len(stripped.split())
        if (preview_limit is None or preview_limit <= 0) or idx <= preview_limit:
            preview_records.append({"line_number": idx, "text": line})

    metadata = {
        "line_count": len(raw_lines),
        "non_empty_line_count": non_empty_count,
        "word_count": total_words,
        "character_count": len(text),
        "encoding": encoding,
        "size_bytes": len(data_bytes),
    }

    return build_result(
        source_name=filename,
        format_name="TXT",
        status="success",
        record_or_page_count=len(raw_lines),
        extracted_preview=preview_records,
        metadata=metadata,
    )


# Supported Log Regex Patterns
LOG_PATTERNS = [
    # Pattern 1: [TIMESTAMP] [SEVERITY] MESSAGE or TIMESTAMP [SEVERITY] MESSAGE
    re.compile(
        r"^(?:\[(?P<ts1>\d{4}[-/]\d{2}[-/]\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)\]|"
        r"(?P<ts2>\d{4}[-/]\d{2}[-/]\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)|"
        r"(?P<ts3>[A-Z][a-z]{2}\s+\d+\s+\d{2}:\d{2}:\d{2}))"
        r"\s*[:\-]?\s*"
        r"(?:\[?(?P<sev>INFO|WARN|WARNING|ERROR|ERR|DEBUG|CRITICAL|FATAL|TRACE)\]?:?)"
        r"\s*[:\-]?\s*"
        r"(?P<msg>.*)$",
        re.IGNORECASE,
    ),
    # Pattern 2: [SEVERITY] [TIMESTAMP] MESSAGE or [SEVERITY] TIMESTAMP MESSAGE
    re.compile(
        r"^\[?(?P<sev>INFO|WARN|WARNING|ERROR|ERR|DEBUG|CRITICAL|FATAL|TRACE)\]?:?"
        r"\s*[:\-]?\s*"
        r"(?:\[(?P<ts1>\d{4}[-/]\d{2}[-/]\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)\]|"
        r"(?P<ts2>\d{4}[-/]\d{2}[-/]\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)|"
        r"(?P<ts3>[A-Z][a-z]{2}\s+\d+\s+\d{2}:\d{2}:\d{2}))"
        r"\s*[:\-]?\s*"
        r"(?P<msg>.*)$",
        re.IGNORECASE,
    ),
]


def extract_log(data_bytes: bytes, filename: str, preview_limit: int = 50) -> Dict[str, Any]:
    """
    Extracts structured log attributes: timestamp, severity, and message.
    Preserves unmatched lines with full content and matched=False flag.
    """
    try:
        text = data_bytes.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = data_bytes.decode("latin-1", errors="replace")

    raw_lines = text.splitlines()
    parsed_records = []
    severity_counts: Dict[str, int] = {}
    matched_count = 0
    unmatched_count = 0

    for idx, line in enumerate(raw_lines, start=1):
        clean_line = line.strip()
        matched = False
        ts_val = None
        sev_val = None
        msg_val = clean_line

        for pattern in LOG_PATTERNS:
            m = pattern.match(clean_line)
            if m:
                matched = True
                ts_val = m.group("ts1") or m.group("ts2") or m.group("ts3")
                raw_sev = (m.group("sev") or "").upper()
                # Normalize WARNING -> WARN, ERR -> ERROR
                sev_val = "WARN" if raw_sev == "WARNING" else ("ERROR" if raw_sev == "ERR" else raw_sev)
                msg_val = m.group("msg") or ""
                break

        if matched:
            matched_count += 1
            severity_counts[sev_val] = severity_counts.get(sev_val, 0) + 1
        else:
            unmatched_count += 1

        record = {
            "line_number": idx,
            "timestamp": ts_val,
            "severity": sev_val,
            "message": msg_val,
            "matched": matched,
            "raw_line": line,
        }
        parsed_records.append(record)

    preview_records = parsed_records if (preview_limit is None or preview_limit <= 0) else parsed_records[:preview_limit]

    metadata = {
        "total_lines": len(raw_lines),
        "matched_lines": matched_count,
        "unmatched_lines": unmatched_count,
        "severity_counts": severity_counts,
        "has_timestamps": matched_count > 0,
        "size_bytes": len(data_bytes),
    }

    return build_result(
        source_name=filename,
        format_name="LOG",
        status="success",
        record_or_page_count=len(raw_lines),
        extracted_preview=preview_records,
        metadata=metadata,
    )


def extract_pdf(data_bytes: bytes, filename: str, preview_limit: int = 50) -> Dict[str, Any]:
    """
    Extracts PDF pages, text preview, and metadata.
    Uses pypdf if installed; falls back to pure-Python header & page estimation if not installed.
    """
    if not data_bytes.startswith(b"%PDF"):
        raise ValueError(f"Invalid PDF header in {filename}")

    # Detect PDF version from header
    m_ver = re.search(rb"%PDF-(\d+\.\d+)", data_bytes[:1024])
    pdf_version = m_ver.group(1).decode("ascii") if m_ver else "unknown"

    if PYPDF_AVAILABLE and pypdf is not None:
        try:
            reader = pypdf.PdfReader(io.BytesIO(data_bytes))
            page_count = len(reader.pages)
            pages_to_process = reader.pages if (preview_limit is None or preview_limit <= 0) else reader.pages[:preview_limit]
            pages_preview = []
            total_extracted_chars = 0
            for page_idx, page in enumerate(pages_to_process, start=1):
                try:
                    page_text = page.extract_text() or ""
                except Exception:
                    page_text = ""
                total_extracted_chars += len(page_text.strip())
                pages_preview.append({
                    "page_number": page_idx,
                    "char_count": len(page_text),
                    "text_preview": page_text[:500] if (preview_limit and preview_limit > 0) else page_text,
                })

            doc_info = {}
            if reader.metadata:
                for k, v in reader.metadata.items():
                    key_clean = str(k).lstrip("/")
                    doc_info[key_clean] = str(v)

            has_text = total_extracted_chars > 0
            is_partial = (page_count > 0 and not has_text)

            metadata = {
                "page_count": page_count,
                "pdf_version": pdf_version,
                "is_encrypted": reader.is_encrypted,
                "document_info": doc_info,
                "has_extractable_text": has_text,
                "size_bytes": len(data_bytes),
                "pypdf_used": True,
            }

            status = "partial" if is_partial else "success"
            error_msg = (
                "Scanned or image-only PDF: no digital text stream found on pages. OCR is required for text extraction."
                if is_partial
                else None
            )

            return build_result(
                source_name=filename,
                format_name="PDF",
                status=status,
                record_or_page_count=page_count,
                extracted_preview=pages_preview,
                metadata=metadata,
                error=error_msg,
            )
        except Exception as e:
            raise ValueError(f"pypdf failed to extract {filename}: {str(e)}")

    # Pure-Python fallback when pypdf is not installed
    # Estimate page count via /Type /Page and /Count
    page_objs = re.findall(rb"/Type\s*/Page(?![s\w])", data_bytes)
    counts = re.findall(rb"/Count\s+(\d+)", data_bytes)
    estimated_pages = max([int(c) for c in counts], default=0)
    if estimated_pages == 0:
        estimated_pages = len(page_objs)
    if estimated_pages == 0:
        estimated_pages = 1  # Minimum 1 if valid PDF header present

    preview = [
        {
            "page_number": 1,
            "status": "binary_preview_available",
            "note": "Install pypdf for complete page text extraction.",
        }
    ]

    metadata = {
        "page_count": estimated_pages,
        "pdf_version": pdf_version,
        "is_encrypted": b"/Encrypt" in data_bytes,
        "size_bytes": len(data_bytes),
        "pypdf_used": False,
        "dependency_required": "pypdf",
    }

    return build_result(
        source_name=filename,
        format_name="PDF",
        status="partial",
        record_or_page_count=estimated_pages,
        extracted_preview=preview,
        metadata=metadata,
        error="pypdf package is not installed; extracted header and structural page count in partial mode.",
    )


def extract_image(data_bytes: bytes, filename: str, format_hint: Optional[str] = None) -> Dict[str, Any]:
    """
    Extracts image dimensions, color channels, and EXIF metadata for JPG and PNG.
    Uses Pillow if installed; falls back to pure-Python binary chunk parsing if not installed.
    """
    # Detect JPG vs PNG
    is_png = data_bytes.startswith(b"\x89PNG\r\n\x1a\n")
    is_jpg = data_bytes.startswith(b"\xff\xd8")

    if not is_png and not is_jpg:
        # Check by extension or hint
        target_fmt = (format_hint or Path(filename).suffix.lstrip(".").upper())
        if target_fmt not in ("JPG", "JPEG", "PNG"):
            raise ValueError(f"Unsupported image file or invalid header for {filename}")
        is_png = (target_fmt == "PNG")
        is_jpg = (target_fmt in ("JPG", "JPEG"))

    resolved_format = "PNG" if is_png else "JPG"

    if PILLOW_AVAILABLE and PILImage is not None:
        try:
            with PILImage.open(io.BytesIO(data_bytes)) as img:
                width, height = img.size
                mode = img.mode
                img_format = img.format or resolved_format
                exif_dict = {}
                if hasattr(img, "_getexif") and img._getexif():
                    raw_exif = img._getexif()
                    if raw_exif and ExifTags:
                        for tag_id, value in raw_exif.items():
                            tag_name = ExifTags.TAGS.get(tag_id, str(tag_id))
                            # Keep scalar string/number representation
                            if isinstance(value, (int, float, str)):
                                exif_dict[tag_name] = value

                preview = [{
                    "width": width,
                    "height": height,
                    "mode": mode,
                    "format": img_format,
                    "aspect_ratio": round(width / max(height, 1), 3),
                }]

                metadata = {
                    "width": width,
                    "height": height,
                    "mode": mode,
                    "format": img_format,
                    "size_bytes": len(data_bytes),
                    "exif": exif_dict,
                    "pillow_used": True,
                }

                return build_result(
                    source_name=filename,
                    format_name=resolved_format,
                    status="success",
                    record_or_page_count=1,
                    extracted_preview=preview,
                    metadata=metadata,
                )
        except Exception as e:
            raise ValueError(f"Pillow failed to open image {filename}: {str(e)}")

    # Pure-Python fallback when Pillow is not installed
    if is_png:
        if len(data_bytes) < 24:
            raise ValueError(f"PNG binary data too short in {filename}")
        # IHDR chunk starts at byte 12 (4 length + 4 'IHDR'), data at 16
        w, h, bit_depth, color_type = struct.unpack(">IIBB", data_bytes[16:26])
        color_type_map = {
            0: "Grayscale",
            2: "RGB",
            3: "Indexed/Palette",
            4: "Grayscale + Alpha",
            6: "RGBA",
        }
        color_desc = color_type_map.get(color_type, f"Type_{color_type}")
        preview = [{
            "width": w,
            "height": h,
            "color_mode": color_desc,
            "format": "PNG",
            "aspect_ratio": round(w / max(h, 1), 3),
        }]
        metadata = {
            "width": w,
            "height": h,
            "bit_depth": bit_depth,
            "color_type": color_type,
            "color_mode": color_desc,
            "size_bytes": len(data_bytes),
            "pillow_used": False,
            "dependency_required": "Pillow",
        }
        return build_result(
            source_name=filename,
            format_name="PNG",
            status="partial",
            record_or_page_count=1,
            extracted_preview=preview,
            metadata=metadata,
            error="Pillow package is not installed; extracted dimensions and color mode from PNG IHDR header.",
        )

    elif is_jpg:
        if len(data_bytes) < 4:
            raise ValueError(f"JPEG binary data too short in {filename}")
        stream = data_bytes
        idx = 2
        w, h, channels, precision = None, None, None, None
        sof_markers = {
            0xC0: "Baseline",
            0xC1: "Extended",
            0xC2: "Progressive",
        }

        while idx < len(stream) - 8:
            if stream[idx] != 0xFF:
                idx += 1
                continue
            idx += 1
            marker = stream[idx]
            idx += 1
            if marker in (0xD8, 0xD9):  # SOI, EOI
                continue
            if idx + 2 > len(stream):
                break
            seg_len = struct.unpack(">H", stream[idx:idx + 2])[0]
            if marker in sof_markers:
                precision, h, w, channels = struct.unpack(">BHHB", stream[idx + 2:idx + 8])
                break
            idx += seg_len

        if w is None or h is None:
            raise ValueError(f"Could not locate SOF marker in JPEG file {filename}")

        chan_map = {1: "Grayscale", 3: "YCbCr/RGB", 4: "CMYK"}
        chan_desc = chan_map.get(channels, f"{channels}-channel")

        preview = [{
            "width": w,
            "height": h,
            "channels": chan_desc,
            "format": "JPG",
            "aspect_ratio": round(w / max(h, 1), 3),
        }]
        metadata = {
            "width": w,
            "height": h,
            "precision": precision,
            "channels": channels,
            "color_mode": chan_desc,
            "size_bytes": len(data_bytes),
            "pillow_used": False,
            "dependency_required": "Pillow",
        }
        return build_result(
            source_name=filename,
            format_name="JPG",
            status="partial",
            record_or_page_count=1,
            extracted_preview=preview,
            metadata=metadata,
            error="Pillow package is not installed; extracted dimensions and channels from JPEG SOF header.",
        )

    raise ValueError(f"Unsupported image format for {filename}")


def extract_parquet(data_bytes: bytes, filename: str, preview_limit: int = 50) -> Dict[str, Any]:
    """
    Extracts Parquet schema, column types, row count, and a bounded sample of records using DuckDB.
    Ensures safe temporary file handling and database connection cleanup.
    """
    if len(data_bytes) < 4:
        raise ValueError(f"Parquet file {filename} is too short or empty ({len(data_bytes)} bytes)")

    if not DUCKDB_AVAILABLE or duckdb is None:
        raise RuntimeError("DuckDB package is required for Parquet preview but is not available.")

    temp_dir = ensure_temp_dir()
    temp_file = temp_dir / f"nimbus_pq_{uuid.uuid4().hex[:12]}.parquet"

    conn = None
    try:
        with open(temp_file, "wb") as f:
            f.write(data_bytes)

        conn = duckdb.connect(":memory:")
        file_path_str = str(temp_file).replace("\\", "/")

        # 1. Schema & Column Types
        desc = conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{file_path_str}')").fetchall()
        columns = [row[0] for row in desc]
        column_types = {row[0]: row[1] for row in desc}

        # 2. Row count
        row_count = conn.execute(f"SELECT count(*) FROM read_parquet('{file_path_str}')").fetchone()[0]

        # 3. Sample records bounded by preview_limit
        limit = preview_limit if (preview_limit is not None and preview_limit > 0) else 50
        cur = conn.execute(f"SELECT * FROM read_parquet('{file_path_str}') LIMIT {limit}")
        col_names = [d[0] for d in cur.description]
        rows = cur.fetchall()

        def sanitize_val(v):
            if v is None:
                return None
            if isinstance(v, (int, float, bool, str)):
                return v
            return str(v)

        preview_records = [
            {col_names[i]: sanitize_val(row[i]) for i in range(len(col_names))}
            for row in rows
        ]

        metadata = {
            "format": "Parquet",
            "total_records": row_count,
            "columns": columns,
            "column_types": column_types,
            "size_bytes": len(data_bytes),
            "duckdb_used": True,
        }

        return build_result(
            source_name=filename,
            format_name="PARQUET",
            status="success",
            record_or_page_count=row_count,
            extracted_preview=preview_records,
            metadata=metadata,
        )
    except Exception as e:
        raise ValueError(f"Failed to parse Parquet file {filename}: {str(e)}")
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        if temp_file.exists():
            try:
                temp_file.unlink()
            except Exception:
                pass


# ============================================================================
# UNIFIED DISPATCHER
# ============================================================================

def process_multi_format_content(
    file_input: Union[str, Path, bytes, io.BytesIO],
    filename: Optional[str] = None,
    format_hint: Optional[str] = None,
    preview_limit: int = 50,
) -> Dict[str, Any]:
    """
    Unified entry point to extract content from any of the 8 supported formats:
    CSV, JSON, XML, TXT, LOG, PDF, JPG, PNG.

    Returns a standardized dictionary matching the Nimbus Multi-Format Result schema.
    Guarantees isolation and error trapping.
    """
    resolved_name = filename or "unnamed_file"
    try:
        data_bytes, resolved_name = read_input_bytes(file_input, filename=filename)
    except Exception as e:
        return build_result(
            source_name=resolved_name,
            format_name=format_hint or "UNKNOWN",
            status="error",
            record_or_page_count=0,
            extracted_preview=None,
            metadata={},
            error=f"Failed to read file input: {str(e)}",
        )

    # Detect or validate format
    detected_format = (format_hint or detect_format(resolved_name, data_bytes) or "").upper()
    if detected_format == "JPEG":
        detected_format = "JPG"

    if detected_format not in SUPPORTED_FORMATS:
        return build_result(
            source_name=resolved_name,
            format_name=detected_format or "UNKNOWN",
            status="error",
            record_or_page_count=0,
            extracted_preview=None,
            metadata={"size_bytes": len(data_bytes)},
            error=f"Unsupported format '{detected_format}'. Supported formats: {sorted(list(SUPPORTED_FORMATS))}",
        )

    # Dispatch to specific extractor
    try:
        if detected_format == "CSV":
            return extract_csv(data_bytes, resolved_name, preview_limit=preview_limit)
        elif detected_format == "JSON":
            return extract_json(data_bytes, resolved_name, preview_limit=preview_limit)
        elif detected_format == "XML":
            return extract_xml(data_bytes, resolved_name, preview_limit=preview_limit)
        elif detected_format == "TXT":
            return extract_txt(data_bytes, resolved_name, preview_limit=preview_limit)
        elif detected_format == "LOG":
            return extract_log(data_bytes, resolved_name, preview_limit=preview_limit)
        elif detected_format == "PDF":
            return extract_pdf(data_bytes, resolved_name, preview_limit=preview_limit)
        elif detected_format in ("JPG", "PNG"):
            return extract_image(data_bytes, resolved_name, format_hint=detected_format)
        elif detected_format == "PARQUET":
            return extract_parquet(data_bytes, resolved_name, preview_limit=preview_limit)
        else:
            return build_result(
                source_name=resolved_name,
                format_name=detected_format,
                status="error",
                record_or_page_count=0,
                extracted_preview=None,
                metadata={},
                error=f"No extractor implemented for {detected_format}",
            )
    except Exception as e:
        return build_result(
            source_name=resolved_name,
            format_name=detected_format,
            status="error",
            record_or_page_count=0,
            extracted_preview=None,
            metadata={"size_bytes": len(data_bytes)},
            error=f"Extraction failure for {detected_format}: {str(e)}",
        )
