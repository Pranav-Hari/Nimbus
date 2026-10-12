import os
import sys
import io
import time
import uuid
from pathlib import Path
from datetime import datetime, timezone
import json
import subprocess
import threading

PROJECT_DIR = Path(__file__).resolve().parent.parent

for pkg in [
    r"P:\Tools\NimbusBackendPackages",
    r"P:\Tools\NimbusPythonPackages",
    r"P:\Tools\NimbusSparkVenv\Lib\site-packages",
    str(PROJECT_DIR),
]:
    if os.path.isdir(pkg) and pkg not in sys.path:
        sys.path.insert(0, pkg)

from typing import Optional, Dict, Any, List
from fastapi import FastAPI, File, HTTPException, UploadFile, Query
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from minio import Minio
from minio.error import S3Error

PROJECT_DIR = Path(__file__).resolve().parent.parent


def load_env_file():
    env_file = PROJECT_DIR / ".env"
    if not env_file.exists():
        raise RuntimeError("Project .env file is missing.")

    for line in env_file.read_text(encoding="utf-8-sig").splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())


load_env_file()

client = Minio(
    "127.0.0.1:9000",
    access_key=os.environ["MINIO_ROOT_USER"],
    secret_key=os.environ["MINIO_ROOT_PASSWORD"],
    secure=False,
)

app = FastAPI(title="Nimbus API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

ALLOWED_BUCKETS = {"raw-zone", "refined-zone"}
MAX_UPLOAD_BYTES = 25 * 1024 * 1024


@app.get("/api/health")
def health():
    try:
        buckets = [b.name for b in client.list_buckets()]
        return {
            "status": "ok",
            "storage": "connected",
            "buckets": buckets,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Storage unavailable: {type(exc).__name__}",
        )


@app.get("/api/buckets")
def list_buckets():
    try:
        buckets = [b.name for b in client.list_buckets()]
        return [
            {"name": name, "allowed": name in ALLOWED_BUCKETS}
            for name in buckets
        ]
    except S3Error as exc:
        raise HTTPException(status_code=502, detail=exc.code)


def require_bucket(bucket: str):
    if bucket not in ALLOWED_BUCKETS:
        raise HTTPException(
            status_code=400,
            detail="Only raw-zone and refined-zone are available through this API.",
        )

    try:
        if not client.bucket_exists(bucket):
            raise HTTPException(status_code=404, detail="Bucket not found.")
    except S3Error as exc:
        raise HTTPException(status_code=502, detail=exc.code)


@app.get("/api/buckets/{bucket}/objects")
def list_objects(bucket: str):
    require_bucket(bucket)

    try:
        objects = []
        for obj in client.list_objects(bucket, recursive=True):
            objects.append({
                "name": obj.object_name,
                "size": obj.size,
                "last_modified": (
                    obj.last_modified.isoformat()
                    if obj.last_modified else None
                ),
                "etag": obj.etag,
            })
        return {"bucket": bucket, "objects": objects, "count": len(objects)}
    except S3Error as exc:
        raise HTTPException(status_code=502, detail=exc.code)


@app.post("/api/buckets/{bucket}/upload")
async def upload_object(
    bucket: str,
    file: UploadFile = File(...),
    overwrite: bool = Query(False),
):
    require_bucket(bucket)
    if bucket != "raw-zone":
        raise HTTPException(
            status_code=400,
            detail="Direct uploads are restricted to 'raw-zone'. Refined zone objects must be generated via processing workflows.",
        )

    filename = (file.filename or "").replace("\\", "/").split("/")[-1]
    if not filename or filename in {".", ".."}:
        raise HTTPException(status_code=400, detail="Invalid filename.")

    # Check if object already exists in the target bucket
    object_exists = False
    try:
        client.stat_object(bucket, filename)
        object_exists = True
    except S3Error as exc:
        if exc.code not in ("NoSuchKey", "NoSuchObject"):
            raise HTTPException(status_code=502, detail=exc.code)

    if object_exists and not overwrite:
        raise HTTPException(
            status_code=409,
            detail=f"Object '{filename}' already exists in '{bucket}'. Explicit confirmation is required to overwrite.",
        )

    # Stream to a temporary file to avoid loading large uploads into RAM.
    import tempfile

    temp_path = None
    size = 0
    start_time = time.perf_counter()

    try:
        with tempfile.NamedTemporaryFile(
            prefix="nimbus-upload-", suffix=".tmp", delete=False
        ) as temp:
            temp_path = Path(temp.name)
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                if size > MAX_UPLOAD_BYTES:
                    raise HTTPException(
                        status_code=413,
                        detail="Maximum upload size is 25 MB.",
                    )
                temp.write(chunk)

        if size == 0:
            raise HTTPException(status_code=400, detail="Empty files are not accepted.")

        client.fput_object(
            bucket,
            filename,
            str(temp_path),
            content_type=file.content_type or "application/octet-stream",
        )
        elapsed_ms = round((time.perf_counter() - start_time) * 1000, 2)
        return {
            "bucket": bucket,
            "name": filename,
            "size": size,
            "uploaded": True,
            "overwritten": object_exists,
            "upload_duration_ms": elapsed_ms,
        }
    except S3Error as exc:
        raise HTTPException(status_code=502, detail=exc.code)
    finally:
        await file.close()
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)


def stream_minio_object(bucket: str, target_object: str):
    require_bucket(bucket)
    target = target_object.strip()
    if not target:
        raise HTTPException(status_code=400, detail="Missing required object name.")
    if target.endswith("/") or target in {".", ".."}:
        raise HTTPException(status_code=400, detail="Cannot download a prefix or directory path.")

    try:
        stat = client.stat_object(bucket, target)
    except S3Error as exc:
        if exc.code in ("NoSuchKey", "NoSuchObject"):
            raise HTTPException(status_code=404, detail=f"Object '{target}' not found in '{bucket}'.")
        raise HTTPException(status_code=502, detail=f"Storage error: {exc.code}")

    try:
        response = client.get_object(bucket, target)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve object: {type(exc).__name__}")

    def iterfile():
        try:
            while True:
                chunk = response.read(64 * 1024)
                if not chunk:
                    break
                yield chunk
        finally:
            response.close()
            response.release_conn()

    filename_only = target.split("/")[-1]
    headers = {
        "Content-Disposition": f'attachment; filename="{filename_only}"',
        "Content-Length": str(stat.size),
    }
    return StreamingResponse(
        iterfile(),
        media_type=stat.content_type or "application/octet-stream",
        headers=headers,
    )


@app.get("/api/buckets/{bucket}/objects/{object_name:path}/download")
def download_object_path(bucket: str, object_name: str):
    return stream_minio_object(bucket, object_name)


@app.get("/api/objects/download")
def download_object_query(
    bucket: str = "raw-zone",
    object_name: Optional[str] = None,
    name: Optional[str] = None,
):
    target = (object_name or name or "").strip()
    return stream_minio_object(bucket, target)


def execute_object_delete(bucket: str, target_object: str):
    require_bucket(bucket)
    target = target_object.strip()
    if not target:
        raise HTTPException(status_code=400, detail="Missing required object name.")
    if target.endswith("/") or target in {".", ".."}:
        raise HTTPException(
            status_code=400,
            detail="Cannot delete a prefix or directory path. Specify an exact object.",
        )

    try:
        client.stat_object(bucket, target)
    except S3Error as exc:
        if exc.code in ("NoSuchKey", "NoSuchObject"):
            raise HTTPException(status_code=404, detail=f"Object '{target}' not found in '{bucket}'.")
        raise HTTPException(status_code=502, detail=f"Storage error: {exc.code}")

    try:
        client.remove_object(bucket, target)
        return {"bucket": bucket, "object_name": target, "deleted": True}
    except S3Error as exc:
        raise HTTPException(status_code=502, detail=f"Failed to delete object: {exc.code}")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Deletion error: {type(exc).__name__}")


@app.delete("/api/buckets/{bucket}/objects/{object_name:path}")
def delete_object_path(bucket: str, object_name: str):
    return execute_object_delete(bucket, object_name)


@app.delete("/api/objects")
def delete_object_query(
    bucket: str = "raw-zone",
    object_name: Optional[str] = None,
    name: Optional[str] = None,
):
    target = (object_name or name or "").strip()
    return execute_object_delete(bucket, target)

# Phase 2: PySpark processing integration
SPARK_PYTHON = Path(r"P:\Tools\NimbusSparkVenv\Scripts\python.exe")
SPARK_PIPELINE = PROJECT_DIR / "backend" / "spark_pipeline.py"

SPARK_JOB_LOCK = threading.Lock()


@app.post("/api/processing/run")
def run_processing():
    if not SPARK_JOB_LOCK.acquire(blocking=False):
        raise HTTPException(
            status_code=409,
            detail="A Spark processing job is already running.",
        )

    try:
        if not SPARK_PYTHON.is_file():
            raise HTTPException(
                status_code=500,
                detail="The configured Spark Python interpreter was not found.",
            )

        if not SPARK_PIPELINE.is_file():
            raise HTTPException(
                status_code=500,
                detail="The Spark pipeline script was not found.",
            )

        env = os.environ.copy()
        env["PYTHONPATH"] = os.pathsep.join([
            r"P:\Tools\NimbusBackendPackages",
            r"P:\Tools\NimbusPythonPackages",
        ])
        env["JAVA_HOME"] = r"P:\Tools\Java17\jdk-17.0.20.1+1"
        env["PATH"] = (
            env["JAVA_HOME"] + r"\bin;" + env.get("PATH", "")
        )
        env["SPARK_LOCAL_IP"] = "127.0.0.1"
        env["PYSPARK_PYTHON"] = str(SPARK_PYTHON)
        env["PYSPARK_DRIVER_PYTHON"] = str(SPARK_PYTHON)
        env["TEMP"] = r"P:\Tools\NimbusSparkTemp"
        env["TMP"] = r"P:\Tools\NimbusSparkTemp"

        Path(env["TEMP"]).mkdir(parents=True, exist_ok=True)

        try:
            completed = subprocess.run(
                [str(SPARK_PYTHON), str(SPARK_PIPELINE)],
                cwd=str(PROJECT_DIR),
                env=env,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=600,
                check=False,
            )
        except subprocess.TimeoutExpired:
            raise HTTPException(
                status_code=504,
                detail="The Spark job exceeded the 10-minute time limit.",
            )

        result_lines = [
            line.removeprefix("NIMBUS_RESULT_JSON=")
            for line in completed.stdout.splitlines()
            if line.startswith("NIMBUS_RESULT_JSON=")
        ]

        if result_lines:
            result = json.loads(result_lines[-1])
            if result.get("status") == "success" and completed.returncode == 0:
                return result

            raise HTTPException(
                status_code=500,
                detail={
                    "message": "Spark processing failed.",
                    "result": result,
                    "log_tail": completed.stderr.splitlines()[-15:],
                },
            )

        raise HTTPException(
            status_code=500,
            detail={
                "message": "The Spark process did not return a structured result.",
                "return_code": completed.returncode,
                "log_tail": (
                    completed.stderr.splitlines()
                    + completed.stdout.splitlines()
                )[-20:],
            },
        )

    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Unable to execute Spark processing: {type(exc).__name__}",
        )
    finally:
        SPARK_JOB_LOCK.release()


# Phase 3: DuckDB analytics endpoints
from typing import Optional
from pydantic import BaseModel
try:
    from backend.duckdb_service import run_analytical_queries, PREDEFINED_QUERIES
except ModuleNotFoundError:
    from duckdb_service import run_analytical_queries, PREDEFINED_QUERIES


class AnalyticsRequest(BaseModel):
    run_id: Optional[str] = None
    query: Optional[str] = None


@app.get("/api/analytics/queries")
def list_available_queries():
    return {
        "queries": [
            {"key": key, "title": val["title"], "sql": val["sql"].strip()}
            for key, val in PREDEFINED_QUERIES.items()
        ]
    }


@app.api_route("/api/analytics/duckdb", methods=["GET", "POST"])
def execute_duckdb_analytics(
    request: Optional[AnalyticsRequest] = None,
    run_id: Optional[str] = None,
    query: Optional[str] = None,
):
    target_run = request.run_id if (request and request.run_id) else run_id
    target_query = request.query if (request and request.query) else query
    try:
        return run_analytical_queries(client, run_id=target_run, query_key=target_query)
    except ValueError as val_err:
        raise HTTPException(status_code=400, detail=str(val_err))
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"DuckDB analytics execution failed: {type(exc).__name__}: {str(exc)}",
        )


# Phase 4: Benchmark endpoints
BENCHMARK_JOB_LOCK = threading.Lock()

try:
    from backend.benchmark import execute_benchmark_for_dataset, get_available_datasets
except ModuleNotFoundError:
    from benchmark import execute_benchmark_for_dataset, get_available_datasets


class BenchmarkRequest(BaseModel):
    dataset: Optional[str] = "100k"
    run_id: Optional[str] = None
    repetitions: Optional[int] = 5


@app.get("/api/benchmark/datasets")
def list_benchmark_datasets():
    return {"datasets": get_available_datasets(client)}


@app.post("/api/benchmark/run")
def run_benchmark_endpoint(request: Optional[BenchmarkRequest] = None):
    dataset_choice = request.dataset if request and request.dataset else "100k"
    target_run = request.run_id if request and request.run_id else None
    repetitions = request.repetitions if request and request.repetitions else 5

    if not BENCHMARK_JOB_LOCK.acquire(blocking=False):
        raise HTTPException(
            status_code=409,
            detail="A benchmark execution is already in progress. Please wait for it to complete.",
        )

    try:
        result = execute_benchmark_for_dataset(
            client, dataset_id=dataset_choice, run_id=target_run, repetitions=repetitions
        )
        return result
    except ValueError as val_err:
        raise HTTPException(status_code=400, detail=str(val_err))
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Benchmark execution failed: {type(exc).__name__}: {str(exc)}",
        )
    finally:
        BENCHMARK_JOB_LOCK.release()


# Phase 4 / Multi-Format Endpoints
try:
    from backend.multiformat_service import (
        process_multi_format_content,
        detect_format,
        get_dependency_status,
        SUPPORTED_FORMATS,
    )
except ModuleNotFoundError:
    from multiformat_service import (
        process_multi_format_content,
        detect_format,
        get_dependency_status,
        SUPPORTED_FORMATS,
    )


class MultiFormatProcessRequest(BaseModel):
    object_name: Optional[str] = None
    name: Optional[str] = None
    bucket: Optional[str] = "raw-zone"
    format: Optional[str] = None


@app.get("/api/objects/preview")
def preview_object(
    bucket: str = "raw-zone",
    object_name: Optional[str] = None,
    name: Optional[str] = None,
):
    target_object = (object_name or name or "").strip()
    if not target_object:
        raise HTTPException(
            status_code=400,
            detail="Missing required query parameter: 'object_name'.",
        )

    require_bucket(bucket)

    response = None
    try:
        response = client.get_object(bucket, target_object)
        data_bytes = response.read()
    except S3Error as exc:
        if exc.code in ("NoSuchKey", "NoSuchObject"):
            raise HTTPException(
                status_code=404,
                detail=f"Object '{target_object}' not found in '{bucket}'.",
            )
        raise HTTPException(status_code=502, detail=f"Storage error: {exc.code}")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve object from storage: {type(exc).__name__}",
        )
    finally:
        if response is not None:
            try:
                response.close()
                response.release_conn()
            except Exception:
                pass

    detected_format = detect_format(target_object, data_bytes)
    if not detected_format or detected_format not in SUPPORTED_FORMATS:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Unsupported format for object '{target_object}'. "
                f"Supported formats: {sorted(list(SUPPORTED_FORMATS))}"
            ),
        )

    try:
        t_prev_start = time.perf_counter()
        result = process_multi_format_content(
            file_input=data_bytes,
            filename=target_object,
            format_hint=detected_format,
            preview_limit=50,
        )
        result["preview_duration_ms"] = round((time.perf_counter() - t_prev_start) * 1000, 2)
        return result
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Extraction failure: {type(exc).__name__}: {str(exc)}",
        )


@app.post("/api/processing/multiformat")
def process_multiformat_object(request: MultiFormatProcessRequest):
    target_object = (request.object_name or request.name or "").strip()
    if not target_object:
        raise HTTPException(
            status_code=400,
            detail="Missing required field: 'object_name'.",
        )

    source_bucket = (request.bucket or "raw-zone").strip()
    if source_bucket != "raw-zone":
        raise HTTPException(
            status_code=400,
            detail="Multi-format processing only accepts source objects from 'raw-zone'.",
        )

    require_bucket(source_bucket)
    require_bucket("refined-zone")

    response = None
    try:
        response = client.get_object(source_bucket, target_object)
        data_bytes = response.read()
    except S3Error as exc:
        if exc.code in ("NoSuchKey", "NoSuchObject"):
            raise HTTPException(
                status_code=404,
                detail=f"Object '{target_object}' not found in '{source_bucket}'.",
            )
        raise HTTPException(status_code=502, detail=f"Storage error: {exc.code}")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve object from storage: {type(exc).__name__}",
        )
    finally:
        if response is not None:
            try:
                response.close()
                response.release_conn()
            except Exception:
                pass

    detected_format = request.format or detect_format(target_object, data_bytes)
    if not detected_format or detected_format not in SUPPORTED_FORMATS:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Unsupported format for object '{target_object}'. "
                f"Supported formats: {sorted(list(SUPPORTED_FORMATS))}"
            ),
        )

    t_proc_start = time.perf_counter()
    result = process_multi_format_content(
        file_input=data_bytes,
        filename=target_object,
        format_hint=detected_format,
        preview_limit=0,
    )
    t_extract_end = time.perf_counter()
    extraction_duration_ms = round((t_extract_end - t_proc_start) * 1000, 2)

    now_utc = datetime.now(timezone.utc)
    run_id = f"run_{now_utc.strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"
    fmt_lower = detected_format.lower()
    prefix = f"multiformat/{fmt_lower}/{run_id}/"

    records_obj_name = f"{prefix}extracted_records.json"
    manifest_obj_name = f"{prefix}manifest.json"

    try:
        records_payload = {
            "source_name": target_object,
            "format": detected_format,
            "status": result["status"],
            "record_or_page_count": result["record_or_page_count"],
            "extraction_duration_ms": extraction_duration_ms,
            "extracted_records": result["extracted_preview"],
            "metadata": result["metadata"],
        }
        records_bytes = json.dumps(records_payload, indent=2, ensure_ascii=False).encode("utf-8")
        client.put_object(
            "refined-zone",
            records_obj_name,
            io.BytesIO(records_bytes),
            length=len(records_bytes),
            content_type="application/json",
        )

        total_duration_ms = round((time.perf_counter() - t_proc_start) * 1000, 2)

        manifest = {
            "run_id": run_id,
            "source_bucket": source_bucket,
            "source_object": target_object,
            "format": detected_format,
            "status": result["status"],
            "record_or_page_count": result["record_or_page_count"],
            "extraction_duration_ms": extraction_duration_ms,
            "processing_duration_ms": total_duration_ms,
            "metadata": result["metadata"],
            "output_location": {
                "bucket": "refined-zone",
                "prefix": prefix,
                "records_object": records_obj_name,
                "manifest_object": manifest_obj_name,
            },
            "dependency_status": result.get("dependency_status", {}),
            "error": result.get("error"),
            "processed_at": now_utc.isoformat(),
        }

        manifest_bytes = json.dumps(manifest, indent=2, ensure_ascii=False).encode("utf-8")
        client.put_object(
            "refined-zone",
            manifest_obj_name,
            io.BytesIO(manifest_bytes),
            length=len(manifest_bytes),
            content_type="application/json",
        )
    except S3Error as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Failed to store extracted records in refined-zone: {exc.code}",
        )
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Storage error while saving refined records: {type(exc).__name__}: {str(exc)}",
        )

    return {
        "run_id": run_id,
        "source_object": target_object,
        "format": detected_format,
        "status": result["status"],
        "record_or_page_count": result["record_or_page_count"],
        "extraction_duration_ms": extraction_duration_ms,
        "processing_duration_ms": total_duration_ms,
        "output_prefix": prefix,
        "records_object": records_obj_name,
        "manifest_object": manifest_obj_name,
        "manifest": manifest,
        "extracted_preview": (
            result["extracted_preview"][:50]
            if isinstance(result["extracted_preview"], list)
            else result["extracted_preview"]
        ),
        "error": result.get("error"),
    }