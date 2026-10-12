import os
import sys
from pathlib import Path

# Support standard installed packages, standard PYTHONPATH, or optional external package paths
try:
    from minio import Minio
except ImportError:
    for env_var in ("NIMBUS_PYTHONPATH", "NIMBUS_PACKAGES_DIR", "EXTRA_PYTHONPATH"):
        custom_path = os.environ.get(env_var)
        if custom_path:
            for p in custom_path.split(os.pathsep):
                if os.path.isdir(p) and p not in sys.path:
                    sys.path.insert(0, p)
    try:
        from minio import Minio
    except ImportError as err:
        raise ImportError(
            "The 'minio' package is required to run this connectivity test. "
            "Install it via 'pip install minio' or ensure it is accessible via PYTHONPATH."
        ) from err

project = Path(__file__).resolve().parent
settings = {}

env_file = project / ".env"
if env_file.exists():
    for line in env_file.read_text(encoding="utf-8-sig").splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            key, value = line.split("=", 1)
            settings[key.strip()] = value.strip()

access_key = settings.get("MINIO_ROOT_USER") or os.environ.get("MINIO_ROOT_USER")
secret_key = settings.get("MINIO_ROOT_PASSWORD") or os.environ.get("MINIO_ROOT_PASSWORD")

if not access_key or not secret_key:
    raise RuntimeError(
        "MinIO credentials not found. Ensure .env exists in project root or "
        "MINIO_ROOT_USER and MINIO_ROOT_PASSWORD are set in the environment."
    )

client = Minio(
    "127.0.0.1:9000",
    access_key=access_key,
    secret_key=secret_key,
    secure=False,
)

bucket = "nimbus-test"
source = project / "sample.txt"
downloaded = project / "sample_downloaded.txt"

source.write_text(
    "Nimbus MinIO storage test\n"
    "Upload, download, and persistence verification.\n",
    encoding="utf-8",
)

if not client.bucket_exists(bucket):
    client.make_bucket(bucket)

client.fput_object(bucket, "sample.txt", str(source))
client.fget_object(bucket, "sample.txt", str(downloaded))

if source.read_bytes() != downloaded.read_bytes():
    raise RuntimeError("Downloaded file differs from the original.")

print("Bucket:", bucket)
print("Upload: PASS")
print("Download: PASS")
print("Content comparison: PASS")

source.unlink(missing_ok=True)
downloaded.unlink(missing_ok=True)
