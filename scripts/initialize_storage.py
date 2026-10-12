"""
Nimbus Storage Initialization & Seed Utility
Module: scripts/initialize_storage.py

Responsibilities:
- Verifies and ensures default buckets ('raw-zone' and 'refined-zone') exist in MinIO.
- Seeds multi-format demonstration samples from samples/input/ into 'raw-zone'
  (metadata.json, sample.log, and sales.csv).

Note on sales.csv:
- 'sales.csv' is seeded specifically as a demonstration and regression test fixture
  for the legacy PySpark pipeline and associated DuckDB city sales queries.
- Sales-specific processing is NOT a required Nimbus product workflow; Nimbus
  operates format-agnostically across all supported file formats.
"""

import os
import sys
from pathlib import Path

for pkg in [r"P:\Tools\NimbusPythonPackages", r"P:\Tools\NimbusBackendPackages"]:
    if os.path.isdir(pkg) and pkg not in sys.path:
        sys.path.insert(0, pkg)

from minio import Minio

# Resolve project root dynamically from scripts/
project = Path(__file__).resolve().parent.parent
settings = {}

env_path = project / ".env"
if not env_path.is_file():
    raise RuntimeError(f".env file not found at project root: {project}")

for line in env_path.read_text(encoding="utf-8-sig").splitlines():
    if "=" in line and not line.lstrip().startswith("#"):
        key, value = line.split("=", 1)
        settings[key.strip()] = value.strip().strip('"').strip("'")

client = Minio(
    "127.0.0.1:9000",
    access_key=settings["MINIO_ROOT_USER"],
    secret_key=settings["MINIO_ROOT_PASSWORD"],
    secure=False,
)

for bucket in ("raw-zone", "refined-zone"):
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        print(f"Created bucket: {bucket}")
    else:
        print(f"Bucket already exists: {bucket}")

sample_dir = project / "samples" / "input"

if sample_dir.is_dir():
    for path in sorted(sample_dir.iterdir()):
        if path.is_file():
            client.fput_object("raw-zone", path.name, str(path))
            print(f"Uploaded: raw-zone/{path.name}")

print("\nObjects currently in raw-zone:")
for obj in client.list_objects("raw-zone", recursive=True):
    print(f"  {obj.object_name} ({obj.size} bytes)")

print("\nBuckets verified:")
for bucket in ("raw-zone", "refined-zone"):
    print(f"  {bucket}: {client.bucket_exists(bucket)}")
