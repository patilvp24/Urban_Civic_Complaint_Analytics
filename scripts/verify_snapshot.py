"""
NYC 311 Snapshot Integrity Verifier
Verifies row counts, distinct unique_keys, schema header, and SHA-256 checksum.
"""

import os
import sys
import json
import hashlib
from typing import Dict, Any

METADATA_PATH = "data/metadata/dataset_metadata.json"
EXPECTED_COLUMNS = [
    "unique_key",
    "created_date",
    "closed_date",
    "agency",
    "complaint_type",
    "descriptor",
    "status",
    "borough",
    "latitude",
    "longitude"
]


def verify_snapshot(metadata_path: str = METADATA_PATH) -> Dict[str, Any]:
    if not os.path.exists(metadata_path):
        raise FileNotFoundError(f"Metadata file not found: {metadata_path}")

    with open(metadata_path, "r", encoding="utf-8") as f:
        meta = json.load(f)

    csv_path = meta.get("raw_csv_path")
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Snapshot CSV file not found: {csv_path}")

    print("=" * 70)
    print("VERIFYING NYC 311 SNAPSHOT INTEGRITY")
    print(f"Target File   : {csv_path}")
    print(f"Metadata File : {metadata_path}")
    print("=" * 70)

    # 1. Checksum verification
    print("Calculating SHA-256 checksum (streaming)...")
    hasher = hashlib.sha256()
    line_count = 0
    unique_keys = set()
    header = None

    with open(csv_path, "rb") as f:
        for raw_line in f:
            hasher.update(raw_line)
            line = raw_line.decode("utf-8", errors="replace").strip()
            if not line:
                continue
            if header is None:
                header = [c.strip('"') for c in line.split(",")]
                continue
            
            line_count += 1
            key = line.split(",", 1)[0].strip().strip('"')
            unique_keys.add(key)

    computed_sha = hasher.hexdigest()
    recorded_sha = meta.get("sha256_checksum")
    recorded_distinct = meta.get("distinct_unique_keys", 0)

    print(f"Header Columns Found: {len(header)} columns")
    print(f"Columns: {header}")
    print(f"Total Data Rows in CSV: {line_count:,}")
    print(f"Distinct Unique Keys  : {len(unique_keys):,}")
    print(f"Recorded Checksum     : {recorded_sha}")
    print(f"Computed Checksum     : {computed_sha}")

    # Assertions
    errors = []
    if computed_sha != recorded_sha:
        errors.append(f"Checksum mismatch! Computed: {computed_sha}, Recorded: {recorded_sha}")
    if len(unique_keys) < 1000001:
        errors.append(f"Distinct records ({len(unique_keys):,}) is less than required 1,000,001!")
    if len(unique_keys) != line_count:
        errors.append(f"Duplicate records found in CSV! Rows: {line_count}, Unique: {len(unique_keys)}")
    
    missing_cols = [c for c in EXPECTED_COLUMNS if c not in header]
    if missing_cols:
        errors.append(f"Missing required columns in header: {missing_cols}")

    if errors:
        print("\n[FAILED] Verification errors detected:")
        for err in errors:
            print(f" - {err}")
        return {"status": "FAILED", "errors": errors}
    
    print("\n[PASSED] Snapshot integrity 100% verified!")
    print(f" - File size: {meta.get('file_size_mb')} MB")
    print(f" - Distinct real records: {len(unique_keys):,}")
    print(f" - SHA-256 match: TRUE")
    return {"status": "PASSED", "distinct_records": len(unique_keys), "checksum": computed_sha}


if __name__ == "__main__":
    result = verify_snapshot()
    if result["status"] != "PASSED":
        sys.exit(1)
