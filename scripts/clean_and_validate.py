"""
NYC 311 Civic Complaint Data Cleaning and Normalization Script
Processes data/raw/nyc_311_snapshot_1M.csv via streaming.
Generates data/cleaned/nyc_311_cleaned.tsv and data/metadata/cleaning_metadata.json.
"""

import os
import sys
import csv
import time
import json
import hashlib
from datetime import datetime
from typing import Dict, Any, Tuple, Optional, Set

EXPECTED_SOURCE_COLUMNS = [
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

NORMALIZED_COLUMNS = [
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


def sanitize_text(text: Optional[str]) -> str:
    """Sanitize text fields: strip whitespace, replace tabs/newlines with spaces."""
    if text is None:
        return ""
    # Replace tabs, newlines, carriage returns with space
    cleaned = text.replace("\t", " ").replace("\r", " ").replace("\n", " ").strip()
    # Collapse multiple spaces
    return " ".join(cleaned.split())


def parse_and_normalize_timestamp(ts_str: Optional[str]) -> Tuple[Optional[str], bool]:
    """
    Normalizes timestamps to 'YYYY-MM-DD HH:MM:SS'.
    Returns (normalized_timestamp, is_valid).
    If ts_str is empty, returns ("", True).
    If unparseable, returns (None, False).
    """
    if not ts_str:
        return "", True
    cleaned = ts_str.strip()
    if not cleaned:
        return "", True

    # Fast path for standard ISO 'YYYY-MM-DDTHH:MM:SS.000' or 'YYYY-MM-DDTHH:MM:SS'
    if len(cleaned) >= 19 and cleaned[10] == "T" and cleaned[4] == "-" and cleaned[7] == "-":
        date_part = cleaned[:10]
        time_part = cleaned[11:19]
        # Quick validation of digits
        if (date_part[:4].isdigit() and date_part[5:7].isdigit() and date_part[8:10].isdigit() and
                time_part[:2].isdigit() and time_part[3:5].isdigit() and time_part[6:8].isdigit()):
            return f"{date_part} {time_part}", True

    # Fallback to datetime.strptime for varied formats
    for fmt in ("%Y-%m-%d %H:%M:%S", "%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            dt = datetime.strptime(cleaned, fmt)
            return dt.strftime("%Y-%m-%d %H:%M:%S"), True
        except ValueError:
            continue

    return None, False


def validate_coordinates(lat_str: Optional[str], lon_str: Optional[str]) -> Tuple[str, str, bool, bool]:
    """
    Validates latitude and longitude strings.
    Returns (clean_lat, clean_lon, was_present, is_valid).
    """
    if not lat_str and not lon_str:
        return "", "", False, True
    
    was_present = bool(lat_str or lon_str)
    try:
        lat = float(lat_str.strip()) if lat_str and lat_str.strip() else None
        lon = float(lon_str.strip()) if lon_str and lon_str.strip() else None
    except ValueError:
        return "", "", was_present, False

    if lat is not None and not (-90.0 <= lat <= 90.0):
        return "", "", was_present, False
    if lon is not None and not (-180.0 <= lon <= 180.0):
        return "", "", was_present, False

    clean_lat = f"{lat:.6f}" if lat is not None else ""
    clean_lon = f"{lon:.6f}" if lon is not None else ""
    return clean_lat, clean_lon, was_present, True


def normalize_borough(borough_str: Optional[str]) -> str:
    """Normalize borough string to standard uppercase or 'UNSPECIFIED'."""
    if not borough_str:
        return "UNSPECIFIED"
    b = borough_str.strip().upper()
    if not b or b in ("UNSPECIFIED", "0", "N/A", "UNKNOWN"):
        return "UNSPECIFIED"
    valid_boroughs = {"MANHATTAN", "BROOKLYN", "QUEENS", "BRONX", "STATEN ISLAND"}
    if b in valid_boroughs:
        return b
    return b


def clean_and_normalize_stream(
    raw_csv_path: str = "data/raw/nyc_311_snapshot_1M.csv",
    cleaned_tsv_path: str = "data/cleaned/nyc_311_cleaned.tsv",
    metadata_output_path: str = "data/metadata/cleaning_metadata.json",
    log_interval: int = 100000
) -> Dict[str, Any]:
    """
    Streams raw CSV, validates and normalizes fields, writes cleaned TSV.
    Guarantees that raw CSV is strictly read-only and never modified.
    """
    if not os.path.exists(raw_csv_path):
        raise FileNotFoundError(f"Raw CSV snapshot not found at {raw_csv_path}")

    os.makedirs(os.path.dirname(cleaned_tsv_path), exist_ok=True)
    os.makedirs(os.path.dirname(metadata_output_path), exist_ok=True)

    print("=" * 70)
    print("STARTING STREAMING DATA CLEANING & NORMALIZATION")
    print(f"Source Raw CSV  : {raw_csv_path}")
    print(f"Target TSV      : {cleaned_tsv_path}")
    print("=" * 70)

    start_time = time.time()
    source_rows = 0
    cleaned_rows = 0
    excluded_rows = 0
    excluded_reasons: Dict[str, int] = {}

    seen_unique_keys: Set[str] = set()
    duplicate_key_count = 0

    stats = {
        "missing_closed_dates": 0,
        "missing_descriptors": 0,
        "missing_boroughs": 0,
        "missing_coordinates": 0,
        "invalid_coordinates": 0,
        "invalid_created_timestamps": 0,
        "invalid_closed_timestamps": 0,
        "inverted_timestamps": 0  # closed_date < created_date
    }

    hasher = hashlib.sha256()
    tmp_tsv_path = f"{cleaned_tsv_path}.tmp"
    if os.path.exists(tmp_tsv_path):
        os.remove(tmp_tsv_path)

    try:
        with open(raw_csv_path, "r", encoding="utf-8", errors="replace") as in_f, \
             open(tmp_tsv_path, "w", encoding="utf-8", newline="") as out_f:

            reader = csv.DictReader(in_f)
            
            # 1. Verify header columns explicitly
            source_cols = [c.strip() for c in (reader.fieldnames or [])]
            for col in EXPECTED_SOURCE_COLUMNS:
                if col not in source_cols:
                    raise ValueError(f"Required column '{col}' missing from source CSV header: {source_cols}")

            # Write TSV header
            tsv_header = "\t".join(NORMALIZED_COLUMNS) + "\n"
            out_f.write(tsv_header)
            hasher.update(tsv_header.encode("utf-8"))

            for row in reader:
                source_rows += 1
                
                # Check Unique Key
                raw_key = row.get("unique_key", "").strip()
                if not raw_key or not raw_key.isdigit():
                    excluded_rows += 1
                    reason = "missing_or_non_numeric_unique_key"
                    excluded_reasons[reason] = excluded_reasons.get(reason, 0) + 1
                    continue

                if raw_key in seen_unique_keys:
                    duplicate_key_count += 1
                    excluded_rows += 1
                    reason = "duplicate_unique_key"
                    excluded_reasons[reason] = excluded_reasons.get(reason, 0) + 1
                    continue
                seen_unique_keys.add(raw_key)

                # Check Created Date (Mandatory for Civic Analytics)
                raw_created = row.get("created_date", "")
                norm_created, valid_created = parse_and_normalize_timestamp(raw_created)
                if not valid_created or not norm_created:
                    stats["invalid_created_timestamps"] += 1
                    excluded_rows += 1
                    reason = "invalid_created_timestamp"
                    excluded_reasons[reason] = excluded_reasons.get(reason, 0) + 1
                    continue

                # Check Closed Date (Optional - empty means OPEN ticket)
                raw_closed = row.get("closed_date", "")
                norm_closed, valid_closed = parse_and_normalize_timestamp(raw_closed)
                if not valid_closed:
                    stats["invalid_closed_timestamps"] += 1
                    norm_closed = ""
                elif not norm_closed:
                    stats["missing_closed_dates"] += 1

                # Check Chronological Consistency if closed
                if norm_closed and norm_created and norm_closed < norm_created:
                    stats["inverted_timestamps"] += 1

                # Agency & Complaint Type
                agency = sanitize_text(row.get("agency", "")) or "UNKNOWN"
                complaint_type = sanitize_text(row.get("complaint_type", "")) or "UNKNOWN"
                
                # Descriptor
                descriptor = sanitize_text(row.get("descriptor", ""))
                if not descriptor:
                    stats["missing_descriptors"] += 1
                    descriptor = "N/A"

                # Status
                status = sanitize_text(row.get("status", "")) or "Unknown"

                # Borough
                raw_borough = row.get("borough", "")
                norm_b = normalize_borough(raw_borough)
                if norm_b == "UNSPECIFIED":
                    stats["missing_boroughs"] += 1

                # Coordinates
                clean_lat, clean_lon, was_present, coords_valid = validate_coordinates(
                    row.get("latitude", ""), row.get("longitude", "")
                )
                if not was_present:
                    stats["missing_coordinates"] += 1
                elif not coords_valid:
                    stats["invalid_coordinates"] += 1

                # Build clean record strictly in NORMALIZED_COLUMNS order
                cleaned_record = [
                    raw_key,
                    norm_created,
                    norm_closed,
                    agency,
                    complaint_type,
                    descriptor,
                    status,
                    norm_b,
                    clean_lat,
                    clean_lon
                ]

                # Format as TSV line
                tsv_line = "\t".join(cleaned_record) + "\n"
                out_f.write(tsv_line)
                hasher.update(tsv_line.encode("utf-8"))
                cleaned_rows += 1

                if source_rows % log_interval == 0:
                    print(f"  Processed {source_rows:,} rows -> {cleaned_rows:,} cleaned records...")

        # Atomic replacement
        if os.path.exists(cleaned_tsv_path):
            os.remove(cleaned_tsv_path)
        os.rename(tmp_tsv_path, cleaned_tsv_path)

        duration = round(time.time() - start_time, 2)
        final_sha = hasher.hexdigest()
        file_size = os.path.getsize(cleaned_tsv_path)

        metadata = {
            "source_raw_csv_path": raw_csv_path.replace("\\", "/"),
            "cleaned_tsv_path": cleaned_tsv_path.replace("\\", "/"),
            "columns": NORMALIZED_COLUMNS,
            "delimiter": "\\t",
            "source_total_rows": source_rows,
            "cleaned_total_rows": cleaned_rows,
            "distinct_unique_keys": len(seen_unique_keys),
            "duplicate_unique_keys_detected": duplicate_key_count,
            "excluded_rows_total": excluded_rows,
            "excluded_reasons": excluded_reasons,
            "quality_metrics": {
                "open_requests_missing_closed_date": stats["missing_closed_dates"],
                "missing_descriptors_normalized": stats["missing_descriptors"],
                "unspecified_boroughs": stats["missing_boroughs"],
                "missing_coordinates": stats["missing_coordinates"],
                "invalid_coordinates_sanitized": stats["invalid_coordinates"],
                "invalid_created_timestamps_excluded": stats["invalid_created_timestamps"],
                "invalid_closed_timestamps_sanitized": stats["invalid_closed_timestamps"],
                "inverted_timestamps_retained": stats["inverted_timestamps"]
            },
            "file_size_bytes": file_size,
            "file_size_mb": round(file_size / (1024 * 1024), 2),
            "sha256_checksum": final_sha,
            "duration_seconds": duration,
            "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "status": "CLEANED_AND_NORMALIZED"
        }

        with open(metadata_output_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2)

        print("=" * 70)
        print("DATA CLEANING & NORMALIZATION COMPLETE")
        print(f"Source Rows Read    : {source_rows:,}")
        print(f"Cleaned Rows Staged : {cleaned_rows:,}")
        print(f"Excluded Rows       : {excluded_rows:,}")
        print(f"Open Requests (No Closed Date): {stats['missing_closed_dates']:,}")
        print(f"Clean TSV File Size : {metadata['file_size_mb']} MB")
        print(f"SHA-256 Checksum    : {final_sha}")
        print(f"Duration            : {duration}s")
        print("=" * 70)

        return metadata

    except Exception as e:
        if os.path.exists(tmp_tsv_path):
            try:
                os.remove(tmp_tsv_path)
            except OSError:
                pass
        raise e


if __name__ == "__main__":
    clean_and_normalize_stream()
