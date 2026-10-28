"""
NYC 311 Service Requests Dataset Snapshot Downloader
Dataset ID: erm2-nwe9
Source: NYC Open Data (Socrata SODA API)

Downloads a fixed, verified local CSV snapshot containing at least 1,000,001
distinct real service request records. Tracks pagination, deduplication,
SHA-256 checksum generation, and stores immutable metadata.
"""

import os
import sys
import time
import json
import hashlib
import logging
import argparse
from typing import Dict, Any, Tuple, Optional, Set
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Default Configuration
DATASET_ID = "erm2-nwe9"
BASE_URL = f"https://data.cityofnewyork.us/resource/{DATASET_ID}.csv"
SELECTED_COLUMNS = [
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
DEFAULT_CHUNK_SIZE = 50000
DEFAULT_TARGET_ROWS = 1000005  # Ensures >= 1,000,001 distinct records

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("NYC311_Downloader")


def create_session(retries: int = 5, backoff_factor: float = 1.5) -> requests.Session:
    """Create a requests session with automatic retry on transient HTTP errors."""
    session = requests.Session()
    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def compute_file_sha256(filepath: str, block_size: int = 65536) -> str:
    """Compute SHA-256 checksum for an existing file."""
    sha = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(block_size):
            sha.update(chunk)
    return sha.hexdigest()


def build_query_params(columns: list, limit: int, offset: int, order: str = "unique_key ASC") -> Dict[str, Any]:
    """Build SODA API query parameters."""
    return {
        "$select": ",".join(columns),
        "$limit": limit,
        "$offset": offset,
        "$order": order
    }


def verify_existing_snapshot(csv_path: str, metadata_path: str, min_rows: int) -> bool:
    """Verify if an existing snapshot is valid and complete."""
    if not os.path.exists(csv_path) or not os.path.exists(metadata_path):
        return False
    try:
        with open(metadata_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
        recorded_rows = meta.get("distinct_unique_keys", 0)
        recorded_sha = meta.get("sha256_checksum", "")
        if recorded_rows < min_rows:
            logger.warning(f"Existing snapshot has only {recorded_rows} distinct rows (< {min_rows}).")
            return False
        logger.info("Computing checksum of existing snapshot file...")
        current_sha = compute_file_sha256(csv_path)
        if current_sha != recorded_sha:
            logger.warning("Checksum mismatch between file and metadata. Re-verification required.")
            return False
        logger.info(f"Existing snapshot is valid with {recorded_rows:,} verified rows (SHA-256 match).")
        return True
    except Exception as e:
        logger.warning(f"Error verifying existing snapshot: {e}")
        return False


def download_chunk(
    session: requests.Session,
    url: str,
    params: Dict[str, Any],
    timeout: int = 60
) -> Tuple[int, bytes]:
    """Download a single chunk from the SODA endpoint."""
    response = session.get(url, params=params, timeout=timeout)
    if response.status_code != 200:
        raise RuntimeError(
            f"API request failed with HTTP {response.status_code}: {response.text[:300]}"
        )
    return response.status_code, response.content


def download_snapshot(
    target_rows: int = DEFAULT_TARGET_ROWS,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    output_dir: str = "data/raw",
    metadata_dir: str = "data/metadata",
    force: bool = False
) -> Dict[str, Any]:
    """
    Download a fixed local CSV snapshot of at least target_rows distinct records.
    Saves snapshot under output_dir/nyc_311_snapshot_1M.csv and metadata under
    metadata_dir/dataset_metadata.json.
    """
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(metadata_dir, exist_ok=True)

    csv_filename = "nyc_311_snapshot_1M.csv"
    final_csv_path = os.path.join(output_dir, csv_filename)
    tmp_csv_path = os.path.join(output_dir, f"{csv_filename}.tmp")
    metadata_path = os.path.join(metadata_dir, "dataset_metadata.json")

    # Guard against accidental overwrite
    if not force and verify_existing_snapshot(final_csv_path, metadata_path, target_rows):
        logger.info(f"Verified snapshot already exists at {final_csv_path}. Skipping re-download.")
        with open(metadata_path, "r", encoding="utf-8") as f:
            return json.load(f)

    logger.info("=" * 70)
    logger.info("STARTING NYC 311 DATASET SNAPSHOT ACQUISITION")
    logger.info(f"Target Distinct Rows : >= {target_rows:,}")
    logger.info(f"Chunk Size           : {chunk_size:,} records/request")
    logger.info(f"Selected Columns     : {len(SELECTED_COLUMNS)} columns")
    logger.info(f"Output File          : {final_csv_path}")
    logger.info("=" * 70)

    session = create_session()
    hasher = hashlib.sha256()
    seen_keys: Set[str] = set()
    total_written_rows = 0
    header_written = False
    offset = 0
    start_time = time.time()
    first_record_cutoff = None
    last_record_cutoff = None

    if os.path.exists(tmp_csv_path):
        os.remove(tmp_csv_path)

    try:
        with open(tmp_csv_path, "wb") as out_f:
            while len(seen_keys) < target_rows:
                current_limit = min(chunk_size, target_rows - len(seen_keys) + 5000)
                params = build_query_params(SELECTED_COLUMNS, current_limit, offset)
                
                chunk_start = time.time()
                logger.info(
                    f"Fetching chunk at offset {offset:,} (target: {len(seen_keys):,}/{target_rows:,})..."
                )
                
                _, content_bytes = download_chunk(session, BASE_URL, params)
                lines = content_bytes.splitlines(keepends=True)
                
                if not lines:
                    logger.warning("Empty response received from endpoint; no more records available.")
                    break
                
                header_line = lines[0]
                data_lines = lines[1:]
                
                if not header_written:
                    out_f.write(header_line)
                    hasher.update(header_line)
                    header_written = True
                
                if not data_lines:
                    logger.info("No data rows returned in chunk; reached end of dataset.")
                    break
                
                chunk_new_rows = 0
                for line in data_lines:
                    # Extract unique_key (first column in CSV)
                    line_str = line.decode("utf-8", errors="replace")
                    # First column is either "12345" or 12345
                    key = line_str.split(",", 1)[0].strip().strip('"')
                    
                    if key and key not in seen_keys:
                        seen_keys.add(key)
                        out_f.write(line)
                        hasher.update(line)
                        total_written_rows += 1
                        chunk_new_rows += 1
                        
                        if first_record_cutoff is None:
                            first_record_cutoff = line_str
                        last_record_cutoff = line_str
                        
                        if len(seen_keys) >= target_rows:
                            break
                
                chunk_duration = time.time() - chunk_start
                logger.info(
                    f"  Chunk completed: +{chunk_new_rows:,} new rows | "
                    f"Total distinct: {len(seen_keys):,}/{target_rows:,} | "
                    f"Time: {chunk_duration:.2f}s"
                )
                
                offset += len(data_lines)
                
                # Small pause to be polite to the public API
                time.sleep(0.2)

        elapsed_seconds = round(time.time() - start_time, 2)
        final_checksum = hasher.hexdigest()
        file_size_bytes = os.path.getsize(tmp_csv_path)

        if len(seen_keys) < target_rows:
            raise RuntimeError(
                f"Failed to acquire target rows: only collected {len(seen_keys):,} distinct records "
                f"(required: {target_rows:,})."
            )

        # Atomic replacement
        if os.path.exists(final_csv_path):
            os.remove(final_csv_path)
        os.rename(tmp_csv_path, final_csv_path)

        metadata = {
            "dataset_id": DATASET_ID,
            "dataset_title": "311 Service Requests from 2020 to Present",
            "source_url": BASE_URL,
            "query_parameters": {
                "$select": ",".join(SELECTED_COLUMNS),
                "$order": "unique_key ASC",
                "$limit": target_rows
            },
            "snapshot_timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "total_rows_downloaded": total_written_rows,
            "distinct_unique_keys": len(seen_keys),
            "file_size_bytes": file_size_bytes,
            "file_size_mb": round(file_size_bytes / (1024 * 1024), 2),
            "sha256_checksum": final_checksum,
            "raw_csv_path": final_csv_path.replace("\\", "/"),
            "download_duration_seconds": elapsed_seconds,
            "first_record_sample": first_record_cutoff.strip() if first_record_cutoff else "",
            "last_record_sample": last_record_cutoff.strip() if last_record_cutoff else "",
            "status": "VERIFIED_IMMUTABLE_SNAPSHOT"
        }

        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2)

        logger.info("=" * 70)
        logger.info("NYC 311 DATASET SNAPSHOT SUCCESSFULLY ACQUIRED & VERIFIED")
        logger.info(f"Verified Rows (Distinct): {len(seen_keys):,}")
        logger.info(f"File Size               : {metadata['file_size_mb']} MB")
        logger.info(f"SHA-256 Checksum        : {final_checksum}")
        logger.info(f"Duration                : {elapsed_seconds}s")
        logger.info(f"Saved Metadata          : {metadata_path}")
        logger.info("=" * 70)

        return metadata

    except Exception as err:
        logger.error(f"Download aborted due to error: {err}")
        if os.path.exists(tmp_csv_path):
            try:
                os.remove(tmp_csv_path)
            except OSError:
                pass
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download fixed NYC 311 snapshot")
    parser.add_argument("--limit", type=int, default=DEFAULT_TARGET_ROWS, help="Target row count")
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE, help="Chunk size per query")
    parser.add_argument("--output-dir", type=str, default="data/raw", help="Directory for raw snapshot")
    parser.add_argument("--metadata-dir", type=str, default="data/metadata", help="Directory for metadata")
    parser.add_argument("--force", action="store_true", help="Force re-download even if snapshot exists")
    
    args = parser.parse_args()
    try:
        download_snapshot(
            target_rows=args.limit,
            chunk_size=args.chunk_size,
            output_dir=args.output_dir,
            metadata_dir=args.metadata_dir,
            force=args.force
        )
    except Exception as exc:
        logger.critical(f"Process failed: {exc}")
        sys.exit(1)
