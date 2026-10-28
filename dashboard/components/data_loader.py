"""
Data Loader Component for Urban Civic Analytics Dashboard
Loads cached summaries and metadata from data/output/latest and data/metadata.
"""

import os
import json
import pandas as pd
from typing import Dict, Any, Optional, Tuple

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_DIR, "data")
OUTPUT_LATEST = os.path.join(DATA_DIR, "output", "latest")
METADATA_DIR = os.path.join(DATA_DIR, "metadata")


def load_json_file(filepath: str) -> Optional[Dict[str, Any]]:
    if not os.path.exists(filepath):
        return None
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def load_csv_file(filepath: str) -> pd.DataFrame:
    if not os.path.exists(filepath):
        return pd.DataFrame()
    try:
        return pd.read_csv(filepath)
    except Exception:
        return pd.DataFrame()


def get_latest_analytics_data() -> Dict[str, Any]:
    """Load all latest analytical summaries and metadata."""
    manifest = load_json_file(os.path.join(OUTPUT_LATEST, "manifest.json"))
    executive = load_json_file(os.path.join(OUTPUT_LATEST, "summary_executive.json"))
    quality = load_json_file(os.path.join(OUTPUT_LATEST, "summary_quality.json"))
    
    categories = load_csv_file(os.path.join(OUTPUT_LATEST, "summary_categories.csv"))
    agencies = load_csv_file(os.path.join(OUTPUT_LATEST, "summary_agencies.csv"))
    statuses = load_csv_file(os.path.join(OUTPUT_LATEST, "summary_statuses.csv"))
    monthly = load_csv_file(os.path.join(OUTPUT_LATEST, "summary_monthly.csv"))
    boroughs = load_csv_file(os.path.join(OUTPUT_LATEST, "summary_boroughs.csv"))
    agency_status = load_csv_file(os.path.join(OUTPUT_LATEST, "summary_agency_status.csv"))
    borough_category = load_csv_file(os.path.join(OUTPUT_LATEST, "summary_borough_category.csv"))
    agency_res = load_csv_file(os.path.join(OUTPUT_LATEST, "summary_agency_resolution.csv"))
    category_res = load_csv_file(os.path.join(OUTPUT_LATEST, "summary_category_resolution.csv"))
    
    # Metadata
    snapshot_meta = load_json_file(os.path.join(METADATA_DIR, "dataset_metadata.json"))
    cleaning_meta = load_json_file(os.path.join(METADATA_DIR, "cleaning_metadata.json"))

    has_data = executive is not None and not categories.empty

    return {
        "has_data": has_data,
        "manifest": manifest or {},
        "executive": executive or {},
        "quality": quality or {},
        "categories": categories,
        "agencies": agencies,
        "statuses": statuses,
        "monthly": monthly,
        "boroughs": boroughs,
        "agency_status": agency_status,
        "borough_category": borough_category,
        "agency_res": agency_res,
        "category_res": category_res,
        "snapshot_meta": snapshot_meta or {},
        "cleaning_meta": cleaning_meta or {}
    }
