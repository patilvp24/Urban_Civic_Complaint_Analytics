"""
Unit tests for data cleaning, timestamp normalization, coordinate validation,
and duplicate detection.
"""

import unittest
import os
import tempfile
import csv
import json
from scripts.clean_and_validate import (
    sanitize_text,
    parse_and_normalize_timestamp,
    validate_coordinates,
    normalize_borough,
    clean_and_normalize_stream,
    NORMALIZED_COLUMNS
)


class TestCleanAndValidate(unittest.TestCase):

    def test_sanitize_text(self):
        """Verify tab, newline, and whitespace sanitization."""
        raw = "Loud Music\tParty\r\nInside Apartment   "
        expected = "Loud Music Party Inside Apartment"
        self.assertEqual(sanitize_text(raw), expected)
        self.assertEqual(sanitize_text(None), "")
        self.assertEqual(sanitize_text(""), "")

    def test_timestamp_normalization(self):
        """Verify timestamp normalization for valid, missing, and invalid values."""
        # Standard Socrata ISO
        val, ok = parse_and_normalize_timestamp("2020-09-25T15:43:34.000")
        self.assertTrue(ok)
        self.assertEqual(val, "2020-09-25 15:43:34")

        # ISO without milliseconds
        val, ok = parse_and_normalize_timestamp("2021-12-31T23:59:59")
        self.assertTrue(ok)
        self.assertEqual(val, "2021-12-31 23:59:59")

        # Missing timestamp (open ticket)
        val, ok = parse_and_normalize_timestamp("")
        self.assertTrue(ok)
        self.assertEqual(val, "")
        
        val, ok = parse_and_normalize_timestamp(None)
        self.assertTrue(ok)
        self.assertEqual(val, "")

        # Malformed timestamp
        val, ok = parse_and_normalize_timestamp("not-a-date")
        self.assertFalse(ok)
        self.assertIsNone(val)

    def test_coordinate_validation(self):
        """Verify latitude/longitude bounds and non-numeric handling."""
        # Valid NYC coordinates
        lat, lon, present, valid = validate_coordinates("40.712776", "-74.005974")
        self.assertTrue(present)
        self.assertTrue(valid)
        self.assertEqual(lat, "40.712776")
        self.assertEqual(lon, "-74.005974")

        # Empty coordinates
        lat, lon, present, valid = validate_coordinates("", "")
        self.assertFalse(present)
        self.assertTrue(valid)
        self.assertEqual(lat, "")
        self.assertEqual(lon, "")

        # Invalid bounds
        lat, lon, present, valid = validate_coordinates("95.0", "-74.0")
        self.assertTrue(present)
        self.assertFalse(valid)

        # Non-numeric
        lat, lon, present, valid = validate_coordinates("invalid", "-74.0")
        self.assertTrue(present)
        self.assertFalse(valid)

    def test_normalize_borough(self):
        """Verify borough normalization and unspecified handling."""
        self.assertEqual(normalize_borough("brooklyn"), "BROOKLYN")
        self.assertEqual(normalize_borough("Unspecified"), "UNSPECIFIED")
        self.assertEqual(normalize_borough(""), "UNSPECIFIED")
        self.assertEqual(normalize_borough(None), "UNSPECIFIED")
        self.assertEqual(normalize_borough("0"), "UNSPECIFIED")
        self.assertEqual(normalize_borough("STATEN ISLAND"), "STATEN ISLAND")

    def test_streaming_cleaner_integration(self):
        """Verify full streaming pipeline on a mock CSV with edge cases."""
        with tempfile.TemporaryDirectory() as td:
            raw_csv = os.path.join(td, "mock_raw.csv")
            clean_tsv = os.path.join(td, "mock_clean.tsv")
            meta_json = os.path.join(td, "mock_meta.json")

            # Write header + 4 rows:
            # 1. Valid closed request
            # 2. Valid open request (missing closed_date)
            # 3. Duplicate unique_key of row 1 (should be excluded)
            # 4. Invalid created_date (should be excluded)
            rows = [
                ["unique_key", "created_date", "closed_date", "agency", "complaint_type", "descriptor", "status", "borough", "latitude", "longitude"],
                ["1001", "2020-01-01T10:00:00.000", "2020-01-01T12:00:00.000", "NYPD", "Noise", "Loud Music\tParty", "Closed", "MANHATTAN", "40.71", "-74.00"],
                ["1002", "2020-01-02T11:00:00.000", "", "DOT", "Street Light", "Out", "Open", "BROOKLYN", "", ""],
                ["1001", "2020-01-01T10:00:00.000", "2020-01-01T12:00:00.000", "NYPD", "Noise", "Duplicate", "Closed", "MANHATTAN", "", ""],
                ["1003", "corrupt_timestamp", "", "DEP", "Water", "Leak", "Open", "QUEENS", "40.72", "-73.90"]
            ]

            with open(raw_csv, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerows(rows)

            meta = clean_and_normalize_stream(
                raw_csv_path=raw_csv,
                cleaned_tsv_path=clean_tsv,
                metadata_output_path=meta_json,
                log_interval=10
            )

            # Assertions
            self.assertEqual(meta["source_total_rows"], 4)
            self.assertEqual(meta["cleaned_total_rows"], 2)
            self.assertEqual(meta["excluded_rows_total"], 2)
            self.assertEqual(meta["duplicate_unique_keys_detected"], 1)
            self.assertEqual(meta["quality_metrics"]["open_requests_missing_closed_date"], 1)
            self.assertEqual(meta["excluded_reasons"]["duplicate_unique_key"], 1)
            self.assertEqual(meta["excluded_reasons"]["invalid_created_timestamp"], 1)

            # Check that output TSV does not contain tabs inside text
            with open(clean_tsv, "r", encoding="utf-8") as f:
                lines = f.readlines()
                self.assertEqual(len(lines), 3) # header + 2 data rows
                row1_fields = lines[1].strip().split("\t")
                self.assertEqual(len(row1_fields), len(NORMALIZED_COLUMNS))
                self.assertEqual(row1_fields[0], "1001")
                self.assertEqual(row1_fields[1], "2020-01-01 10:00:00")
                self.assertEqual(row1_fields[5], "Loud Music Party") # Tab sanitized to space!


if __name__ == "__main__":
    unittest.main()
