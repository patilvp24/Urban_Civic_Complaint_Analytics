"""
Unit Tests for Downloader Configuration and Deduplication Logic
Can be run via `python -m unittest discover tests` or `pytest`.
"""

import unittest
import os
import tempfile
import json
import hashlib
from scripts.download_snapshot import (
    SELECTED_COLUMNS,
    DATASET_ID,
    BASE_URL,
    build_query_params,
    compute_file_sha256,
    verify_existing_snapshot
)


class TestDownloaderConfig(unittest.TestCase):

    def test_required_columns_present(self):
        """Verify all planned analytical columns are declared."""
        required = [
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
        for col in required:
            self.assertIn(col, SELECTED_COLUMNS, f"Missing required analytical column: {col}")

    def test_query_params_builder(self):
        """Verify SODA query parameter dictionary structure."""
        params = build_query_params(SELECTED_COLUMNS, limit=1000, offset=5000, order="unique_key ASC")
        self.assertEqual(params["$limit"], 1000)
        self.assertEqual(params["$offset"], 5000)
        self.assertEqual(params["$order"], "unique_key ASC")
        self.assertIn("unique_key", params["$select"])
        self.assertIn("borough", params["$select"])

    def test_sha256_computation(self):
        """Verify streaming sha256 function computes accurate hashes."""
        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as tf:
            test_bytes = b"NYC 311 Big Data MapReduce Analytics Test Payload\n"
            tf.write(test_bytes)
            temp_path = tf.name

        try:
            expected_hash = hashlib.sha256(test_bytes).hexdigest()
            computed_hash = compute_file_sha256(temp_path)
            self.assertEqual(computed_hash, expected_hash)
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def test_verify_existing_snapshot_validation(self):
        """Verify that verify_existing_snapshot catches corruptions and valid files."""
        with tempfile.TemporaryDirectory() as td:
            csv_path = os.path.join(td, "test_snapshot.csv")
            meta_path = os.path.join(td, "metadata.json")
            content = b"unique_key,agency\n1001,NYPD\n1002,DOT\n"
            
            with open(csv_path, "wb") as f:
                f.write(content)
                
            file_sha = hashlib.sha256(content).hexdigest()
            valid_meta = {
                "distinct_unique_keys": 2,
                "sha256_checksum": file_sha
            }
            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump(valid_meta, f)

            # Should pass when min_rows <= 2
            self.assertTrue(verify_existing_snapshot(csv_path, meta_path, min_rows=2))
            
            # Should fail when required min_rows is higher
            self.assertFalse(verify_existing_snapshot(csv_path, meta_path, min_rows=10))

            # Should fail if file checksum does not match
            corrupt_meta = {
                "distinct_unique_keys": 2,
                "sha256_checksum": "badhash12345"
            }
            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump(corrupt_meta, f)
            self.assertFalse(verify_existing_snapshot(csv_path, meta_path, min_rows=2))


if __name__ == "__main__":
    unittest.main()
