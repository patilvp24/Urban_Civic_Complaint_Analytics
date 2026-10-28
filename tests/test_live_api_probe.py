"""
Lightweight Live API Probe Test
Validates connectivity, schema alignment, and response integrity
with the official NYC Open Data 311 endpoint (erm2-nwe9).
Requests only 5 rows (< 1 KB) to ensure speed and low overhead.
"""

import unittest
import requests
from scripts.download_snapshot import (
    BASE_URL,
    SELECTED_COLUMNS,
    build_query_params,
    create_session
)


class TestLiveAPIProbe(unittest.TestCase):

    def test_official_endpoint_probe(self):
        """Probe NYC Open Data SODA API to verify columns and data fidelity."""
        session = create_session(retries=3, backoff_factor=1.0)
        params = build_query_params(SELECTED_COLUMNS, limit=5, offset=0, order="unique_key ASC")

        try:
            response = session.get(BASE_URL, params=params, timeout=15)
        except requests.exceptions.RequestException as e:
            self.skipTest(f"Live API network connection failed: {e}")
            return

        self.assertEqual(
            response.status_code,
            200,
            f"Endpoint returned unexpected status code {response.status_code}: {response.text[:200]}"
        )

        lines = response.text.strip().splitlines()
        self.assertGreaterEqual(len(lines), 2, "Expected at least a header line and 1 data row.")

        # Header check
        header_cols = [c.strip('"') for c in lines[0].split(",")]
        for expected_col in SELECTED_COLUMNS:
            self.assertIn(
                expected_col,
                header_cols,
                f"Expected column '{expected_col}' not returned in SODA API header."
            )

        # First row checks
        sample_row = lines[1]
        fields = [f.strip('"') for f in sample_row.split(",")]
        unique_key = fields[0]
        self.assertTrue(unique_key.isdigit(), f"Expected numeric unique_key, got '{unique_key}'")


if __name__ == "__main__":
    unittest.main()
