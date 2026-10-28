"""
Unit tests for Dashboard Data Loader and Components
"""

import unittest
import os
import pandas as pd
from dashboard.components.data_loader import get_latest_analytics_data, load_json_file, load_csv_file


class TestDashboardDataLoader(unittest.TestCase):

    def test_load_latest_analytics(self):
        """Verify that get_latest_analytics_data loads real generated MapReduce summaries."""
        data = get_latest_analytics_data()
        self.assertTrue(data["has_data"], "Dashboard data loader reported has_data=False")

        # Verify executive metrics
        exec_m = data["executive"]
        self.assertIn("total_records_analyzed", exec_m)
        self.assertEqual(exec_m["total_records_analyzed"], 1000005)
        self.assertGreater(exec_m["resolution_rate_percent"], 90.0)

        # Verify categories DataFrame
        cat_df = data["categories"]
        self.assertFalse(cat_df.empty, "Categories summary dataframe is empty")
        self.assertIn("complaint_type", cat_df.columns)
        self.assertIn("count", cat_df.columns)
        self.assertIn("percentage", cat_df.columns)

        # Verify agencies DataFrame
        agency_df = data["agencies"]
        self.assertFalse(agency_df.empty, "Agencies summary dataframe is empty")
        self.assertIn("agency", agency_df.columns)

        # Verify boroughs DataFrame
        boroughs_df = data["boroughs"]
        self.assertFalse(boroughs_df.empty, "Boroughs summary dataframe is empty")
        self.assertEqual(len(boroughs_df), 6)  # 5 boroughs + UNSPECIFIED

        # Verify resolution DataFrame
        res_df = data["agency_res"]
        self.assertFalse(res_df.empty, "Agency resolution summary dataframe is empty")
        self.assertIn("avg_hours", res_df.columns)
        self.assertIn("avg_days", res_df.columns)


if __name__ == "__main__":
    unittest.main()
