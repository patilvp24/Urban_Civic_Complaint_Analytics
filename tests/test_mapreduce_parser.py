"""
Unit tests for MapReduce output parser and summary generator.
"""

import unittest
import os
import tempfile
import json
import csv
from scripts.parse_mr_results import parse_mapreduce_output


class TestMapReduceParser(unittest.TestCase):

    def test_parse_mapreduce_output_mock(self):
        """Verify parsing of MapReduce tab-delimited output into CSV and JSON."""
        with tempfile.TemporaryDirectory() as td:
            mr_in = os.path.join(td, "mr_out")
            summary_out = os.path.join(td, "summaries")
            os.makedirs(mr_in, exist_ok=True)

            mock_part_r = os.path.join(mr_in, "part-r-00000")
            lines = [
                "CATEGORY\tNoise - Residential\t50000\n",
                "CATEGORY\tIllegal Parking\t30000\n",
                "CATEGORY\tBlocked Driveway\t20000\n",
                "AGENCY\tNYPD\t90000\n",
                "AGENCY\tDOT\t10000\n",
                "STATUS\tClosed\t95000\n",
                "STATUS\tOpen\t5000\n",
                "MONTH\t2020-01\t60000\n",
                "MONTH\t2020-02\t40000\n",
                "BOROUGH\tBROOKLYN\t40000\n",
                "BOROUGH\tQUEENS\t35000\n",
                "BOROUGH\tMANHATTAN\t25000\n",
                "AGENCY_STATUS\tNYPD::Closed\t86000\n",
                "AGENCY_STATUS\tNYPD::Open\t4000\n",
                "BOROUGH_CATEGORY\tBROOKLYN::Noise - Residential\t20000\n",
                "RES_AGENCY\tNYPD\t86000\t12.5000\t0.5208\t1075000.00\n",
                "RES_CATEGORY\tNoise - Residential\t48000\t4.2000\t0.1750\t201600.00\n",
                "RES_OVERALL\tALL\t95000\t11.8000\t0.4917\t1121000.00\n",
                "QUALITY\tTOTAL_RECORDS\t100000\n",
                "QUALITY\tOPEN_REQUESTS\t5000\n",
                "QUALITY\tCLOSED_WITH_VALID_DURATION\t95000\n"
            ]

            with open(mock_part_r, "w", encoding="utf-8") as f:
                f.writelines(lines)

            exec_summary = parse_mapreduce_output(mr_in, summary_out)

            # Assertions
            self.assertEqual(exec_summary["total_records_analyzed"], 100000)
            self.assertEqual(exec_summary["closed_requests_count"], 95000)
            self.assertEqual(exec_summary["open_requests_count"], 5000)
            self.assertEqual(exec_summary["resolution_rate_percent"], 95.0)
            self.assertEqual(exec_summary["top_complaint_type"], "Noise - Residential")
            self.assertEqual(exec_summary["top_agency"], "NYPD")
            self.assertEqual(exec_summary["top_borough"], "BROOKLYN")

            # Verify files were created
            expected_files = [
                "summary_categories.csv",
                "summary_agencies.csv",
                "summary_statuses.csv",
                "summary_monthly.csv",
                "summary_boroughs.csv",
                "summary_agency_status.csv",
                "summary_borough_category.csv",
                "summary_agency_resolution.csv",
                "summary_category_resolution.csv",
                "summary_executive.json",
                "summary_quality.json"
            ]
            for ef in expected_files:
                path = os.path.join(summary_out, ef)
                self.assertTrue(os.path.exists(path), f"Expected file {ef} was not generated")

            # Check contents of summary_categories.csv
            with open(os.path.join(summary_out, "summary_categories.csv"), "r", encoding="utf-8") as f:
                reader = list(csv.DictReader(f))
                self.assertEqual(len(reader), 3)
                self.assertEqual(reader[0]["complaint_type"], "Noise - Residential")
                self.assertEqual(reader[0]["count"], "50000")
                self.assertEqual(reader[0]["percentage"], "50.0")


if __name__ == "__main__":
    unittest.main()
