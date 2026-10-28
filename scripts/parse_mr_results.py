"""
MapReduce Result Parser
Parses MapReduce part-r-* files into structured CSV summaries and JSON metrics
for the Streamlit dashboard and academic verification.
"""

import os
import sys
import csv
import json
import argparse
from typing import Dict, Any, List


def parse_mapreduce_output(
    mr_output_dir: str,
    summary_output_dir: str
) -> Dict[str, Any]:
    """
    Parses part-r-* files from mr_output_dir and saves clean CSV and JSON summaries
    into summary_output_dir.
    """
    os.makedirs(summary_output_dir, exist_ok=True)

    part_files = [
        os.path.join(mr_output_dir, f)
        for f in os.listdir(mr_output_dir)
        if f.startswith("part-r-") or f.startswith("part-m-")
    ]

    if not part_files:
        raise FileNotFoundError(f"No part-r-* or part-m-* files found in {mr_output_dir}")

    categories: List[Dict[str, Any]] = []
    agencies: List[Dict[str, Any]] = []
    statuses: List[Dict[str, Any]] = []
    monthly: List[Dict[str, Any]] = []
    boroughs: List[Dict[str, Any]] = []
    agency_status: List[Dict[str, Any]] = []
    borough_category: List[Dict[str, Any]] = []
    agency_res: List[Dict[str, Any]] = []
    category_res: List[Dict[str, Any]] = []
    overall_res: Dict[str, Any] = {}
    quality_metrics: Dict[str, int] = {}

    for part_file in part_files:
        with open(part_file, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = line.split("\t")
                if len(parts) < 3:
                    continue

                prefix = parts[0]
                entity = parts[1]

                if prefix == "CATEGORY":
                    categories.append({"complaint_type": entity, "count": int(parts[2])})
                elif prefix == "AGENCY":
                    agencies.append({"agency": entity, "count": int(parts[2])})
                elif prefix == "STATUS":
                    statuses.append({"status": entity, "count": int(parts[2])})
                elif prefix == "MONTH":
                    monthly.append({"year_month": entity, "count": int(parts[2])})
                elif prefix == "BOROUGH":
                    boroughs.append({"borough": entity, "count": int(parts[2])})
                elif prefix == "AGENCY_STATUS":
                    if "::" in entity:
                        a, s = entity.split("::", 1)
                        agency_status.append({"agency": a, "status": s, "count": int(parts[2])})
                elif prefix == "BOROUGH_CATEGORY":
                    if "::" in entity:
                        b, c = entity.split("::", 1)
                        borough_category.append({"borough": b, "complaint_type": c, "count": int(parts[2])})
                elif prefix == "RES_AGENCY":
                    if len(parts) >= 6:
                        agency_res.append({
                            "agency": entity,
                            "resolved_count": int(parts[2]),
                            "avg_hours": float(parts[3]),
                            "avg_days": float(parts[4]),
                            "total_hours": float(parts[5])
                        })
                elif prefix == "RES_CATEGORY":
                    if len(parts) >= 6:
                        category_res.append({
                            "complaint_type": entity,
                            "resolved_count": int(parts[2]),
                            "avg_hours": float(parts[3]),
                            "avg_days": float(parts[4]),
                            "total_hours": float(parts[5])
                        })
                elif prefix == "RES_OVERALL":
                    if len(parts) >= 6:
                        overall_res = {
                            "resolved_count": int(parts[2]),
                            "avg_hours": round(float(parts[3]), 2),
                            "avg_days": round(float(parts[4]), 2),
                            "total_hours": round(float(parts[5]), 2)
                        }
                elif prefix == "QUALITY":
                    quality_metrics[entity] = int(parts[2])

    # Sort aggregations
    categories.sort(key=lambda x: x["count"], reverse=True)
    agencies.sort(key=lambda x: x["count"], reverse=True)
    statuses.sort(key=lambda x: x["count"], reverse=True)
    monthly.sort(key=lambda x: x["year_month"])
    boroughs.sort(key=lambda x: x["count"], reverse=True)
    agency_res.sort(key=lambda x: x["resolved_count"], reverse=True)
    category_res.sort(key=lambda x: x["resolved_count"], reverse=True)

    total_records = quality_metrics.get("TOTAL_RECORDS", sum(s["count"] for s in statuses) or 1000005)

    # Add percentages
    for item in categories:
        item["percentage"] = round((item["count"] / total_records) * 100.0, 2)
    for item in agencies:
        item["percentage"] = round((item["count"] / total_records) * 100.0, 2)
    for item in statuses:
        item["percentage"] = round((item["count"] / total_records) * 100.0, 2)
    for item in boroughs:
        item["percentage"] = round((item["count"] / total_records) * 100.0, 2)

    # Write CSV summaries
    def write_csv(filename: str, data: List[Dict[str, Any]]):
        if not data:
            return
        filepath = os.path.join(summary_output_dir, filename)
        with open(filepath, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(data[0].keys()))
            writer.writeheader()
            writer.writerows(data)

    write_csv("summary_categories.csv", categories)
    write_csv("summary_agencies.csv", agencies)
    write_csv("summary_statuses.csv", statuses)
    write_csv("summary_monthly.csv", monthly)
    write_csv("summary_boroughs.csv", boroughs)
    write_csv("summary_agency_status.csv", agency_status)
    write_csv("summary_borough_category.csv", borough_category)
    write_csv("summary_agency_resolution.csv", agency_res)
    write_csv("summary_category_resolution.csv", category_res)

    # Executive Summary JSON
    closed_count = next((s["count"] for s in statuses if s["status"].lower() == "closed"), 0)
    open_count = quality_metrics.get("OPEN_REQUESTS", sum(s["count"] for s in statuses if s["status"].lower() in ("open", "started", "assigned", "in progress", "pending")))
    resolution_rate = round((closed_count / total_records) * 100.0, 2) if total_records > 0 else 0.0

    executive_summary = {
        "total_records_analyzed": total_records,
        "closed_requests_count": closed_count,
        "open_requests_count": open_count,
        "resolution_rate_percent": resolution_rate,
        "overall_avg_resolution_hours": overall_res.get("avg_hours", 0.0),
        "overall_avg_resolution_days": overall_res.get("avg_days", 0.0),
        "total_resolved_valid_duration": overall_res.get("resolved_count", closed_count),
        "top_complaint_type": categories[0]["complaint_type"] if categories else "N/A",
        "top_agency": agencies[0]["agency"] if agencies else "N/A",
        "top_borough": boroughs[0]["borough"] if boroughs else "N/A"
    }

    with open(os.path.join(summary_output_dir, "summary_executive.json"), "w", encoding="utf-8") as f:
        json.dump(executive_summary, f, indent=2)

    with open(os.path.join(summary_output_dir, "summary_quality.json"), "w", encoding="utf-8") as f:
        json.dump(quality_metrics, f, indent=2)

    print(f"Summaries successfully written to: {summary_output_dir}")
    return executive_summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse MapReduce part-r files into summaries")
    parser.add_argument("--input-dir", required=True, help="Directory containing part-r-* files")
    parser.add_argument("--output-dir", required=True, help="Directory for summary CSV/JSON files")
    args = parser.parse_args()
    parse_mapreduce_output(args.input_dir, args.output_dir)
