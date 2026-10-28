# NYC 311 Dataset Data Quality & Sanitization Audit

## 1. Executive Summary

An audit was conducted on the raw 1,000,005-record snapshot acquired from NYC Open Data (`erm2-nwe9`). The data cleaning process applied deterministic normalization rules without discarding valid civic complaints.

| Metric | Raw Input (`.csv`) | Cleaned Output (`.tsv`) | Notes |
| :--- | :--- | :--- | :--- |
| **Record Count** | 1,000,005 | 1,000,005 | 100% records preserved |
| **Distinct Unique Keys** | 1,000,005 | 1,000,005 | Verified zero duplicates |
| **File Size** | 165.24 MB | 124.03 MB | TSV formatting reduces delimiter overhead |
| **Format** | Quoted CSV | Tab-Separated Values (TSV) | Embedded tabs/newlines removed |
| **SHA-256 Checksum** | `a6b3313d...` | `509e5b4d...` | Fixed cryptographic signatures |

---

## 2. Field-by-Field Sanitization Rules

1. **`unique_key`**: Validated as numeric string. Preserved as primary partition key.
2. **`created_date`**: Normalized from ISO-8601 (`YYYY-MM-DDTHH:MM:SS.000`) to SQL-standard `YYYY-MM-DD HH:MM:SS`. 100% valid.
3. **`closed_date`**: Normalized when present. Missing values (`8,839` records, 0.88%) indicate actively open service requests and are preserved as empty string `""`.
4. **`agency` & `complaint_type`**: Stripped of superfluous whitespace. Missing values defaulted to `"UNKNOWN"`.
5. **`descriptor`**: Sanitized against embedded tabs and newlines to prevent column shifting in MapReduce. 1 record missing descriptor normalized to `"N/A"`.
6. **`status`**: Preserved in title case (`Closed`, `Open`, `In Progress`, `Assigned`, etc.).
7. **`borough`**: Normalized to uppercase (`BROOKLYN`, `MANHATTAN`, `QUEENS`, `BRONX`, `STATEN ISLAND`). 15,792 records with missing or unspecified borough standardized to `"UNSPECIFIED"`.
8. **`latitude` & `longitude`**: Validated within global geographical bounds ($[-90, 90]$ and $[-180, 180]$). 32,228 records (3.22%) with missing GPS coordinates preserved as empty strings.

---

## 3. Data Anomalies Documented

* **Inverted Timestamps (`9,830` records, 0.98%):** Records where `closed_date < created_date` in the upstream city agency database (frequently due to manual clerical corrections or backdated entries). These records represent real service complaints and are **retained** in the dataset; however, the resolution time MapReduce job will explicitly skip calculating duration for these anomalous records to avoid negative elapsed times.
