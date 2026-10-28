# Phase 2 Verification: Data Cleaning, Normalization, and HDFS Staging

## 1. Overview and Environment Status
- **Operating Environment:** Ubuntu 22.04 on WSL2 (Host: Windows 11)
- **Java Version:** OpenJDK 11 (`11.0.32.1`)
- **Hadoop Version:** Apache Hadoop 3.3.6 (Path: `/home/veetrag24/hadoop`)
- **HDFS Status:** Active (NameNode, DataNode, SecondaryNameNode verified running via `jps`)
- **Local Project Path:** `C:\Users\veetrag\Desktop\B.Tech_IT\BDAL\Urban_Civic_Complaint_Analytics`
- **WSL2 Mount Path:** `/mnt/c/Users/veetrag/Desktop/B.Tech_IT/BDAL\Urban_Civic_Complaint_Analytics`

---

## 2. Source Snapshot Verification
Before processing, the Phase 1 raw snapshot was verified:
- **Raw File:** `data/raw/nyc_311_snapshot_1M.csv`
- **Header Columns (10):** `unique_key,created_date,closed_date,agency,complaint_type,descriptor,status,borough,latitude,longitude`
- **Verified Record Count:** 1,000,005 data rows (+ 1 header row)
- **Distinct Unique Keys:** 1,000,005 (0 duplicates)
- **SHA-256 Checksum:** `a6b3313d2ede84c4436b0fba056ab7eeb637be64a1a8df027fa0f41e12a38c2c`
- **File Size:** 173,267,591 bytes (165.24 MB)

---

## 3. Data Cleaning and Normalization Execution
Executed streaming preprocessing script `scripts/clean_and_validate.py`:

```powershell
py scripts/clean_and_validate.py
```

### Sanitization and Transformation Applied
1. **Timestamp Normalization:** All `created_date` and `closed_date` timestamps converted from Socrata ISO-8601 (`YYYY-MM-DDTHH:MM:SS.000`) to SQL/Hadoop standard `YYYY-MM-DD HH:MM:SS`.
2. **Open Request Preservation:** 8,839 actively open requests without a `closed_date` were preserved with an empty string field.
3. **Delimiter Overhead Reduction & Single-Line Guarantee:** Output generated as Tab-Separated Values (TSV). Any tabs (`\t`) or newline characters (`\r`, `\n`) embedded within complaint text were stripped and replaced with spaces.
4. **Borough Normalization:** Inconsistent casing and empty borough entries (15,792 records) standardized to `"UNSPECIFIED"`.
5. **Coordinate Validation:** Latitude and longitude verified as floating-point numbers within global boundaries ($[-90, 90]$ and $[-180, 180]$). 32,228 records without GPS coordinates were preserved with empty strings.
6. **Inverted Timestamps Audited:** 9,830 records with `closed_date < created_date` (upstream clerical errors) were retained in the dataset, with their anomalies logged for resolution-time job handling.

### Cleaning Output Metrics
- **Cleaned File:** `data/cleaned/nyc_311_cleaned.tsv`
- **Metadata Output:** `data/metadata/cleaning_metadata.json`
- **Cleaned Rows Staged:** 1,000,005
- **Excluded Rows:** 0
- **Cleaned File Size:** 130,055,233 bytes (124.03 MB)
- **SHA-256 Checksum:** `509e5b4dc31c85c548a215ba3bb5967931271db41ac7a7e8b8fc1b6e2c23260a`
- **Processing Time:** 34.47 seconds (streaming)

---

## 4. Automated Testing
Unit tests were executed via Python's standard test runner:

```powershell
py -m unittest discover tests
```

### Test Suite Results:
- `tests/test_downloader_config.py` (4 tests) - **ALL PASSED**
- `tests/test_live_api_probe.py` (1 test) - **PASSED**
- `tests/test_clean_and_validate.py` (5 tests) - **ALL PASSED**
  - `test_sanitize_text`: Validated tab and newline removal.
  - `test_timestamp_normalization`: Tested standard, missing, and malformed dates.
  - `test_coordinate_validation`: Tested coordinate bounds and non-numeric strings.
  - `test_normalize_borough`: Tested borough string standardization.
  - `test_streaming_cleaner_integration`: End-to-end integration test with duplicate IDs and corrupt dates.

**Total Tests Run:** 10 | **Passing:** 10 | **Failures:** 0

---

## 5. HDFS Staging Verification

### Pre-Staging Isolation Audit
Inspected HDFS root directory before creating any paths:
```text
Found 4 items
drwxr-xr-x   - veetrag24 supergroup          0 2026-10-21 09:23 /assignment
drwxr-xr-x   - veetrag24 supergroup          0 2026-10-27 14:50 /household_power
drwxr-xr-x   - veetrag24 supergroup          0 2026-10-21 10:41 /tmp
drwxr-xr-x   - veetrag24 supergroup          0 2026-10-21 10:55 /wordcount
```
*Confirmed `/household_power` was preserved without any interference or changes.*

### Dedicated Namespace Creation & Upload
Created dedicated `/civic311` namespace:
```bash
hdfs dfs -mkdir -p /civic311/input/raw /civic311/input/cleaned
hdfs dfs -put /mnt/c/Users/veetrag/Desktop/B.Tech_IT/BDAL/Urban_Civic_Complaint_Analytics/data/raw/nyc_311_snapshot_1M.csv /civic311/input/raw/
hdfs dfs -put /mnt/c/Users/veetrag/Desktop/B.Tech_IT/BDAL/Urban_Civic_Complaint_Analytics/data/cleaned/nyc_311_cleaned.tsv /civic311/input/cleaned/
```

### Staged File Verification on HDFS
```text
/home/veetrag24/hadoop/bin/hdfs dfs -ls -R /civic311/input
drwxr-xr-x   - veetrag24 supergroup          0 2026-10-27 23:37 /civic311/input/cleaned
-rw-r--r--   1 veetrag24 supergroup  130055233 2026-10-27 23:37 /civic311/input/cleaned/nyc_311_cleaned.tsv
drwxr-xr-x   - veetrag24 supergroup          0 2026-10-27 23:36 /civic311/input/raw
-rw-r--r--   1 veetrag24 supergroup  173267591 2026-10-27 23:36 /civic311/input/raw/nyc_311_snapshot_1M.csv
```

### Staged Record Count Verification on HDFS
```bash
/home/veetrag24/hadoop/bin/hdfs dfs -cat /civic311/input/raw/nyc_311_snapshot_1M.csv | wc -l
# Result: 1000006 (1 header + 1,000,005 data records)

/home/veetrag24/hadoop/bin/hdfs dfs -cat /civic311/input/cleaned/nyc_311_cleaned.tsv | wc -l
# Result: 1000006 (1 header + 1,000,005 data records)
```

---

## 6. Known Limitations & MapReduce Preconditions
1. **Open Tickets:** 8,839 records have no `closed_date`. MapReduce jobs calculating resolution times must filter out records where `closed_date` is empty.
2. **Inverted Timestamps:** 9,830 records have `closed_date < created_date` due to municipal backdating. MapReduce jobs must check that `duration_hours >= 0` before aggregating resolution times.
3. **Delimiter Configuration:** Cleaned data uses `\t` (tab) delimiters; MapReduce mappers must split records with `\t` rather than commas to avoid issues with complaint descriptors.
