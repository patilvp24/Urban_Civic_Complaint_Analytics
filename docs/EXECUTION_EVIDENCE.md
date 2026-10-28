# Hadoop MapReduce Execution Evidence & Job Verification Log

This document records the exact command outputs, job identifiers, hardware counters, and checksum verification metrics from the genuine MapReduce execution on the 1,000,005-record NYC 311 dataset.

---

## 1. Environment & Stack Metadata
- **Host Operating System:** Windows 11 Home / Pro
- **Execution Subsystem:** Ubuntu 22.04 LTS (WSL2)
- **Java Runtime:** OpenJDK 11 (`11.0.32.1+1-post-1ubuntu1-22.04-Ubuntu`)
- **Hadoop Version:** Apache Hadoop 3.3.6 (Path: `/home/veetrag24/hadoop`)
- **MapReduce Framework:** Apache YARN with CapacityScheduler
- **Cluster Daemons:** NameNode, DataNode, SecondaryNameNode, ResourceManager, NodeManager

---

## 2. Real MapReduce Job Execution Details
- **Run Identifier:** `run_20261027_235453`
- **Hadoop Job ID:** `job_1793125477161_0001`
- **Application ID:** `application_1793125477161_0001`
- **HDFS Input Path:** `/civic311/input/cleaned/nyc_311_cleaned.tsv` (130,055,233 bytes, 1,000,006 lines)
- **HDFS Output Path:** `/civic311/output/run_20261027_235453`
- **Output Marker:** `/civic311/output/run_20261027_235453/_SUCCESS`
- **Output Data File:** `/civic311/output/run_20261027_235453/part-r-00000` (62,380 bytes)
- **Job Execution Duration:** **56.286 seconds** (59 seconds end-to-end pipeline)

---

## 3. Official Hadoop Job Counters (Verbatim from Cluster)

### Map-Reduce Framework Counters
```text
Map input records                 = 1,000,006 (1 header + 1,000,005 data records)
Map output records                = 12,959,850
Map output bytes                  = 513,858,295 bytes (~513.8 MB)
Map output materialized bytes     = 76,201 bytes
Input split bytes                 = 129 bytes
Combine input records             = 12,969,120
Combine output records            = 10,597 (99.92% shuffle data reduction)
Reduce input groups               = 1,327
Reduce shuffle bytes              = 76,201 bytes
Reduce input records              = 1,327
Reduce output records             = 1,327
CPU time spent                    = 44,790 ms
Total committed heap usage        = 418,381,824 bytes (~418 MB)
```

### Custom `UrbanAnalytics` Domain Counters
```text
TOTAL_INPUT_RECORDS               = 1,000,005
RESOLVED_VALID_DURATION           = 981,336
OPEN_REQUESTS                     = 8,839
INVERTED_TIMESTAMPS               = 9,830
```

### Exact Count Reconciliation
$$\text{Total Records} = \text{Resolved Valid} + \text{Open Requests} + \text{Inverted Timestamps}$$
$$1,000,005 = 981,336 + 8,839 + 9,830$$
$$\text{Reconciliation Error} = 0\text{ records (100.000\% perfect balance)}$$

---

## 4. Key Measured Analytics Summary

### Top 5 Complaint Categories
1. **Noise - Residential:** 136,228 records (13.62%)
2. **Request Large Bulky Item Collection:** 98,416 records (9.84%)
3. **HEAT/HOT WATER:** 82,580 records (8.26%)
4. **Illegal Parking:** 66,556 records (6.66%)
5. **Non-Emergency Police Matter:** 61,636 records (6.16%)

### Top 5 Responding Agencies & Response Speed
1. **NYPD (Police Department):** 407,464 requests (40.75%) | **Avg Resolution: 5.82 hours (0.24 days)**
2. **DSNY (Sanitation Department):** 163,805 requests (16.38%) | **Avg Resolution: 27.04 days**
3. **HPD (Housing Preservation):** 150,609 requests (15.06%) | **Avg Resolution: 16.72 days**
4. **DOT (Transportation):** 91,527 requests (9.15%) | **Avg Resolution: 18.49 days**
5. **DEP (Environmental Protection):** 51,335 requests (5.13%) | **Avg Resolution: 15.09 days**

### Borough Distribution
- **Brooklyn:** 300,033 (30.00%)
- **Queens:** 241,977 (24.20%)
- **Manhattan:** 196,327 (19.63%)
- **Bronx:** 194,372 (19.44%)
- **Staten Island:** 51,504 (5.15%)
- **Unspecified:** 15,792 (1.58%)
- **Total:** 1,000,005 records

---

## 5. Suggested Academic Screenshots Checklist
When submitting or presenting this project, capture screenshots of:
1. **Dashboard Executive Overview (`http://localhost:8501`):** Shows KPI cards (1,000,005 Records, 98.1% Resolution Rate, 30.6d Avg Resolution) and top category/status charts.
2. **Complaint Category Analytics View:** Interactive horizontal bar chart and Top-N slider.
3. **Agency Workload & Stacked Chart:** Shows NYPD, DSNY, HPD volume and stacked status composition.
4. **Monthly Trends Line Graph:** Interactive time-series showing monthly complaint fluctuations.
5. **Resolution Analytics View:** Side-by-side comparison of agency resolution durations.
6. **Hadoop & Pipeline Status Page:** Cluster metrics, run manifest, and interactive "Run MapReduce Analysis" execution log.
7. **Terminal / Shell Evidence:** Terminal showing `jps` with all 5 Hadoop daemons and successful execution output.
