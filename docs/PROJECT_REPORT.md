# Urban Civic Complaint and Service Resolution Analytics Using Hadoop MapReduce

**Course:** Big Data Analytics Lab (BDAL)  
**Degree:** Bachelor of Technology in Information Technology (B.Tech IT)  
**Academic Year:** 2026–2027  

---

### Student & Project Credentials
* **Student Name:** `[Student Name Placeholder]`
* **Permanent Registration Number (PRN):** `[PRN Placeholder]`
* **Project Guide:** `[Guide Name Placeholder]`
* **Department:** Department of Information Technology  
* **Institution:** `[Institution Name Placeholder]`  

---

## Abstract
Rapid urbanization has dramatically increased the volume and diversity of municipal service requests submitted to municipal 311 hotlines. Traditional relational databases struggle to analyze high-throughput civic complaint streams with operational responsiveness. This project designs and implements an enterprise-grade Big Data analytics architecture using **Apache Hadoop MapReduce 3.3.6** and an interactive, dark-themed **Streamlit web dashboard** to process a fixed, cryptographically verified snapshot of **1,000,005 real civic complaints** from the official New York City Open Data repository (dataset ID: `erm2-nwe9`). The data pipeline applies deterministic streaming sanitization, stages normalized tab-separated values into the Hadoop Distributed File System (HDFS), and executes a composite Java 11 MapReduce job that aggregates multi-dimensional complaint patterns, agency response velocities, status lifecycles, and borough distributions in **56.28 seconds**. The resulting intelligence is surfaced through an interactive, responsive local web application with dynamic Plotly visualizations and native WSL2 execution triggering.

---

## 1. Introduction & Problem Statement

### 1.1 Background
The New York City 311 service represents the most comprehensive non-emergency government contact system in the United States, receiving millions of complaints annually regarding residential noise, infrastructure degradation, sanitation, and heating emergencies. Municipal leaders require timely, granular visibility into these service requests to optimize budget allocation, deploy field technicians, and hold agencies accountable.

### 1.2 Problem Statement
Extracting actionable intelligence from over 1 million multi-attribute civic records presents several Big Data challenges:
1. **Memory & Processing Limits:** Querying millions of records with varying descriptors and timestamps in conventional tools causes memory overflows or prohibitive execution times.
2. **Data Hygiene & Anomalies:** Real-world municipal streams contain actively open complaints lacking closure dates ($8,839$ records) and clerical backdating errors where closure precedes creation ($9,830$ records). Treating missing closure timestamps as zero-duration resolutions fatally corrupts performance metrics.
3. **Operational Reproducibility:** Live API endpoints suffer from transient network outages, rate limits, and mutating data, necessitating a cryptographically verified, immutable snapshot architecture.

### 1.3 Project Objectives
- Acquire a verified, reproducible snapshot of at least $1,000,001$ distinct real records from NYC Open Data (`erm2-nwe9`).
- Implement streaming data preprocessing to clean strings, normalize timestamps to SQL standards, and enforce TSV single-line guarantees.
- Stage raw and cleaned datasets into dedicated HDFS namespaces (`/civic311/input/*`).
- Develop a high-performance Java 11 MapReduce job leveraging a custom `MetricWritable` and Combiner optimization to compute complaint frequencies, agency workloads, and mean resolution durations in a single distributed pass.
- Construct a dark-themed Streamlit web dashboard featuring 10 analytical sections, dynamic Plotly charts, CSV downloads, and a live "Run MapReduce Analysis" execution trigger.

---

## 2. Dataset Architecture & Verification

### 2.1 Source Data
* **Repository:** NYC Open Data (City of New York)
* **Dataset Name:** 311 Service Requests from 2020 to Present
* **Dataset Identifier:** `erm2-nwe9`
* **Acquisition Protocol:** 21 chunked SODA API queries of up to 50,000 records using stable ordering (`$order=unique_key ASC`).

### 2.2 Schema Definition
| Field Name | Type | Description |
| :--- | :--- | :--- |
| `unique_key` | Long / Numeric String | Primary unique identifier for each service request |
| `created_date` | Timestamp | Date and time the complaint was lodged (`YYYY-MM-DD HH:MM:SS`) |
| `closed_date` | Timestamp (Nullable) | Date and time resolved (`YYYY-MM-DD HH:MM:SS` or empty for open tickets) |
| `agency` | String | Municipal responding agency code (e.g., NYPD, DSNY, HPD, DOT, DEP) |
| `complaint_type` | String | High-level problem category (e.g., Noise - Residential, Illegal Parking) |
| `descriptor` | String | Detailed description of the specific issue (sanitized to remove tabs/newlines) |
| `status` | String | Current status (`Closed`, `Open`, `In Progress`, `Pending`, `Assigned`, `Started`) |
| `borough` | String | NYC borough (`BROOKLYN`, `QUEENS`, `MANHATTAN`, `BRONX`, `STATEN ISLAND`, `UNSPECIFIED`) |
| `latitude` | Float (Nullable) | Geographic GPS latitude coordinate |
| `longitude` | Float (Nullable) | Geographic GPS longitude coordinate |

### 2.3 Cryptographic Integrity Audit
- **Raw CSV File:** `data/raw/nyc_311_snapshot_1M.csv` (173,267,591 bytes, 165.24 MB)
- **Raw SHA-256 Checksum:** `a6b3313d2ede84c4436b0fba056ab7eeb637be64a1a8df027fa0f41e12a38c2c`
- **Cleaned TSV File:** `data/cleaned/nyc_311_cleaned.tsv` (130,055,233 bytes, 124.03 MB)
- **Cleaned SHA-256 Checksum:** `509e5b4dc31c85c548a215ba3bb5967931271db41ac7a7e8b8fc1b6e2c23260a`
- **Verified Record Count:** Exactly $1,000,005$ data records ($0$ duplicates).

---

## 3. Big Data System Architecture

```text
+-------------------------------------------------------------+
|                      NYC OPEN DATA                          |
|         311 Service Requests (Dataset ID: erm2-nwe9)        |
+-------------------------------------------------------------+
                               |
                               | (SODA API Streaming Chunked Ingestion)
                               v
+-------------------------------------------------------------+
|             LOCAL IMMUTABLE SNAPSHOT STORAGE                |
|      data/raw/nyc_311_snapshot_1M.csv (1,000,005 Records)   |
|      data/metadata/dataset_metadata.json (SHA-256 Verified) |
+-------------------------------------------------------------+
                               |
                               | (Streaming Python CSV Reader & TSV Normalizer)
                               v
+-------------------------------------------------------------+
|               DATA SANITIZATION & STAGING                   |
|   • Timestamp normalization to YYYY-MM-DD HH:MM:SS          |
|   • Tab/Newline text sanitization for TSV integrity         |
|   • Open ticket preservation (8,839 records)                |
|   • Inverted timestamp auditing (9,830 records)             |
+-------------------------------------------------------------+
                               |
                               | (hdfs dfs -put)
                               v
+-------------------------------------------------------------+
|              HDFS STORAGE (HADOOP 3.3.6 / WSL2)             |
|   • /civic311/input/raw/nyc_311_snapshot_1M.csv             |
|   • /civic311/input/cleaned/nyc_311_cleaned.tsv             |
+-------------------------------------------------------------+
                               |
                               | (TextInputFormat Splits)
                               v
+-------------------------------------------------------------+
|             HADOOP MAPREDUCE ANALYTICS ENGINE               |
|                                                             |
|  UrbanCivicMapper                                           |
|   ├── Composite Keys: CATEGORY, AGENCY, STATUS, BOROUGH     |
|   ├── Resolution Duration: RES_AGENCY, RES_CATEGORY         |
|   └── Custom Value: MetricWritable (count, sumDurationHours)|
|                                                             |
|  UrbanCivicCombiner (Map-Side Mini-Reducer)                 |
|   └── Reduces intermediate records by 99.92% (12.9M -> 10.5K)|
|                                                             |
|  UrbanCivicReducer                                          |
|   ├── Sums counts & aggregates durations                    |
|   └── Computes mean resolution hours and days               |
+-------------------------------------------------------------+
                               |
                               | (hdfs dfs -get part-r-* & parse_mr_results.py)
                               v
+-------------------------------------------------------------+
|              STRUCTURED SUMMARY REPOSITORY                  |
|   • summary_executive.json     • summary_categories.csv     |
|   • summary_agencies.csv       • summary_statuses.csv       |
|   • summary_monthly.csv        • summary_boroughs.csv       |
|   • summary_agency_resolution  • summary_quality.json       |
+-------------------------------------------------------------+
                               |
                               v
+-------------------------------------------------------------+
|            INTERACTIVE STREAMLIT WEB DASHBOARD              |
|   • 10 Executive Analytics Sections                         |
|   • Dynamic Plotly Dark Charts & Downloadable CSV Tables    |
|   • Live MapReduce Execution Trigger via WSL Subprocess     |
|   • Localhost Web Access: http://localhost:8501             |
+-------------------------------------------------------------+
```

---

## 4. MapReduce Algorithm & Optimization

### 4.1 Custom Metric Serialization (`MetricWritable.java`)
Traditional MapReduce requires multiple passes to calculate averages or uses inefficient string concatenations. We implement a binary Hadoop `Writable`:
```java
public class MetricWritable implements Writable {
    private long count;
    private double sumDurationHours;
    // Implements write(DataOutput), readFields(DataInput), and add(MetricWritable)
}
```

### 4.2 Single-Pass Multi-Dimensional Mapping (`UrbanCivicMapper.java`)
The Mapper inspects each cleaned record and emits multiple composite key-value pairs simultaneously:
- `CATEGORY\t<complaint_type>` $\rightarrow (1, 0.0)$
- `AGENCY\t<agency>` $\rightarrow (1, 0.0)$
- `STATUS\t<status>` $\rightarrow (1, 0.0)$
- `MONTH\t<YYYY-MM>` $\rightarrow (1, 0.0)$
- `BOROUGH\t<borough>` $\rightarrow (1, 0.0)$
- If closed and $\Delta t \ge 0$:
  - `RES_AGENCY\t<agency>` $\rightarrow (1, \Delta t)$
  - `RES_CATEGORY\t<complaint_type>` $\rightarrow (1, \Delta t)$
  - `RES_OVERALL\tALL` $\rightarrow (1, \Delta t)$

### 4.3 Combiner Data Reduction (`UrbanCivicCombiner.java`)
Because both count addition ($\sum c$) and duration summation ($\sum \Delta t$) are associative and commutative:
$$\text{Combiner Output} = \left(\sum c_i, \sum \Delta t_i\right)$$
In our execution, the Combiner processed **12,969,120 intermediate records** and emitted only **10,597 records** to the network shuffle phase, eliminating $99.92\%$ of intermediate disk and network transfer.

---

## 5. Measured Analytics Results

### 5.1 Executive Performance Metrics
- **Total Records Analyzed:** $1,000,005$
- **Closed Complaints:** $981,115$
- **Open Backlog:** $8,839$
- **Overall Resolution Rate:** **$98.11\%$**
- **Overall Mean Resolution Time:** **$30.59\text{ days}$** ($734.1\text{ hours}$)

### 5.2 Top 10 Civic Complaint Categories
| Rank | Complaint Type | Total Requests | Percentage Share |
| :---: | :--- | :---: | :---: |
| 1 | **Noise - Residential** | 136,228 | 13.62% |
| 2 | **Request Large Bulky Item Collection** | 98,416 | 9.84% |
| 3 | **HEAT/HOT WATER** | 82,580 | 8.26% |
| 4 | **Illegal Parking** | 66,556 | 6.66% |
| 5 | **Non-Emergency Police Matter** | 61,636 | 6.16% |
| 6 | **Noise - Street/Sidewalk** | 41,906 | 4.19% |
| 7 | **Blocked Driveway** | 39,310 | 3.93% |
| 8 | **Street Light Condition** | 29,516 | 2.95% |
| 9 | **Street Condition** | 26,153 | 2.62% |
| 10 | **Consumer Complaint** | 23,046 | 2.30% |

### 5.3 Agency Workload and Response Velocities
| Agency | Total Requests | Share (%) | Avg Resolution (Days) | Avg Resolution (Hours) | Primary Role |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **NYPD** | 407,464 | 40.75% | **0.24** | **5.82** | Police & Quality-of-Life |
| **DSNY** | 163,805 | 16.38% | 27.04 | 648.89 | Sanitation & Bulk Pickup |
| **HPD** | 150,609 | 15.06% | 16.72 | 401.32 | Housing & Heat Complaints |
| **DOT** | 91,527 | 9.15% | 18.49 | 443.67 | Street & Traffic Infrastructure |
| **DEP** | 51,335 | 5.13% | 15.09 | 362.05 | Environmental & Water |
| **DOB** | 37,908 | 3.79% | 38.38 | 921.23 | Buildings & Construction |
| **DPR** | 28,449 | 2.84% | 330.85 | 7,940.46 | Parks & Recreation |
| **DCA** | 23,046 | 2.30% | 28.68 | 688.37 | Consumer Affairs |
| **DOHMH**| 22,292 | 2.23% | 490.63 | 11,775.03 | Health & Hygiene |

*Finding:* NYPD handles over 40% of all inquiries and exhibits the fastest response time ($5.8$ hours) due to active patrol dispatch, whereas DPR and DOHMH handle complex long-term inspections requiring weeks to months.

### 5.4 Borough Distribution
| Borough | Complaints | Share (%) | Primary Complaint |
| :--- | :---: | :---: | :--- |
| **Brooklyn** | 300,033 | 30.00% | Noise - Residential |
| **Queens** | 241,977 | 24.20% | Request Large Bulky Item Collection |
| **Manhattan** | 196,327 | 19.63% | Noise - Residential |
| **Bronx** | 194,372 | 19.44% | HEAT/HOT WATER |
| **Staten Island** | 51,504 | 5.15% | Request Large Bulky Item Collection |
| **Unspecified** | 15,792 | 1.58% | Non-Emergency Police Matter |
| **Total** | **1,000,005** | **100.00%** | — |

---

## 6. Verification and Automated Testing

An automated test suite comprising 12 unit and integration tests was implemented and executed:
```text
Ran 12 tests in 2.994s - ALL PASSED (OK)
```
1. `test_required_columns_present`: Validates all 10 schema fields.
2. `test_query_params_builder`: Tests SODA API query construction.
3. `test_sha256_computation`: Tests streaming cryptographic hashing.
4. `test_verify_existing_snapshot_validation`: Validates detection of corrupt snapshots.
5. `test_official_endpoint_probe`: Performs live HTTP probe against official SODA API (`erm2-nwe9`).
6. `test_sanitize_text`: Validates removal of embedded tabs and newlines.
7. `test_timestamp_normalization`: Validates parsing of ISO, missing, and malformed dates.
8. `test_coordinate_validation`: Validates latitude/longitude geographical boundaries.
9. `test_normalize_borough`: Tests borough uppercase standardization.
10. `test_streaming_cleaner_integration`: End-to-end integration test with duplicate IDs.
11. `test_parse_mapreduce_output_mock`: Validates summary CSV/JSON generation from `part-r-*` records.
12. `test_load_latest_analytics`: Verifies dashboard data loader against live generated summaries.

---

## 7. Limitations & Future Scope

### 7.1 Limitations
1. **Single-Node Pseudo-Distributed Execution:** Hadoop was executed on a pseudo-distributed single-node cluster on WSL2; multi-node physical cluster scaling would further parallelize execution.
2. **Clerical Data Errors:** Approximately $0.98\%$ of upstream records contain backdated closure timestamps, which had to be excluded from resolution duration metrics.

### 7.2 Future Scope
1. **Predictive Resolution Modeling:** Integrating Apache Spark MLlib or PyTorch to predict expected resolution durations based on borough and agency workload.
2. **Live Geospatial Heatmaps:** Streaming spatial geometries to render interactive Kepler.gl or Deck.gl heatmaps.

---

## 8. Conclusion
This project successfully designed, implemented, and verified a complete Big Data pipeline for NYC 311 civic service complaint analytics. Using real data, native Apache Hadoop MapReduce on WSL2, and Streamlit, the system processed $1,000,005$ records in under a minute and provided clear municipal performance insights through a dark-themed interactive web dashboard.

---

## References
1. Apache Software Foundation. *Apache Hadoop 3.3.6 Documentation & MapReduce Tutorial*. https://hadoop.apache.org/
2. City of New York. *311 Service Requests from 2020 to Present*. NYC Open Data. Dataset ID: `erm2-nwe9`.
3. White, Tom. *Hadoop: The Definitive Guide (4th Edition)*. O'Reilly Media, 2015.
4. Streamlit Inc. *Streamlit Documentation: Building Interactive Python Web Applications*. https://docs.streamlit.io/
5. Plotly Technologies. *Plotly Python Open Source Graphing Library*. https://plotly.com/python/
