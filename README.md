# Urban Civic Complaint and Service Resolution Analytics Using Hadoop MapReduce

[![Hadoop](https://img.shields.io/badge/Apache%20Hadoop-3.3.6-red?logo=apachehadoop)](https://hadoop.apache.org/)
[![Java](https://img.shields.io/badge/Java-OpenJDK%2011-orange?logo=openjdk)](https://openjdk.org/)
[![Python](https://img.shields.io/badge/Python-3.14%20%7C%203.10-blue?logo=python)](https://python.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.66.1-FF4B4B?logo=streamlit)](https://streamlit.io/)
[![Dataset](https://img.shields.io/badge/NYC%20311-1%2C000%2C005%20Records-brightgreen)](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2020-to-Present/erm2-nwe9)

An end-to-end Big Data Analytics mini-project analyzing over **1,000,000 real-world civic complaint records** from the official **NYC Open Data 311 Service Requests** repository (dataset ID: `erm2-nwe9`) using **Apache Hadoop MapReduce 3.3.6 on Ubuntu 22.04 WSL2** and a responsive, dark-themed **Streamlit web dashboard**.

---

## 🚀 Quick Start & Local Launch

### Option 1: Direct Windows Launch (Recommended)
Double-click [`RUN_DASHBOARD.bat`](file:///C:/Users/veetrag/Desktop/B.Tech_IT/BDAL/Urban_Civic_Complaint_Analytics/RUN_DASHBOARD.bat) or execute in PowerShell:
```powershell
.\RUN_DASHBOARD.bat
```
This automatically starts the local web dashboard and opens:
**👉 http://localhost:8501**

### Option 2: WSL2 Terminal Launch
```bash
bash scripts/run_dashboard_wsl.sh
```

### Option 3: Manual Command Line
```powershell
.\.venv\Scripts\streamlit run dashboard/app.py --server.port 8501
```

---

## 📊 Live MapReduce Analytics Summary (Real Execution)

| Metric | Measured Value | Notes |
| :--- | :--- | :--- |
| **Total Records Analyzed** | **1,000,005** | 100% verified real 311 records |
| **Hadoop Job ID** | `job_1793125477161_0001` | Genuine MapReduce cluster execution |
| **Job Execution Time** | **56.28 seconds** | Complete cluster runtime |
| **Overall Resolution Rate** | **98.11%** | 981,115 resolved complaints |
| **Mean Resolution Duration** | **30.59 days** (734.1 hours) | Excludes active open tickets |
| **Top Complaint Category** | **Noise - Residential** | 136,228 records (13.62%) |
| **Top Responding Agency** | **NYPD** (Police Dept) | 407,464 records (40.75%) |
| **Fastest Agency Response** | **NYPD** | **5.82 hours** mean response |
| **Most Impacted Borough** | **Brooklyn** | 300,033 records (30.00%) |

---

## 📁 Project Directory Structure

```text
Urban_Civic_Complaint_Analytics/
├── RUN_DASHBOARD.bat                  # One-click Windows dashboard launcher
├── README.md                          # Project documentation and quick start
├── requirements.txt                   # Python dependencies (Streamlit, Plotly, Pandas)
├── .gitignore                         # Excludes large CSVs and binaries
├── data/
│   ├── raw/
│   │   └── nyc_311_snapshot_1M.csv    # Immutable raw snapshot (165.24 MB, 1,000,005 rows)
│   ├── cleaned/
│   │   └── nyc_311_cleaned.tsv        # Normalized TSV (124.03 MB, staged in HDFS)
│   ├── metadata/
│   │   ├── dataset_metadata.json      # Cryptographic snapshot audit (SHA-256)
│   │   └── cleaning_metadata.json     # Sanitization metrics and anomaly logs
│   └── output/
│       ├── latest/                    # Current active summaries loaded by dashboard
│       └── run_20261027_235453/       # Real MapReduce outputs (part-r-00000) & manifest
├── hadoop/
│   ├── urban-civic-analytics.jar      # Compiled Java 11 MapReduce binary (12 KB)
│   └── src/main/java/org/urban/analytics/
│       ├── MetricWritable.java        # Custom binary Hadoop Writable (count, duration)
│       ├── UrbanCivicMapper.java      # Single-pass multi-dimensional mapper
│       ├── UrbanCivicCombiner.java    # Map-side mini-reducer (99.92% shuffle reduction)
│       ├── UrbanCivicReducer.java     # Reducer computing counts, means, and rates
│       └── UrbanCivicAnalyticsDriver.java # Job orchestrator and configuration
├── scripts/
│   ├── download_snapshot.py           # Chunked SODA API downloader with deduplication
│   ├── verify_snapshot.py             # Independent SHA-256 integrity verifier
│   ├── clean_and_validate.py          # Streaming normalizer & data quality auditor
│   ├── compile_hadoop_jar.sh          # Compiles Java 11 against Hadoop 3.3.6 classpath
│   ├── run_hadoop_pipeline.sh         # Executes real MapReduce pipeline and parses results
│   ├── run_dashboard_wsl.sh           # WSL2 dashboard launcher
│   └── parse_mr_results.py            # Converts part-r-* into dashboard CSVs/JSONs
├── dashboard/
│   ├── app.py                         # Complete Streamlit dark-themed dashboard
│   ├── components/
│   │   ├── charts.py                  # Reusable Plotly dark-themed visualizations
│   │   ├── data_loader.py             # High-performance summary reader
│   │   └── wsl_runner.py              # Live WSL2 MapReduce execution trigger
│   └── styles/
│       └── dark_theme.css             # Glassmorphic dark styling & neon accents
├── tests/
│   ├── test_downloader_config.py      # Unit tests for parameter builder & hashing
│   ├── test_live_api_probe.py         # Lightweight live HTTP probe (NYC Open Data)
│   ├── test_clean_and_validate.py     # Timestamp, coordinate, and text sanitization tests
│   ├── test_mapreduce_parser.py       # Output parser and metric calculation tests
│   └── test_dashboard_loader.py       # Dashboard loader and schema tests
└── docs/
    ├── PROJECT_REPORT.md              # Exhaustive academic report for university review
    ├── TROUBLESHOOTING.md             # Daemon operations and port conflict fixes
    ├── EXECUTION_EVIDENCE.md          # Real job counters, cluster logs, and screenshots guide
    └── PHASE2_VERIFICATION.md         # Data cleaning and HDFS staging verification
```

---

## 🧪 Automated Testing

Execute the complete automated test suite (12 tests covering downloader, live probe, sanitization, MapReduce parser, and dashboard data loader):
```powershell
.\.venv\Scripts\python -m unittest discover tests -v
```
**Result:** `12 tests passed in ~3.0 seconds (OK)`.

---

## 🔄 Re-Running the Hadoop MapReduce Pipeline

To trigger a new MapReduce run on the 1,000,005-record HDFS dataset:
1. **From Web UI:** Open the dashboard, navigate to **9. Hadoop & Pipeline Status**, and click **"Run MapReduce Analysis Now"**.
2. **From Terminal (WSL2):**
   ```bash
   bash scripts/run_hadoop_pipeline.sh
   ```
The pipeline automatically compiles the JAR, runs the job on YARN, extracts `part-r-*` outputs, generates structured CSV summaries, and writes an execution manifest.

---

## 📜 Academic Deliverables & Documentation
* **Comprehensive Project Report:** [`docs/PROJECT_REPORT.md`](file:///C:/Users/veetrag/Desktop/B.Tech_IT/BDAL/Urban_Civic_Complaint_Analytics/docs/PROJECT_REPORT.md)
* **Execution Evidence & Counters:** [`docs/EXECUTION_EVIDENCE.md`](file:///C:/Users/veetrag/Desktop/B.Tech_IT/BDAL/Urban_Civic_Complaint_Analytics/docs/EXECUTION_EVIDENCE.md)
* **System Troubleshooting:** [`docs/TROUBLESHOOTING.md`](file:///C:/Users/veetrag/Desktop/B.Tech_IT/BDAL/Urban_Civic_Complaint_Analytics/docs/TROUBLESHOOTING.md)
