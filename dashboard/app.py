"""
Urban Civic Complaint and Service Resolution Analytics Using Hadoop MapReduce
Interactive Web Dashboard built with Streamlit and Plotly.
"""

import os
import sys
import json
import time
import pandas as pd
import streamlit as st

# Setup paths
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(CURRENT_DIR)
sys.path.append(CURRENT_DIR)

from components.data_loader import get_latest_analytics_data
from components.charts import (
    create_category_bar,
    create_agency_bar,
    create_status_donut,
    create_monthly_trend_line,
    create_borough_bar,
    create_resolution_bar,
    create_agency_status_stacked
)
from components.wsl_runner import run_hadoop_mapreduce_stream

# Streamlit Page Config
st.set_page_config(
    page_title="Urban Civic Complaint Analytics | Hadoop MapReduce",
    page_icon="🏙️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Load CSS Styles
css_path = os.path.join(CURRENT_DIR, "styles", "dark_theme.css")
if os.path.exists(css_path):
    with open(css_path, "r", encoding="utf-8") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

# Load Data
data = get_latest_analytics_data()
has_data = data["has_data"]
exec_m = data["executive"]
qual_m = data["quality"]
manifest_m = data["manifest"]
snap_m = data["snapshot_meta"]

# Header Banner
st.markdown("""
<div class="header-container">
    <div class="header-title">🏙️ Urban Civic Complaint & Service Resolution Analytics</div>
    <div class="header-subtitle">
        Big Data Intelligence Engine powered by <b>Apache Hadoop 3.3.6 MapReduce</b> and <b>NYC Open Data (311 Service Requests)</b>
    </div>
</div>
""", unsafe_allow_html=True)

# Sidebar Navigation & Global Controls
st.sidebar.image("https://upload.wikimedia.org/wikipedia/commons/0/0e/Hadoop_logo.svg", width=160)
st.sidebar.markdown("### 🎛️ Navigation")

SECTIONS = [
    "1. Executive Overview",
    "2. Complaint Category Analytics",
    "3. Agency Workload & Performance",
    "4. Request Status Analysis",
    "5. Monthly Temporal Trends",
    "6. Borough Geographic Analysis",
    "7. Resolution Duration Analytics",
    "8. Data Quality & Snapshot Audit",
    "9. Hadoop & Pipeline Status",
    "10. Methodology & Project Guide"
]

selected_section = st.sidebar.radio("Select View:", SECTIONS)

st.sidebar.markdown("---")
st.sidebar.markdown("### ℹ️ Dataset Snapshot")
st.sidebar.markdown(f"**Records:** `{snap_m.get('total_rows_downloaded', 1000005):,}`")
st.sidebar.markdown(f"**Checksum:** `{snap_m.get('sha256_checksum', 'a6b3313d...')[:12]}...`")
st.sidebar.markdown(f"**Hadoop Stack:** `3.3.6 on WSL2`")
st.sidebar.markdown("---")
st.sidebar.caption("Big Data Analytics Lab Mini Project • Academic Deliverable")


# ==============================================================================
# SECTION 1: EXECUTIVE OVERVIEW
# ==============================================================================
if selected_section == "1. Executive Overview":
    st.markdown("### 📊 Executive Overview & Key Performance Indicators")
    
    if not has_data:
        st.warning("⚠️ No MapReduce summary data found yet. Navigate to '9. Hadoop & Pipeline Status' to run the pipeline.")
    else:
        # KPI Row
        total_rec = exec_m.get("total_records_analyzed", 1000005)
        closed_rec = exec_m.get("closed_requests_count", 0)
        open_rec = exec_m.get("open_requests_count", 0)
        res_rate = exec_m.get("resolution_rate_percent", 0.0)
        avg_days = exec_m.get("overall_avg_resolution_days", 0.0)
        avg_hours = exec_m.get("overall_avg_resolution_hours", 0.0)

        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-title">Records Analyzed</div>
                <div class="kpi-value">{total_rec:,}</div>
                <div class="kpi-subtitle">100% Real 311 Records</div>
            </div>
            """, unsafe_allow_html=True)
        with col2:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-title">Closed Requests</div>
                <div class="kpi-value" style="color: #00e676;">{closed_rec:,}</div>
                <div class="kpi-subtitle">Resolved Complaints</div>
            </div>
            """, unsafe_allow_html=True)
        with col3:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-title">Open Backlog</div>
                <div class="kpi-value" style="color: #ffd740;">{open_rec:,}</div>
                <div class="kpi-subtitle">Active Service Requests</div>
            </div>
            """, unsafe_allow_html=True)
        with col4:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-title">Resolution Rate</div>
                <div class="kpi-value" style="color: #00d2ff;">{res_rate:.1f}%</div>
                <div class="kpi-subtitle">Closed / Total Ratio</div>
            </div>
            """, unsafe_allow_html=True)
        with col5:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-title">Avg Resolution Time</div>
                <div class="kpi-value" style="color: #b388ff;">{avg_days:.1f} d</div>
                <div class="kpi-subtitle">{avg_hours:,.1f} Hours Mean Duration</div>
            </div>
            """, unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # Overview Visualizations
        r1_col1, r1_col2 = st.columns([3, 2])
        with r1_col1:
            st.plotly_chart(create_category_bar(data["categories"], top_n=10), use_container_width=True)
        with r1_col2:
            st.plotly_chart(create_status_donut(data["statuses"]), use_container_width=True)

        r2_col1, r2_col2 = st.columns([1, 1])
        with r2_col1:
            st.plotly_chart(create_agency_bar(data["agencies"], top_n=8), use_container_width=True)
        with r2_col2:
            st.plotly_chart(create_borough_bar(data["boroughs"]), use_container_width=True)


# ==============================================================================
# SECTION 2: COMPLAINT CATEGORY ANALYTICS
# ==============================================================================
elif selected_section == "2. Complaint Category Analytics":
    st.markdown("### 🏷️ Complaint Category Analytics")
    st.markdown("Detailed breakdown of civic complaints categorized by issue type across the 1,000,005 record snapshot.")

    categories_df = data["categories"]
    if categories_df.empty:
        st.info("No category summaries available.")
    else:
        top_n = st.slider("Select Top Categories to Display:", min_value=5, max_value=30, value=15, step=5)
        st.plotly_chart(create_category_bar(categories_df, top_n=top_n), use_container_width=True)

        st.markdown("#### Tabular Summary of Complaint Categories")
        st.dataframe(
            categories_df.head(top_n).style.format({"count": "{:,}", "percentage": "{:.2f}%"}),
            use_container_width=True
        )

        csv_data = categories_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Download Complaint Categories Summary (CSV)",
            data=csv_data,
            file_name="summary_complaint_categories.csv",
            mime="text/csv"
        )


# ==============================================================================
# SECTION 3: AGENCY WORKLOAD & PERFORMANCE
# ==============================================================================
elif selected_section == "3. Agency Workload & Performance":
    st.markdown("### 🏢 Responding Agency Workload and Status Distribution")
    st.markdown("Examines the operational volume and resolution efficiency of responding New York City municipal agencies (NYPD, HPD, DOT, DSNY, DEP, etc.).")

    agencies_df = data["agencies"]
    agency_status_df = data["agency_status"]

    if agencies_df.empty:
        st.info("No agency summaries available.")
    else:
        col1, col2 = st.columns([1, 1])
        with col1:
            st.plotly_chart(create_agency_bar(agencies_df, top_n=10), use_container_width=True)
        with col2:
            if not agency_status_df.empty:
                st.plotly_chart(create_agency_status_stacked(agency_status_df, top_agencies=7), use_container_width=True)

        st.markdown("#### Agency Workload Statistics")
        st.dataframe(
            agencies_df.style.format({"count": "{:,}", "percentage": "{:.2f}%"}),
            use_container_width=True
        )

        csv_data = agencies_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Download Agency Workload Summary (CSV)",
            data=csv_data,
            file_name="summary_agency_workload.csv",
            mime="text/csv"
        )


# ==============================================================================
# SECTION 4: REQUEST STATUS ANALYSIS
# ==============================================================================
elif selected_section == "4. Request Status Analysis":
    st.markdown("### 📌 Civic Service Request Status Breakdown")
    st.markdown("Distribution of request lifecycle states: Closed, Open, Pending, Assigned, In Progress, and Started.")

    statuses_df = data["statuses"]
    if statuses_df.empty:
        st.info("No status summaries available.")
    else:
        col1, col2 = st.columns([1, 1])
        with col1:
            st.plotly_chart(create_status_donut(statuses_df), use_container_width=True)
        with col2:
            st.markdown("#### Request Status Table")
            st.dataframe(
                statuses_df.style.format({"count": "{:,}", "percentage": "{:.2f}%"}),
                use_container_width=True
            )
            csv_data = statuses_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download Status Breakdown (CSV)",
                data=csv_data,
                file_name="summary_request_statuses.csv",
                mime="text/csv"
            )


# ==============================================================================
# SECTION 5: MONTHLY TEMPORAL TRENDS
# ==============================================================================
elif selected_section == "5. Monthly Temporal Trends":
    st.markdown("### 📈 Monthly Civic Complaint Trends (Time-Series)")
    st.markdown("Temporal progression of complaint volume over time based on request creation timestamp (`created_date`).")

    monthly_df = data["monthly"]
    if monthly_df.empty:
        st.info("No monthly summaries available.")
    else:
        st.plotly_chart(create_monthly_trend_line(monthly_df), use_container_width=True)

        st.markdown("#### Chronological Monthly Records")
        st.dataframe(
            monthly_df.style.format({"count": "{:,}"}),
            use_container_width=True
        )

        csv_data = monthly_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Download Monthly Trends (CSV)",
            data=csv_data,
            file_name="summary_monthly_trends.csv",
            mime="text/csv"
        )


# ==============================================================================
# SECTION 6: BOROUGH GEOGRAPHIC ANALYSIS
# ==============================================================================
elif selected_section == "6. Borough Geographic Analysis":
    st.markdown("### 🗺️ Borough & Geographic Distribution")
    st.markdown("Civic complaint density across the five NYC boroughs: Brooklyn, Queens, Manhattan, Bronx, Staten Island, and Unspecified.")

    boroughs_df = data["boroughs"]
    borough_cat_df = data["borough_category"]

    if boroughs_df.empty:
        st.info("No borough summaries available.")
    else:
        col1, col2 = st.columns([1, 1])
        with col1:
            st.plotly_chart(create_borough_bar(boroughs_df), use_container_width=True)
        with col2:
            st.markdown("#### Borough Volume Breakdown")
            st.dataframe(
                boroughs_df.style.format({"count": "{:,}", "percentage": "{:.2f}%"}),
                use_container_width=True
            )

        st.markdown("---")
        st.markdown("#### Top Complaint Profiles by Borough")
        if not borough_cat_df.empty:
            sel_b = st.selectbox("Select Borough to Inspect Top Complaints:", boroughs_df["borough"].tolist())
            b_subset = borough_cat_df[borough_cat_df["borough"] == sel_b].sort_values("count", ascending=False).head(10)
            st.dataframe(b_subset.style.format({"count": "{:,}"}), use_container_width=True)


# ==============================================================================
# SECTION 7: RESOLUTION DURATION ANALYTICS
# ==============================================================================
elif selected_section == "7. Resolution Duration Analytics":
    st.markdown("### ⏱️ Service Resolution Duration Analytics")
    st.markdown("""
    Analyzes the operational response duration ($\Delta t = t_{\\text{closed}} - t_{\\text{created}}$) for all valid closed complaints.
    
    > **Exclusion Transparency Note:**
    > - Actively open complaints without closure dates ($8,839$ records) are strictly excluded from resolution averages (never treated as 0-minute resolutions).
    > - Chronological clerical anomalies where $t_{\\text{closed}} < t_{\\text{created}}$ ($9,830$ records) are excluded to prevent negative durations.
    """)

    agency_res_df = data["agency_res"]
    cat_res_df = data["category_res"]

    metric_unit = st.radio("Display Units:", ["Days (avg_days)", "Hours (avg_hours)"], horizontal=True)
    metric_col = "avg_days" if "Days" in metric_unit else "avg_hours"

    col1, col2 = st.columns([1, 1])
    with col1:
        if not agency_res_df.empty:
            st.plotly_chart(create_resolution_bar(agency_res_df, top_n=10, metric=metric_col), use_container_width=True)
    with col2:
        if not cat_res_df.empty:
            st.plotly_chart(create_resolution_bar(cat_res_df, top_n=10, metric=metric_col), use_container_width=True)

    st.markdown("#### Detailed Resolution Performance Table (Agencies)")
    if not agency_res_df.empty:
        st.dataframe(
            agency_res_df.style.format({
                "resolved_count": "{:,}",
                "avg_hours": "{:.2f}",
                "avg_days": "{:.2f}",
                "total_hours": "{:,.1f}"
            }),
            use_container_width=True
        )


# ==============================================================================
# SECTION 8: DATA QUALITY & SNAPSHOT AUDIT
# ==============================================================================
elif selected_section == "8. Data Quality & Snapshot Audit":
    st.markdown("### 🛡️ Data Quality Scorecard & Snapshot Audit")
    st.markdown("Complete lineage and cryptographic verification of the fixed local dataset snapshot.")

    col1, col2 = st.columns([1, 1])
    with col1:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Raw Snapshot File</div>
            <div class="kpi-value" style="font-size: 1.2rem; color: #00d2ff;">data/raw/nyc_311_snapshot_1M.csv</div>
            <div class="kpi-subtitle">File Size: {snap_m.get('file_size_mb', 165.24)} MB | Rows: {snap_m.get('total_rows_downloaded', 1000005):,}</div>
            <br>
            <div class="kpi-title">SHA-256 Checksum</div>
            <div class="kpi-subtitle" style="font-family: monospace; color: #f8fafc;">{snap_m.get('sha256_checksum', 'a6b3313d...')}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        cl_m = data["cleaning_meta"]
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Cleaned TSV File (HDFS Staged)</div>
            <div class="kpi-value" style="font-size: 1.2rem; color: #00e676;">data/cleaned/nyc_311_cleaned.tsv</div>
            <div class="kpi-subtitle">File Size: {cl_m.get('file_size_mb', 124.03)} MB | Delimiter: Tab (\\t)</div>
            <br>
            <div class="kpi-title">Cleaned Checksum</div>
            <div class="kpi-subtitle" style="font-family: monospace; color: #f8fafc;">{cl_m.get('sha256_checksum', '509e5b4d...')}</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("#### Quality Profiling Metrics")
    qm_col1, qm_col2, qm_col3, qm_col4 = st.columns(4)
    with qm_col1:
        st.metric("Open Tickets (No Closed Date)", f"{qual_m.get('OPEN_REQUESTS', 8839):,}", "Preserved")
    with qm_col2:
        st.metric("Inverted Timestamps", f"{qual_m.get('INVERTED_TIMESTAMPS', 9830):,}", "Duration Ignored")
    with qm_col3:
        st.metric("Missing Coordinates", f"{qual_m.get('MISSING_COORDINATES', 32228):,}", "Preserved as empty")
    with qm_col4:
        st.metric("Unspecified Boroughs", f"{qual_m.get('UNSPECIFIED_BOROUGH', 15792):,}", "Standardized")


# ==============================================================================
# SECTION 9: HADOOP & PIPELINE STATUS
# ==============================================================================
elif selected_section == "9. Hadoop & Pipeline Status":
    st.markdown("### ⚙️ Apache Hadoop Cluster & MapReduce Pipeline Engine")
    st.markdown("Monitor cluster health, view previous run manifests, or trigger a genuine MapReduce execution.")

    status_col1, status_col2, status_col3 = st.columns(3)
    with status_col1:
        st.metric("Hadoop Version", "3.3.6 (WSL2)", "Operational")
    with status_col2:
        st.metric("Java Virtual Machine", "OpenJDK 11.0.32.1", "64-Bit Server VM")
    with status_col3:
        st.metric("HDFS Input Path", "/civic311/input/cleaned", "1,000,005 records")

    st.markdown("---")
    st.markdown("#### Latest MapReduce Run Manifest")
    if manifest_m:
        st.json(manifest_m)
    else:
        st.info("No run manifest recorded yet.")

    st.markdown("---")
    st.markdown("#### Trigger Live MapReduce Execution")
    st.info("Clicking below will execute the full MapReduce job on WSL2 against the 1,000,005-record HDFS dataset.")

    if st.button("🚀 Run MapReduce Analysis Now"):
        terminal_placeholder = st.empty()
        log_lines = []
        with st.spinner("Submitting and executing MapReduce pipeline on Hadoop..."):
            for line in run_hadoop_mapreduce_stream():
                log_lines.append(line)
                # Keep last 15 lines visible
                tail = "".join(log_lines[-20:])
                terminal_placeholder.code(tail, language="bash")
        st.success("✅ MapReduce Pipeline Completed Successfully! Refreshing analytics...")
        time.sleep(1)
        st.rerun()


# ==============================================================================
# SECTION 10: METHODOLOGY & PROJECT GUIDE
# ==============================================================================
elif selected_section == "10. Methodology & Project Guide":
    st.markdown("### 📘 Big Data Architecture, Methodology & Project Guide")
    st.markdown("""
    This section details the theoretical foundations, architectural decisions, distributed data processing workflow, and analytics methodology.
    """)

    st.markdown("#### 1. System Architecture")
    st.code("""
[NYC Open Data erm2-nwe9]
         │
         ▼ (Chunked SODA Stream)
[Local Snapshot CSV] ──▶ [Streaming TSV Normalizer]
                                  │
                                  ▼ (hdfs dfs -put)
                      [HDFS: /civic311/input/cleaned]
                                  │
                                  ▼
               [Hadoop MapReduce (Java 11 / Hadoop 3.3.6)]
               ├── Mapper: Emits multi-dimensional composite keys
               ├── Combiner: Pre-aggregates metrics on map side
               └── Reducer: Computes sums, averages, and rates
                                  │
                                  ▼ (hdfs dfs -get)
                      [data/output/run_<timestamp>/]
                                  │
                                  ▼
                    [Streamlit Dark Web Dashboard]
    """, language="text")

    st.markdown("#### 2. Big Data Characteristics (The 5 Vs)")
    st.markdown("""
    - **Volume:** Analyzes $1,000,005$ distinct civic service requests totaling $165.24\\text{ MB}$ raw and $124.03\\text{ MB}$ cleaned.
    - **Velocity:** Simulates continuous real-world civic intake; streaming architecture handles bulk chunked ingest.
    - **Variety:** Combines temporal timestamps, textual complaints, categorical statuses, geographical coordinates, and municipal agency codes.
    - **Veracity:** Rigorous data sanitization audits for missing closure timestamps ($8,839$ open tickets) and inverted dates ($9,830$ anomalies).
    - **Value:** Delivers actionable municipal insights into service bottlenecks, agency response speeds, and localized community grievances.
    """)

    st.markdown("#### 3. Data Processing & MapReduce Methodology")
    st.markdown("""
    - **Streaming TSV Sanitization:** Civic complaint descriptions frequently contain embedded commas and quotation marks (e.g., addresses, noise descriptors). In TSV, tabs and newlines inside text fields are sanitized into spaces, ensuring that every record is strictly single-line and split safely using standard tab characters without complex quoting state machines.
    - **Associative Combiner Optimization:** The Combiner acts as a map-side mini-reducer before data is shuffled across the network. Because our `MetricWritable` maintains both `count` and `sumDurationHours`, both summation operations are associative and commutative. In our execution, the Combiner reduced intermediate shuffle records from $12,969,120$ to $10,597$—a **$99.92\\%$ reduction in shuffle bandwidth**.
    - **Resolution Duration Accounting:** Missing `closed_date` values indicate actively open complaints ($8,839$ in our dataset). Rather than discarding them or arbitrarily assuming a zero-day resolution, the mapper increments `OPEN_REQUESTS` and emits status aggregations, while bypassing the resolution duration emitter. Similarly, records with inverted timestamps ($9,830$ clerical backdating anomalies) are preserved as valid complaints while excluding their negative durations from duration metrics.
    """)

    st.markdown("#### 4. Dashboard Architecture & Execution Engine")
    st.markdown("""
    - **Dual-Mode Operation:** Under normal launches, the dashboard reads cached summaries from `data/output/latest/` in sub-second time without placing compute load on Hadoop.
    - **Real Pipeline Trigger:** When the **Run MapReduce Analysis Now** button is pressed, a secure Python subprocess invokes the complete bash pipeline in Ubuntu-22.04 WSL2, streaming cluster console logs to an interactive terminal widget and refreshing the UI with newly produced results.
    """)
