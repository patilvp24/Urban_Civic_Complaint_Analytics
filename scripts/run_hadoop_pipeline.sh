#!/usr/bin/env bash
set -e

PROJECT_DIR="/mnt/c/Users/veetrag/Desktop/B.Tech_IT/BDAL/Urban_Civic_Complaint_Analytics"
cd "$PROJECT_DIR"

HADOOP_BIN="/home/veetrag24/hadoop/bin/hadoop"
HDFS_BIN="/home/veetrag24/hadoop/bin/hdfs"

RUN_ID="run_$(date +%Y%m%d_%H%M%S)"
INPUT_HDFS="/civic311/input/cleaned/nyc_311_cleaned.tsv"
OUTPUT_HDFS="/civic311/output/$RUN_ID"
LOCAL_RUN_DIR="$PROJECT_DIR/data/output/$RUN_ID"
LOCAL_LATEST_DIR="$PROJECT_DIR/data/output/latest"
JAR_PATH="$PROJECT_DIR/hadoop/urban-civic-analytics.jar"

echo "=================================================================="
echo "STARTING REAL HADOOP MAPREDUCE CIVIC ANALYTICS PIPELINE"
echo "Run ID           : $RUN_ID"
echo "HDFS Input       : $INPUT_HDFS"
echo "HDFS Output      : $OUTPUT_HDFS"
echo "Local Output Dir : $LOCAL_RUN_DIR"
echo "=================================================================="

# 1. Verify HDFS is accessible and exit safe mode if needed
echo "Verifying HDFS connectivity..."
$HDFS_BIN dfsadmin -safemode leave 2>&1 || true

# 2. Verify HDFS Input exists
if ! $HDFS_BIN dfs -test -e "$INPUT_HDFS"; then
    echo "[ERROR] Cleaned input not found in HDFS at $INPUT_HDFS!"
    exit 1
fi

# 3. Ensure JAR exists
if [ ! -f "$JAR_PATH" ]; then
    echo "JAR not found, compiling..."
    bash "$PROJECT_DIR/scripts/compile_hadoop_jar.sh"
fi

# 4. Execute Real Hadoop MapReduce Job
START_TIME=$(date +%s)
echo "Submitting MapReduce Job to Hadoop Cluster..."

$HADOOP_BIN jar "$JAR_PATH" org.urban.analytics.UrbanCivicAnalyticsDriver \
    "$INPUT_HDFS" \
    "$OUTPUT_HDFS" \
    1 2>&1 | tee "$PROJECT_DIR/hadoop/last_job_execution.log"

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

echo "=================================================================="
echo "MAPREDUCE EXECUTION FINISHED in ${DURATION}s"
echo "Verifying HDFS output directory..."
$HDFS_BIN dfs -ls "$OUTPUT_HDFS"
echo "=================================================================="

# 5. Extract results from HDFS to Local
mkdir -p "$LOCAL_RUN_DIR"
mkdir -p "$LOCAL_LATEST_DIR"

echo "Extracting part-r-* files from HDFS..."
$HDFS_BIN dfs -get "$OUTPUT_HDFS/part-r-*" "$LOCAL_RUN_DIR/"

# 6. Parse results into structured dashboard summaries
echo "Parsing MapReduce outputs into CSV/JSON summaries..."
python3 "$PROJECT_DIR/scripts/parse_mr_results.py" \
    --input-dir "$LOCAL_RUN_DIR" \
    --output-dir "$LOCAL_RUN_DIR"

# Copy to latest
cp -r "$LOCAL_RUN_DIR/"* "$LOCAL_LATEST_DIR/"

# 7. Write Manifest
MANIFEST_JSON="$LOCAL_RUN_DIR/manifest.json"
cat <<EOF > "$MANIFEST_JSON"
{
  "run_id": "$RUN_ID",
  "timestamp_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "hadoop_version": "3.3.6",
  "java_version": "11.0.32.1",
  "input_hdfs_path": "$INPUT_HDFS",
  "output_hdfs_path": "$OUTPUT_HDFS",
  "local_output_path": "data/output/$RUN_ID",
  "duration_seconds": $DURATION,
  "job_status": "SUCCESS",
  "summary_executive": "data/output/$RUN_ID/summary_executive.json"
}
EOF
cp "$MANIFEST_JSON" "$LOCAL_LATEST_DIR/manifest.json"

echo "=================================================================="
echo "PIPELINE COMPLETED SUCCESSFULLY!"
echo "Manifest: $MANIFEST_JSON"
echo "Latest summaries updated at: $LOCAL_LATEST_DIR"
echo "=================================================================="
