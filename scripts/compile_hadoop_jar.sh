#!/usr/bin/env bash
set -e

PROJECT_DIR="/mnt/c/Users/veetrag/Desktop/B.Tech_IT/BDAL/Urban_Civic_Complaint_Analytics"
cd "$PROJECT_DIR"

HADOOP_BIN="/home/veetrag24/hadoop/bin/hadoop"
BUILD_DIR="$PROJECT_DIR/hadoop/build"
CLASSES_DIR="$BUILD_DIR/classes"
JAR_PATH="$PROJECT_DIR/hadoop/urban-civic-analytics.jar"

echo "=================================================================="
echo "COMPILING URBAN CIVIC MAPREDUCE ANALYTICS JAR"
echo "Project Path : $PROJECT_DIR"
echo "Hadoop Bin   : $HADOOP_BIN"
echo "Classes Dir  : $CLASSES_DIR"
echo "Output JAR   : $JAR_PATH"
echo "=================================================================="

# Setup build directories
rm -rf "$BUILD_DIR"
mkdir -p "$CLASSES_DIR"

# Get Hadoop Classpath
HADOOP_CP=$($HADOOP_BIN classpath)

echo "Compiling Java 11 source files..."
javac -cp "$HADOOP_CP" \
    -d "$CLASSES_DIR" \
    "$PROJECT_DIR/hadoop/src/main/java/org/urban/analytics/"*.java

echo "Packaging compiled classes into JAR..."
jar -cf "$JAR_PATH" -C "$CLASSES_DIR" .

echo "Verifying JAR content:"
jar -tf "$JAR_PATH" | grep "org/urban/analytics"

echo "=================================================================="
echo "BUILD SUCCESSFUL: $JAR_PATH ($(du -h "$JAR_PATH" | cut -f1))"
echo "=================================================================="
