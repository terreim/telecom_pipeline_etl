#!/bin/bash
# ============================================================================
# Download AWS/Hadoop JARs that Spark needs to talk to MinIO (S3-compatible).
#
# WHY:  The base Spark image ships with hadoop-client-runtime (the S3A
#       filesystem driver), but NOT the AWS SDK that S3A calls under the hood.
#       Without these JARs, spark.read.parquet("s3a://...") fails with:
#         java.lang.ClassNotFoundException: software.amazon.awssdk...
#
# HOW:  We download JARs from Maven Central into /opt/spark-extra-jars/,
#       which is a Docker volume shared between master and worker.
#       SPARK_EXTRA_CLASSPATH in docker-compose points here.
#
# VERSIONS: Must match the Hadoop version bundled in the Spark image.
#   Spark 4.0.2 ships hadoop-client-runtime 3.4.1 → use hadoop-aws 3.4.1.
#   hadoop-aws 3.4.1's parent POM (hadoop-project 3.4.1) declares:
#     aws-java-sdk.version     = 1.12.720   (SDK v1, legacy compat)
#     aws-java-sdk-v2.version  = 2.24.6     (SDK v2, primary)
#
# THREE JARS needed:
#   1. hadoop-aws         — S3A filesystem implementation
#   2. aws-java-sdk-bundle — AWS SDK v1 (some credential providers still use it)
#   3. bundle (awssdk v2) — AWS SDK v2 (S3A's primary HTTP client in Hadoop 3.4+)
# ============================================================================

set -e

JAR_DIR="/opt/spark-extra-jars"
MAVEN="https://repo1.maven.org/maven2"

HADOOP_AWS_VER="3.4.1"
AWS_SDK_V1_VER="1.12.720"
AWS_SDK_V2_VER="2.24.6"

download_jar() {
    local url="$1"
    local dest="$2"
    local name
    name=$(basename "$dest")

    if [ -f "$dest" ]; then
        echo "$name already exists, skipping."
        return
    fi

    echo "Downloading $name ..."
    curl -fSL "$url" -o "$dest"
    echo "Done."
}

mkdir -p "$JAR_DIR"

# 1. hadoop-aws: the S3A filesystem plugin for Hadoop
download_jar \
    "$MAVEN/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VER}/hadoop-aws-${HADOOP_AWS_VER}.jar" \
    "$JAR_DIR/hadoop-aws-${HADOOP_AWS_VER}.jar"

# 2. AWS SDK v1 bundle (legacy — some credential chain code still references it)
download_jar \
    "$MAVEN/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_V1_VER}/aws-java-sdk-bundle-${AWS_SDK_V1_VER}.jar" \
    "$JAR_DIR/aws-java-sdk-bundle-${AWS_SDK_V1_VER}.jar"

# 3. AWS SDK v2 bundle (primary — Hadoop 3.4+ uses this for all S3 operations)
download_jar \
    "$MAVEN/software/amazon/awssdk/bundle/${AWS_SDK_V2_VER}/bundle-${AWS_SDK_V2_VER}.jar" \
    "$JAR_DIR/bundle-${AWS_SDK_V2_VER}.jar"

# Clean up old/wrong-version JARs (from previous runs with wrong versions)
for jar in "$JAR_DIR"/*.jar; do
    name=$(basename "$jar")
    case "$name" in
        "hadoop-aws-${HADOOP_AWS_VER}.jar") ;;
        "aws-java-sdk-bundle-${AWS_SDK_V1_VER}.jar") ;;
        "bundle-${AWS_SDK_V2_VER}.jar") ;;
        *)
            echo "Removing outdated JAR: $name"
            rm -f "$jar"
            ;;
    esac
done

echo ""
echo "All Spark S3A JARs ready in $JAR_DIR:"
ls -lh "$JAR_DIR"
