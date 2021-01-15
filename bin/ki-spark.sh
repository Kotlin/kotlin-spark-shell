#!/bin/bash

if [ -z "${SPARK_HOME}" ]
then
  echo "SPARK_HOME is not specified"
  exit 1
fi

SCRIPT_DIR="$(dirname "$0")"

"$SPARK_HOME"/bin/spark-submit \
  --class org.jetbrains.kotlinx.ki.spark.SparkShell \
  "$SCRIPT_DIR"/../lib/ki-spark-*.jar $@

