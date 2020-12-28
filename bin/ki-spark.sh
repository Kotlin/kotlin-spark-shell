#!/bin/bash

if [ -z "${SPARK_HOME}" ]
then
  echo "SPARK_HOME is not specified"
  exit 1
fi

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

${SPARK_HOME}/bin/spark-submit \
  --class org.jetbrains.kotlin.ki.spark.SparkShell \
  ${SCRIPT_DIR}/../lib/ki-spark-*.jar $@

