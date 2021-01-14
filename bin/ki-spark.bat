@echo off

if not defined SPARK_HOME (
  echo SPARK_HOME is not specified
  exit /b
)

set SCRIPT_PATH=%~dp0

%SPARK_HOME%\bin\spark-submit --class org.jetbrains.kotlinx.ki.spark.SparkShell %SCRIPT_PATH%\..\lib\ki-spark-*.jar %*