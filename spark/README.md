# Spark SQLBench-H Benchmarks

## Pre-requisites

- Download Apache Maven from https://maven.apache.org/download.cgi
- Download Apache Spark 3.3.0 from https://spark.apache.org/downloads.html

Untar these downloads and set `MAVEN_HOME` and `SPARK_HOME` environment variables to point to the 
install location.

## Build the benchmark JAR file

```bash
$MAVEN_HOME/bin/mvn package
```

# Standalone Mode

## Start a local Spark cluster in standalone mode

```bash
$SPARK_HOME/sbin/start-master.sh
$SPARK_HOME/sbin/start-worker.sh spark://ripper:7077
```

## Run Single Query

```bash
$SPARK_HOME/bin/spark-submit --master spark://ripper:7077 \
    --class io.sqlbenchmarks.sqlbench.Main \
    --conf spark.driver.memory=8G \
    --conf spark.executor.memory=32G \
    --conf spark.executor.cores=24 \
    --conf spark.cores.max=24
    target/sqlbench-spark-0.1.0-SNAPSHOT-jar-with-dependencies.jar \
    --input-path /mnt/bigdata/tpch/sf100-parquet/ \
    --query-path ~/git/sql-benchmarks/sqlbench-h/queries/sf\=100/ \
    --query 1
```

## Run All Queries

```bash
$SPARK_HOME/bin/spark-submit --master spark://ripper:7077 \
    --class io.sqlbenchmarks.sqlbench.Main \
    --conf spark.driver.memory=8G \
    --conf spark.executor.memory=32G \
    --conf spark.executor.cores=24 \
    --conf spark.cores.max=24 \
    target/sqlbench-spark-0.1.0-SNAPSHOT-jar-with-dependencies.jar \
    --input-path /mnt/bigdata/tpch/sf100-parquet/ \
    --query-path ~/git/sql-benchmarks/sqlbench-h/queries/sf\=100/ \
    --num-queries 22
```

Monitor progress via the Spark UI at http://localhost:8080

## Shut down the cluster

```bash
$SPARK_HOME/sbin/stop-slave.sh
$SPARK_HOME/sbin/stop-master.sh
```

# Testing with Spark RAPIDS

[Spark RAPIDS](https://nvidia.github.io/spark-rapids/) is an open-source plugin that provides GPU-acceleration for 
Apache Spark SQL and ETL jobs.

```bash
$SPARK_HOME/bin/spark-submit --master spark://ripper:7077 \
    --class io.sqlbenchmarks.sqlbench.Main \
    --conf spark.driver.memory=8G \
    --conf spark.executor.memory=32G \
    --conf spark.executor.cores=24 \
    --conf spark.cores.max=24 \
    --jars $SPARK_RAPIDS_PLUGIN_JAR \
    --conf spark.plugins=com.nvidia.spark.SQLPlugin \
    --conf spark.rapids.sql.enabled=true \
    --conf spark.rapids.sql.explain=ALL \
    --conf spark.sql.session.timeZone=UTC \
    --conf spark.rapids.sql.concurrentGpuTasks=2 \
    --conf spark.rapids.sql.joinReordering=true \
    --conf spark.rapids.sql.joinReordering.factDimRatio=0.3 \
    --conf spark.rapids.sql.joinReordering.maxFactTables=2 \
    --conf spark.rapids.sql.joinReordering.filterSelectivity=1.0 \
    --conf spark.rapids.sql.joinReordering.preserveOrder=true \
    --conf spark.rapids.sql.joinReordering.preserveShape=false \
    --conf spark.rapids.memory.pinnedPool.size=20g \
    --conf spark.sql.files.maxPartitionBytes=2g \
    --conf spark.sql.session.timeZone=UTC \
    target/sqlbench-spark-0.1.0-SNAPSHOT-jar-with-dependencies.jar \
    --input-path /mnt/bigdata/tpcds/sf100-parquet/ \
    --query-path ~/git/sql-benchmarks/sqlbench-ds/queries/sf\=100/ \
    --query 44 > /tmp/log.txt 2>&1
```