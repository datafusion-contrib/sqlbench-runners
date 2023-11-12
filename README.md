# SQLBench Runners

This repository contains code to run the SQLBench-H and SQLBench-DS benchmarks using a variety of query engines.

All SQLBench benchmarks following the same format:

- There is one directory containing all data files or directories to be registered as SQL tables
- There is one directory containing all SQL queries to be executed against these tables. The queries are numbered 
  sequentially, starting with query 1. File names are `q1.sql`, `q2.sql`, and so on. Each file may contain multiple 
  SQL statements.

## Creating Docker Images

There is one script per query engine for building the Docker image. For example, the following script can be used
to build a Docker image for DuckDB.

```bash
./build-docker-duckdb.sh
```

It is assumed that the 

## Running Benchmarks

All benchmarks are run in a constrained containerized environment for fairness. The environment is constrained 
by specifying `--cpus` and `-m`.

Here is an example of running the DataFusion Python benchmarks. See the README for each engine for specific instructions.

```bash
docker run \
  --cpus 16 \
  -m 64GB \
  -v /mnt/bigdata/tpch/sf10-parquet/:/data \
  -v `pwd`/../sqlbench-h/queries/sf\=10/:/queries \
  -it sqlbench/datafusion-python /data /queries 22
```

