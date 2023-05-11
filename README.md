# SQLBench Runners

This repository contains code to run SQLBench benchmarks using a variety of databases and query engines.

All SQLBench benchmarks following the same format:

- There is one directory containing all data files to be registered as SQL tables
- There is one directory containing all SQL queries to be executed against these tables. The queries are numbered 
  sequentially, starting with query 1. File names are `q1.sql`, `q2.sql`, and so on. Each file may contain multiple 
  SQL statements.

For a list of available SQLBench benchmarks, see https://sqlbenchmarks.io/benchmarks/

## Creating Docker Images

```bash
./build-docker-images.sh
```

## Running Benchmarks

All benchmarks are run in a constrained containerized environment for fairness. The environment is constrained 
by specifying `--cpus` and `-m`.

```bash
``docker run \
  --cpus 16 \
  -m 64GB \
  -v /mnt/bigdata/tpch/sf10-parquet/:/data \
  -v `pwd`/../sqlbench-h/queries/:/queries \
  -it sqlbench/datafusion-python /data /queries/sf=10 22``
```

