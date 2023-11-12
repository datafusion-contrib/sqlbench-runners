# SQLBench DataFusion Runner

## Build

```bash
cargo build --release
```

## Run Single Query

```bash
./target/release/sqlbench-datafusion \
  --concurrency 24 \
  --data-path /mnt/bigdata/tpch/sf10-parquet/ \
  --query-path ~/git/sql-benchmarks/sqlbench-h/queries/sf\=10/ \
  --iterations 1 \
  --output /tmp \
  --query 1
```

## Run All Queries

```bash
./target/release/sqlbench-datafusion \
  --concurrency 24 \
  --data-path /mnt/bigdata/tpch/sf10-parquet/ \
  --query-path ~/git/sql-benchmarks/sqlbench-h/queries/sf\=10/ \
  --iterations 1 \
  --output /tmp \
  --num-queries 22
```

## Docker

From the root of this project, run the following command to build the Docker image.

```bash
./build-docker-datafusion.sh
```

Then run the benchmarks with this command. Note that you will need to customize the paths to reflect the location
of the query and data files in your environment.

```bash
docker run \
  --cpus 16 \
  -m 64GB \
  -v /mnt/bigdata/tpch/sf10-parquet/:/data \
  -v `pwd`/../sqlbench-h/queries/sf\=10/:/queries \
  -it sqlbench/datafusion \
  --concurrency 16 \
  --data-path /data \
  --query-path /queries \
  --iterations 1 \
  --output /tmp \
  --num-queries 22
```