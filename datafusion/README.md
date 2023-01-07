# SQLBench-H: DataFusion Runner

## Build

```bash
cargo build --release
```

## Run Single Query

```bash
./target/release/datafusion-sqlbench-h \
  --concurrency 24 \
  --data-path /mnt/bigdata/tpch/sf10-parquet/ \
  --query-path ../../queries/sf\=10/ \
  --iterations 1 \
  --output . \
  --query 1
```

## Run All Queries

```bash
./target/release/datafusion-sqlbench-h \
  --concurrency 24 \
  --data-path /mnt/bigdata/tpch/sf10-parquet/ \
  --query-path ../../queries/sf\=10/ \
  --iterations 1 \
  --output .
```