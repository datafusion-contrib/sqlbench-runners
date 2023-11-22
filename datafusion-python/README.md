# SQLBench Runner for DataFusion Python

Example usage:

```bash
python sqlbench-datafusion-python.py /mnt/bigdata/tpch/sf10-parquet/ ~/git/sql-benchmarks/sqlbench-h/queries/sf\=10 22
```

## Run in Docker

From the root of this project, run the following command to build the Docker image.

```bash
./build-docker-datafusion-python.sh
```

Then run the benchmarks with this command. Note that you will need to customize the paths to reflect the location
of the query and data files in your environment.

```bash
docker run \
  --cpus 16 \
  -m 64GB \
  -v /mnt/bigdata/tpch/sf10-parquet/:/data \
  -v `pwd`/../sqlbench-h/queries/sf\=10/:/queries \
  -it sqlbench/datafusion-python /data /queries 22
```
