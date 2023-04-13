#!/bin/bash
docker build -t sqlbench/datafusion-python -f datafusion-python/Dockerfile .
docker build -t sqlbench/duckdb -f duckdb/Dockerfile .