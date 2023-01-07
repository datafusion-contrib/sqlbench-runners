# SQLBench Runners

This repository contains code to run SQLBench benchmarks using a variety of databases and query engines.

All SQLBench benchmarks following the same format:

- There is one directory containing all data files to be registered as SQL tables
- There is one directory containing all SQL queries to be executed against these tables. The queries are numbered 
  sequentially, starting with query 1. File names are `q1.sql`, `q2.sql`, and so on. Each file may contain multiple 
  SQL statements.

For a list of available SQLBench benchmarks, see https://sqlbenchmarks.io/benchmarks/
