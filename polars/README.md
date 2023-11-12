# SQLBench Runner for Polars

This is currently experimental because Polars does not yet have full SQL support.

All queries currently fail.

```
q1,FAILED: SQL expression TypedString { data_type: Date, value: "1998-12-01" } is not yet supported
q2,FAILED: SQL expression Like { negated: false, expr: Identifier(Ident { value: "p_type", quote_style: None }), pattern: Value(SingleQuotedString("%BRASS")), escape_char: None } is not yet supported
q3,FAILED: SQL expression TypedString { data_type: Date, value: "1995-03-21" } is not yet supported
q4,FAILED: SQL expression TypedString { data_type: Date, value: "1996-02-01" } is not yet supported
q5,FAILED: SQL expression TypedString { data_type: Date, value: "1997-01-01" } is not yet supported
q6,FAILED: SQL expression TypedString { data_type: Date, value: "1997-01-01" } is not yet supported
q7,FAILED: not implemented
q8,FAILED: not implemented
q9,FAILED: not implemented
q10,FAILED: SQL expression TypedString { data_type: Date, value: "1993-03-01" } is not yet supported
q11,FAILED: unable to find column "s_suppkey"; valid columns: ["ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost", "ps_comment", "_ignore"]
q12,FAILED: SQL expression TypedString { data_type: Date, value: "1994-01-01" } is not yet supported
q13,FAILED: not implemented
q14,FAILED: SQL expression TypedString { data_type: Date, value: "1994-10-01" } is not yet supported
q15,FAILED: SQL statement type CreateView { or_replace: false, materialized: false, name: ObjectName([Ident { value: "revenue0", quote_style: None }]), columns: [Ident { value: "supplier_no", quote_style: None }, Ident { value: "total_revenue", quote_style: None }], query: Query { with: None, body: Select(Select { distinct: None, top: None, projection: [UnnamedExpr(Identifier(Ident { value: "l_suppkey", quote_style: None })), UnnamedExpr(Function(Function { name: ObjectName([Ident { value: "sum", quote_style: None }]), args: [Unnamed(Expr(BinaryOp { left: Identifier(Ident { value: "l_extendedprice", quote_style: None }), op: Multiply, right: Nested(BinaryOp { left: Value(Number("1", false)), op: Minus, right: Identifier(Ident { value: "l_discount", quote_style: None }) }) }))], filter: None, null_treatment: None, over: None, distinct: false, special: false, order_by: [] }))], into: None, from: [TableWithJoins { relation: Table { name: ObjectName([Ident { value: "lineitem", quote_style: None }]), alias: None, args: None, with_hints: [], version: None, partitions: [] }, joins: [] }], lateral_views: [], selection: Some(BinaryOp { left: BinaryOp { left: Identifier(Ident { value: "l_shipdate", quote_style: None }), op: GtEq, right: TypedString { data_type: Date, value: "1996-12-01" } }, op: And, right: BinaryOp { left: Identifier(Ident { value: "l_shipdate", quote_style: None }), op: Lt, right: BinaryOp { left: TypedString { data_type: Date, value: "1996-12-01" }, op: Plus, right: Interval(Interval { value: Value(SingleQuotedString("3")), leading_field: Some(Month), leading_precision: None, last_field: None, fractional_seconds_precision: None }) } } }), group_by: Expressions([Identifier(Ident { value: "l_suppkey", quote_style: None })]), cluster_by: [], distribute_by: [], sort_by: [], having: None, named_window: [], qualify: None }), order_by: [], limit: None, limit_by: [], offset: None, fetch: None, locks: [] }, with_options: [], cluster_by: [], with_no_schema_binding: false, if_not_exists: false, temporary: false } is not supported
q16,FAILED: SQL expression Like { negated: true, expr: Identifier(Ident { value: "p_type", quote_style: None }), pattern: Value(SingleQuotedString("STANDARD BURNISHED%")), escape_char: None } is not yet supported
q17,FAILED: Unexpected SQL Subquery
q18,FAILED: unable to find column "l_quantity"; valid columns: ["l_orderkey"]
q19,FAILED: unable to find column "p_partkey"; valid columns: ["l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment", "_ignore"]
q20,FAILED: SQL expression Like { negated: false, expr: Identifier(Ident { value: "p_name", quote_style: None }), pattern: Value(SingleQuotedString("brown%")), escape_char: None } is not yet supported
q21,FAILED: no table or alias named 'l1' found
q22,FAILED: not implemented
```

## Run

Example usage:

```bash
python sqlbench-polars.py /mnt/bigdata/tpch/sf10-parquet ../../sqlbench-h/queries/sf\=10 22
```

## Run in Docker

From the root of this project, run the following command to build the Docker image.

```bash
./build-docker-polars.sh
```

Then run the benchmarks with this command. Note that you will need to customize the paths to reflect the location
of the query and data files in your environment.

```bash
docker run \
  --cpus 16 \
  -m 64GB \
  -v /mnt/bigdata/tpch/sf10-parquet/:/data \
  -v `pwd`/../sqlbench-h/queries/sf\=10/:/queries \
  -it sqlbench/polars /data /queries 22
```