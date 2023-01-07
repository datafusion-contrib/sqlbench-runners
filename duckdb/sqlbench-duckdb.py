import duckdb
import glob
import os
import time

def bench(data_path, query_path, num_queries):
    con = duckdb.connect()

    with open("results.csv", 'w') as results:
        start = time.time()

        for file in glob.glob("{}/*.parquet".format(data_path)):
            filename = os.path.basename(file)
            table_name = filename[0:len(filename)-8]
            create_view_sql = "CREATE VIEW {} AS SELECT * FROM read_parquet('{}/*.parquet')".format(table_name, file)
            print(create_view_sql)
            con.execute(create_view_sql)

        end = time.time()
        results.write("setup,{}".format((end-start)*1000))

        for query in range(1, num_queries):
            with open("{}/q{}.sql".format(query_path, query)) as f:
                sql = f.read()
                #print(sql)
                start = time.time()
                x = con.execute(sql).fetchall()
                end = time.time()
                time_millis = (end - start) * 1000
                print("q{},{}".format(query, time_millis))
                results.write("q{},{}".format(query, time_millis))

if __name__ == "__main__":
    bench("/mnt/bigdata/tpch/sf10-parquet", "../../sqlbench-h/queries/sf=10", 22)