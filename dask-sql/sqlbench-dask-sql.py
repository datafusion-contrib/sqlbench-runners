import argparse
from dask_sql import Context
import os
import time
import glob

def bench(data_path, query_path, num_queries):

    with open("results.csv", 'w') as results:
        # register tables
        start = time.time()
        c = Context()
        for file in glob.glob("{}/*.parquet".format(data_path)):
            filename = os.path.basename(file)
            table_name = filename[0:len(filename)-8]
            create_view_sql = "CREATE TABLE {} WITH (location = '{}/*.parquet', format = 'parquet')".format(table_name, file)
            print(create_view_sql)
            c.sql(create_view_sql)
        end = time.time()
        print("Register Tables took {} seconds".format(end-start))
        results.write("setup,{}\n".format((end-start)*1000))

        # run queries
        for query in range(1, num_queries):
            with open("{}/q{}.sql".format(query_path, query)) as f:
                sql = f.read()
                #print(sql)
                try:
                    start = time.time()
                    c.sql(sql)
                    x = c.fetchall()
                    end = time.time()
                    time_millis = (end - start) * 1000
                    print("q{},{}".format(query, time_millis))
                    results.write("q{},{}\n".format(query, time_millis))
                except:
                    print("query", query, "failed")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('data_path')
    parser.add_argument('query_path')
    parser.add_argument('num_queries')
    args = parser.parse_args()
    bench(args.data_path, args.query_path, int(args.num_queries))