import argparse
from ballista import BallistaContext
import os
import time
import glob

def bench(data_path, query_path, num_queries):

    # defaults: shuffle_partitions = 16, batch_size = 8192
    shuffle_partitions = 48
    batch_size = 32768

    with open("configs.csv", 'w') as configs:
        configs.write("shuffle_partitions={}\n".format(shuffle_partitions))
        configs.write("batch_size={}\n".format(batch_size))

    with open("results.csv", 'w') as results:
        # register tables
        start = time.time()
        c = BallistaContext("localhost", 50050, shuffle_partitions, batch_size)
        for file in glob.glob("{}/*.parquet".format(data_path)):
            filename = os.path.basename(file)
            table_name = filename[0:len(filename)-8]
            create_view_sql = "CREATE EXTERNAL TABLE {} STORED AS parquet LOCATION '{}/*.parquet'".format(table_name, file)
            print(create_view_sql)
            c.sql(create_view_sql)
        end = time.time()
        print("Register Tables took {} seconds".format(end-start))
        results.write("setup,{}\n".format(round((end-start)*1000, 1)))
        results.flush()

        # run queries
        for query in range(1, num_queries+1):
            with open("{}/q{}.sql".format(query_path, query)) as f:
                sql = f.read()
                print(sql)

                try:
                    start = time.time()
                    df = c.sql(sql)
                    x = df.collect()
                    end = time.time()
                    time_millis = (end - start) * 1000
                    print("q{},{}".format(query, time_millis))
                    results.write("q{},{}\n".format(query, time_millis))
                    results.flush()

                    explain = df.explain_string()
                    with open("q{}_logical_plan.txt".format(query), "w") as exp:
                        exp.write(explain)

                except Exception as e:
                    print("query", query, "failed", e)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('data_path')
    parser.add_argument('query_path')
    parser.add_argument('num_queries')
    args = parser.parse_args()
    bench(args.data_path, args.query_path, int(args.num_queries))