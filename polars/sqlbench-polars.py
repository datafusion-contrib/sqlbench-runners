import argparse
import polars as pl
import os
import time
import glob

def bench(data_path, query_path, num_queries):

    with open("results.csv", 'w') as results:
        # register tables
        start = time.time()

        ctx = pl.SQLContext()

        for file in glob.glob("{}/*.parquet".format(data_path)):
            filename = os.path.basename(file)
            table_name = filename[0:len(filename)-8]
            # TODO this currently assumes that each table is a directory containing multiple parquet files
            df = pl.scan_parquet("{}/*.parquet".format(file))
            ctx.register(table_name, df)
        end = time.time()
        print("setup,{}".format(round((end-start)*1000,1)))
        results.write("setup,{}\n".format(round((end-start)*1000,1)))

        # run queries
        total_time_millis = 0
        for query in range(1, num_queries+1):
            with open("{}/q{}.sql".format(query_path, query)) as f:
                text = f.read()
                tmp = text.split(';')
                queries = []
                for str in tmp:
                    if len(str.strip()) > 0:
                        queries.append(str.strip())

                try:
                    start = time.time()
                    for sql in queries:
                        # print(sql)
                        df = ctx.execute(sql)
                        result_set = df.collect()
                        print(len(result_set))

                    end = time.time()
                    time_millis = (end - start) * 1000
                    total_time_millis += time_millis
                    print("q{},{}".format(query, round(time_millis,1)))
                    results.write("q{},{}\n".format(query, round(time_millis,1)))
                except Exception as e:
                    print("query", query, "failed", e)
                    results.write("q{},FAILED: {}\n".format(query, e))
        print("total,{}".format(round(total_time_millis,1)))
        results.write("total,{}\n".format(round(total_time_millis,1)))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('data_path')
    parser.add_argument('query_path')
    parser.add_argument('num_queries')
    args = parser.parse_args()
    bench(args.data_path, args.query_path, int(args.num_queries))