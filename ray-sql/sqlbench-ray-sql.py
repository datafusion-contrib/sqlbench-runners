import argparse
import ray
from raysql.context import RaySqlContext
from raysql.worker import Worker
import os
import time
import glob

def bench(data_path, query_path, output_path, num_queries, iterations):

    # start a cluster
    ray.init()

    # create some remote Workers
    workers = [Worker.remote() for i in range(24)]

    # create context
    ctx = RaySqlContext.remote(workers)

    with open("{}/results.csv".format(output_path), 'w') as results:
        # register tables
        start = time.time()
        for file in glob.glob("{}/*.parquet".format(data_path)):
            filename = os.path.basename(file)
            table_name = filename[0:len(filename)-8]
            ray.get(ctx.register_parquet.remote(table_name, file))

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
                    for i in range(iterations):
                        print("iteration", i+1, "of", iterations, "...")
                        result_set = ray.get(ctx.sql.remote(sql))
                        print("final results", result_set)

                    end = time.time()

                    time_millis = ((end - start) * 1000) / iterations
                    print("q{},{}".format(query, time_millis))
                    results.write("q{},{}\n".format(query, time_millis))
                    results.flush()

#                    explain = df.explain_string()
#                    with open("{}/q{}_explain.txt".format(output_path, query), "w") as exp:
#                        exp.write(explain)

                except Exception as e:
                    print("query", query, "failed", e)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('data_path')
    parser.add_argument('query_path')
    parser.add_argument('output_path')
    parser.add_argument('num_queries')
    parser.add_argument('iterations')
    args = parser.parse_args()
    bench(args.data_path, args.query_path, args.output_path, int(args.num_queries), int(args.iterations))
