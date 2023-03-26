import argparse
import ray
from raysql.context import RaySqlContext, ResultSet
import os
import time
import glob

def bench(data_path, query_path, output_path, num_queries, iterations):

    # start a cluster
    ray.init(resources={"worker": 1})

    # create context
    ctx = RaySqlContext(24, use_ray_shuffle=True)

    with open("{}/results.csv".format(output_path), 'w') as results:
        # register tables
        start = time.time()
        for file in glob.glob("{}/*.parquet".format(data_path)):
            filename = os.path.basename(file)
            table_name = filename[0:len(filename)-8]
            ctx.register_parquet(table_name, file)

        end = time.time()
        print("Register Tables took {} seconds".format(end-start))
        results.write("setup,{}\n".format(round((end-start)*1000, 1)))
        results.flush()

        # run queries
        for query in range(1, num_queries+1):
            with open("{}/q{}.sql".format(query_path, query)) as f:
                text = f.read()
                tmp = text.split(';')
                queries = []
                for str in tmp:
                    if len(str.strip()) > 0:
                        queries.append(str)

                try:
                    start = time.time()
                    for i in range(iterations):
                        print("iteration", i+1, "of", iterations, "...")
                        for sql in queries:
                            print(sql)
                            x = ctx.sql(sql)

                            # TODO this is very hacky and is a workaround for
                            # not trying to print empty result sets from DDL
                            # statements, which currently errors
                            if not ('create view' in sql or 'drop view' in sql):
                                result_set = x

                        print("final results", ResultSet(result_set))

                    end = time.time()

                    time_millis = ((end - start) * 1000) / iterations
                    if len(queries) == 2:
                        print("q{},{}".format(query, time_millis))
                    else:
                        print("q{}_{},{}".format(query, i, time_millis))
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
