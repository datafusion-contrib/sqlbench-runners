import os
import subprocess
import time

def clear_page_cache():
    try:
        # Running the command as root
        subprocess.run('echo 1 > /proc/sys/vm/drop_caches', shell=True, check=True, executable='/bin/bash')
        print("Page cache cleared successfully.")
    except subprocess.CalledProcessError as e:
        print("Failed to clear page cache:", e)

def bench_datafusion(version, cpus, mem, data_path, query_path):
    return ("docker run --cpus {} -m {} -v {}:/data -v {}:/queries -it sqlbench/datafusion:{} --concurrency {} --data-path /data --query-path /queries --iterations 1 --output . --num-queries 22"
        .format(cpus, mem, data_path, query_path, version, cpus))

if __name__ == "__main__":
    with open("results.csv", "w") as results:
        results.write("version,scale,cores,duration\n")
        results.flush()
        for version in [25, 26, 27, 28, 30, 31, 32, 33]:
            for scale in [10]:
                data_path = "/mnt/bigdata/tpch/sf{}-parquet/".format(scale)
                query_path = "~/git/sql-benchmarks/sqlbench-h/queries/sf={}".format(scale)
                for cores in [1, 2, 4, 8, 16]:
                    cmd = bench_datafusion(version, cores, '64g', data_path, query_path)

                    # clear page cache before each run
                    clear_page_cache()
                    print(cmd)
                    start = time.time()
                    subprocess.run(cmd, shell=True, check=True, executable='/bin/bash')
                    end = time.time()

                    # write results
                    print(version,scale,cores,end-start)
                    results.write("{},{},{},{}\n".format(version,scale,cores,end-start))
                    results.flush()

