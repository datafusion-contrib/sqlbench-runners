use datafusion::common::Result;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::datasource::MemTable;
use datafusion::physical_plan::displayable;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use datafusion::scalar::ScalarValue;
use datafusion::DATAFUSION_VERSION;
#[cfg(feature = "qpml")]
use qpml::from_datafusion;
use serde::Serialize;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use structopt::StructOpt;
use tokio::time::Instant;

#[derive(StructOpt, Debug)]
#[structopt(name = "basic")]
struct Opt {
    /// Activate debug mode
    #[structopt(long)]
    debug: bool,

    /// Optional path to config file
    #[structopt(short, long, parse(from_os_str))]
    config_path: Option<PathBuf>,

    /// Path to queries
    #[structopt(long, parse(from_os_str))]
    query_path: PathBuf,

    /// Path to data
    #[structopt(short, long, parse(from_os_str))]
    data_path: PathBuf,

    /// Output path
    #[structopt(short, long, parse(from_os_str))]
    output: PathBuf,

    /// Query number. If no query number specified then all queries will be executed.
    #[structopt(short, long)]
    query: Option<u8>,

    /// Number of queries in this benchmark suite
    #[structopt(short, long)]
    num_queries: Option<u8>,

    /// List of queries to exclude
    #[structopt(short, long)]
    exclude: Vec<u8>,

    /// Concurrency
    #[structopt(short, long)]
    concurrency: u8,

    /// Iterations (number of times to run each query)
    #[structopt(short, long)]
    iterations: u8,

    /// Optional GitHub SHA of DataFusion version for inclusion in result yaml file
    #[structopt(short, long)]
    rev: Option<String>,
}

#[derive(Debug, PartialEq, Serialize, Default)]
pub struct Results {
    system_time: u128,
    datafusion_version: String,
    datafusion_github_sha: Option<String>,
    config: HashMap<String, String>,
    command_line_args: Vec<String>,
    register_tables_time: u128,
    /// Vector of (query_number, query_times)
    query_times: Vec<(u8, Vec<u128>)>,
}

impl Results {
    fn new() -> Self {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        Self {
            system_time: current_time.as_millis(),
            datafusion_version: DATAFUSION_VERSION.to_string(),
            datafusion_github_sha: None,
            config: HashMap::new(),
            command_line_args: vec![],
            register_tables_time: 0,
            query_times: vec![],
        }
    }
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let mut results = Results::new();
    for arg in std::env::args() {
        results.command_line_args.push(arg);
    }

    let opt = Opt::from_args();
    results.datafusion_github_sha = opt.rev;

    let query_path = format!("{}", opt.query_path.display());
    let output_path = format!("{}", opt.output.display());

    let mut config = SessionConfig::new().with_target_partitions(opt.concurrency as usize);

    if let Some(config_path) = &opt.config_path {
        let file = File::open(config_path)?;
        let reader = BufReader::new(file);
        let lines = reader.lines();
        for line in lines {
            let line = line?;
            if line.starts_with("#") {
                continue;
            }
            let parts = line.split('=');
            let parts = parts.collect::<Vec<&str>>();
            if parts.len() == 2 {
                config = config.set(parts[0], ScalarValue::Utf8(Some(parts[1].to_string())));
            } else {
                println!("Warning! Skipping config entry {}", line);
            }
        }
    }

    for entry in config.options().entries() {
        if let Some(ref value) = entry.value {
            results.config.insert(entry.key, value.to_string());
        }
    }

    // register all tables in data directory
    let start = Instant::now();
    let ctx = SessionContext::new_with_config(config);
    for file in fs::read_dir(&opt.data_path)? {
        let file = file?;
        let file_path = file.path();
        let path = format!("{}", file.path().display());
        if path.ends_with(".parquet") {
            let filename = Path::file_name(&file_path).unwrap().to_str().unwrap();
            let table_name = &filename[0..filename.len() - 8];
            println!("Registering table {} as {}", table_name, path);
            ctx.register_parquet(&table_name, &path, ParquetReadOptions::default())
                .await?;
        }
    }

    let setup_time = start.elapsed().as_millis();
    println!("Setup time was {} ms", setup_time);
    results.register_tables_time = setup_time;

    match opt.query {
        Some(query) => {
            execute_query(
                &ctx,
                &query_path,
                query,
                opt.debug,
                &output_path,
                opt.iterations,
                &mut results,
            )
            .await?;
        }
        _ => {
            let num_queries = opt.num_queries.unwrap();
            for query in 1..=num_queries {
                if opt.exclude.contains(&query) {
                    println!("Skipping query {}", query);
                    continue;
                }

                let result = execute_query(
                    &ctx,
                    &query_path,
                    query,
                    opt.debug,
                    &output_path,
                    opt.iterations,
                    &mut results,
                )
                .await;
                match result {
                    Ok(_) => {}
                    Err(e) => println!("Fail: {}", e),
                }
            }
        }
    }

    // write results json file
    let json = serde_json::to_string_pretty(&results).unwrap();
    let f = File::create(&format!(
        "{}/results-{}.yaml",
        output_path, results.system_time
    ))?;
    let mut w = BufWriter::new(f);
    w.write(json.as_bytes())?;

    // write simple csv summary file
    let mut w = File::create(&format!("{}/results.csv", output_path))?;
    w.write(format!("setup,{}\n", results.register_tables_time).as_bytes())?;
    for (query, times) in &results.query_times {
        w.write(format!("q{},{}\n", query, times[0]).as_bytes())?;
    }

    Ok(())
}

pub async fn execute_query(
    ctx: &SessionContext,
    query_path: &str,
    query_no: u8,
    debug: bool,
    output_path: &str,
    iterations: u8,
    results: &mut Results,
) -> Result<()> {
    let filename = format!("{}/q{query_no}.sql", query_path);
    println!("Executing query {} from {}", query_no, filename);
    let sql = fs::read_to_string(&filename)?;

    // some queries have multiple statements
    let sql = sql
        .split(';')
        .filter(|s| !s.trim().is_empty())
        .collect::<Vec<_>>();

    let multipart = sql.len() > 1;

    let mut durations = vec![];
    for iteration in 0..iterations {
        // duration for executing all queries in the file
        let mut total_duration_millis = 0;

        for (i, sql) in sql.iter().enumerate() {
            if debug {
                println!("Query {}: {}", query_no, sql);
            }

            let file_suffix = if multipart {
                format!("_part_{}", i + 1)
            } else {
                "".to_owned()
            };

            let start = Instant::now();
            let df = ctx.sql(sql).await?;
            let batches = df.clone().collect().await?;
            let duration = start.elapsed();
            total_duration_millis += duration.as_millis();

            let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();

            println!(
                "Query {query_no}{file_suffix} executed in: {duration:?} and returned {row_count} rows",
            );

            if iteration == 0 {
                let plan = df.clone().into_optimized_plan()?;
                let formatted_query_plan = format!("{}", plan.display_indent());
                if debug {
                    println!("{}", formatted_query_plan);
                }

                let exec = df.create_physical_plan().await?;
                if debug {
                    println!("{}", displayable(exec.as_ref()).indent(false));
                }

                let filename = format!(
                    "{}/q{}{}_logical_plan.txt",
                    output_path, query_no, file_suffix
                );
                let mut file = File::create(&filename)?;
                write!(file, "{}", formatted_query_plan)?;

                // write QPML
                #[cfg(feature = "qpml")]
                {
                    let qpml = from_datafusion(&plan);
                    let filename = format!(
                        "{}/q{}{}_logical_plan.qpml",
                        output_path, query_no, file_suffix
                    );
                    let file = File::create(&filename)?;
                    let mut file = BufWriter::new(file);
                    serde_yaml::to_writer(&mut file, &qpml).unwrap();
                }

                // write results to disk
                if batches.is_empty() {
                    println!("Empty result set returned");
                } else {
                    let filename = format!("{}/q{}{}.csv", output_path, query_no, file_suffix);
                    let t = MemTable::try_new(batches[0].schema(), vec![batches])?;
                    let df = ctx.read_table(Arc::new(t))?;
                    df.write_csv(&filename, DataFrameWriteOptions::default(), None)
                        .await?;
                }
            }
        }
        durations.push(total_duration_millis);
    }
    results.query_times.push((query_no, durations));
    Ok(())
}
