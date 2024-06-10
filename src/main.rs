use core::panic;
use std::{collections::HashMap, str::FromStr, sync::Arc};
mod config;
mod data_storages;
use data_storages::{
    data_storages::{ReadResult, SchemaTypeWithValue},
    DataStorage,
};

use clap::{command, Parser, Subcommand};
use config::Config;
use regex::Regex;

#[derive(Parser, Debug)]
#[command(version, about)]
struct TransOptions {
    /// config path for source and sink.
    #[arg(short, long)]
    config: Option<String>,
    /// source of data, could be name in config or a protocol.
    /// e.g. mysql://xxxx:xxxx/xxxx
    #[arg(long)]
    source: String,
    /// options for source, e.g. --source_option 'query="select * from xxx"'. It's more recommands
    /// to write it into config file with source.
    #[arg(long)]
    source_option: Vec<String>,
    /// options for sink, just like source_option.
    #[arg(long)]
    sink_option: Vec<String>,
    /// schema for source.
    #[arg(long)]
    source_schema: Option<String>,
    /// sink of data, could be name in config or a protocol, just like source.
    #[arg(long)]
    sink: String,
    /// schema for sink.
    #[arg(long)]
    sink_schema: Option<String>,
    /// if use chunk r/w, chunk size
    #[arg(long)]
    chunk_size: Option<u32>,
    /// read buffer size(row number), if reads too fast and buffer size is too large may cause oom, 0 means
    /// unbounded, default 0
    #[arg(long, default_value_t = 0)]
    buffer_size: u32,
}

#[derive(Parser, Debug)]
#[command(version, about)]
struct GenOptions {
    /// output file path.
    #[arg(long, default_value_t = {"example.yaml".to_string()})]
    output: String,
}

#[derive(Subcommand)]
enum Subcommands {
    /// transfer data from source to sink.
    Trans(TransOptions),
    /// generate example config file.
    GenExample(GenOptions),
}

#[derive(Parser)]
#[command(author, version, about)]
struct Cli {
    #[command(subcommand)]
    command: Subcommands,
}

// convert Vec["k1=v1", "k2=v2"] -> HashMap<"k1" -> "v1", "k2" -> "v2">
fn convert_option(config: Vec<String>) -> HashMap<String, String> {
    config
        .into_iter()
        .map(|each| {
            if !each.contains('=') {
                panic!("please specific config in format: 'k=v'.");
            }
            let mut split = each
                .splitn(2, '=')
                .map(String::from_str)
                .map(|s| s.unwrap())
                .collect::<Vec<_>>();
            (split.swap_remove(0), split.swap_remove(1))
        })
        .collect::<HashMap<_, _>>()
}

async fn load_data_storage(
    uri_or_name: &str,
    config: &Option<Config>,
    config_from_args: &HashMap<String, String>,
) -> impl data_storages::DataStorage {
    let r = Regex::new(r"[a-zA-Z0-9]+://.*").unwrap();
    if r.is_match(uri_or_name) {
        data_storages::loader::load_data_storage(uri_or_name, config_from_args).await
    } else {
        match config {
            Some(c) => {
                let storage = c
                    .data_storages
                    .get(uri_or_name)
                    .expect("cannot find any storages names: {uri_or_name} in config file.");
                let mut options_from_config = storage.options.clone();
                options_from_config.extend(config_from_args.clone());
                data_storages::loader::load_data_storage(storage.uri.as_str(), &options_from_config)
                    .await
            }
            None => {
                panic!("must provide a config file if provided a data storage name.")
            }
        }
    }
}

async fn exec_trans<'a: 'b, 'b>(args: TransOptions) {
    let config: Option<Config> = match args.config {
        Some(config_path) => {
            let f = std::fs::File::open(config_path).expect("cannot open file {config_path}");
            Some(serde_yaml::from_reader(f).unwrap())
        }
        None => None,
    };
    let src_options = convert_option(args.source_option);
    let sink_options = convert_option(args.sink_option);
    let mut source = load_data_storage(args.source.as_str(), &config, &src_options).await;

    let src_str_options = &src_options
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect::<HashMap<_, _>>();
    let sink_str_options = &sink_options
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect::<HashMap<_, _>>();
    // try read schema first
    let schema = match source.read_schema(src_str_options).await {
        Ok(schema) => Some(schema),
        Err(err) => {
            println!("may not support get schema, reason: {err}");
            None
        }
    };

    match args.chunk_size {
        // chunk trans
        // TODO: chunk read and write with thread
        Some(chunk_size) => {
            let arc_sync_opts = Arc::from(sink_options.clone());
            let (s, r) = if args.buffer_size == 0 {
                async_channel::unbounded::<ReadResult>()
            } else {
                async_channel::bounded::<ReadResult>(
                    usize::try_from(args.buffer_size).expect("chunk size too large"),
                )
            };
            let mut cursor: Option<SchemaTypeWithValue> = None;
            let writer = tokio::spawn(async move {
                let mut sink = load_data_storage(args.sink.as_str(), &config, &sink_options).await;
                let sink_options_inner_async = &arc_sync_opts
                    .iter()
                    .map(|(key, value)| (key.as_str(), value.as_str()))
                    .collect::<HashMap<_, _>>();
                loop {
                    if let Ok(res) = r.recv().await {
                        sink.write(
                            res.data,
                            schema.clone().or(Some(res.schema)),
                            sink_options_inner_async,
                        )
                        .await
                        .expect("chunk sink error");
                    } else {
                        break;
                    };
                }
            });
            loop {
                let res = source
                    .chunk_read(cursor, chunk_size, src_str_options)
                    .await
                    .expect("read from source error");
                cursor = res.cursor.clone();
                if res.data.is_empty() {
                    s.send(res).await;
                    break;
                }
            }
            writer.await.expect("write error");
        }
        // read all then write
        None => {
            let source_read_res = source
                .read(src_str_options)
                .await
                .expect("read from source error");
            let mut sink = load_data_storage(args.sink.as_str(), &config, &sink_options).await;
            sink.write(
                source_read_res.data,
                Some(source_read_res.schema),
                sink_str_options,
            )
            .await
            .expect("write into sink error");
        }
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    match cli.command {
        Subcommands::Trans(args) => {
            exec_trans(args).await;
        }
        Subcommands::GenExample(args) => {
            let example = Config::example();
            let f = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(args.output)
                .expect("cannot open file");
            serde_yaml::to_writer(f, &example).unwrap();
        }
    };
}
