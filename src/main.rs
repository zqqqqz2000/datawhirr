use core::panic;
use std::{collections::HashMap, str::FromStr};
mod config;

use clap::{command, Parser, Subcommand};
mod data_storages;
use config::Config;
use data_storages::{data_storages::DataStorage, loader};
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
}

#[derive(Subcommand)]
enum Subcommands {
    trans(TransOptions),
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
            if !each.contains("=") {
                panic!("please specific config in format: 'k=v'.");
            }
            let mut split = each
                .splitn(2, "=")
                .map(String::from_str)
                .map(|s| s.unwrap())
                .collect::<Vec<_>>();
            (split.swap_remove(0), split.swap_remove(1))
        })
        .collect::<HashMap<_, _>>()
}

fn load_data_storage(
    uri_or_name: String,
    config: &Option<Config>,
    config_from_args: &HashMap<String, String>,
) -> impl DataStorage {
    let r = Regex::new(r"[a-zA-Z0-9]+://.*").unwrap();
    if r.is_match(uri_or_name.as_str()) {
        loader::load_data_storage(uri_or_name, config_from_args)
    } else {
        match config {
            Some(c) => {
                let storage = c
                    .data_storages
                    .get(&uri_or_name)
                    .expect("cannot find any storages names: {uri_or_name} in config file.");
                let mut options_from_config = storage.options.clone();
                options_from_config.extend(config_from_args.clone());
                loader::load_data_storage(storage.uri.clone(), &options_from_config)
            }
            None => {
                panic!("sdfsdf")
            }
        }
    }
}

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Subcommands::trans(args) => {
            let config: Option<Config> = match args.config {
                Some(config_path) => {
                    let f =
                        std::fs::File::open(config_path).expect("cannot open file {config_path}");
                    Some(serde_yaml::from_reader(f).unwrap())
                }
                None => None,
            };
            let source =
                load_data_storage(args.source, &config, &convert_option(args.source_option));
            let sink = load_data_storage(args.sink, &config, &convert_option(args.sink_option));
        }
    };
}
