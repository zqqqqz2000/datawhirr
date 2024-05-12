use clap::Parser;
mod datawhirr;

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// config path for source and sink.
    #[arg(short, long)]
    config: Option<String>,
    /// source of data, could be name in config or a protocol.
    /// e.g. mysql://xxxx:xxxx/xxxx
    #[arg(long)]
    source: Option<String>,
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
    sink: Option<String>,
    /// schema for sink.
    #[arg(long)]
    sink_schema: Option<String>,
}

fn main() {
    let args = Args::parse();
}
