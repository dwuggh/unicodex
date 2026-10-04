use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(version, about = "Authenticated HTTP and WebSocket model proxy")]
pub struct Cli {
    #[arg(short, long, default_value = "config.yaml")]
    pub config: PathBuf,
}
