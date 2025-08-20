#![deny(
    clippy::disallowed_methods,
    clippy::suspicious,
    clippy::style,
    clippy::clone_on_ref_ptr
)]
#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

mod clickhouse_handler;
use dotenv::dotenv;
use std::env;

use std::path::PathBuf;
use clap::Parser;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use yellowstone_vixen as vixen;
use yellowstone_vixen::Pipeline;
use yellowstone_vixen_parser::token_holders::TokenHoldingParser;
use yellowstone_vixen_yellowstone_grpc_source::YellowstoneGrpcSource;

use crate::clickhouse_handler::ClickHouseAsyncHandler;

#[derive(clap::Parser)]
#[command(version, author, about)]
pub struct Opts {
    #[arg(long, short)]
    config: PathBuf,
    //
    // #[arg(long, default_value = "http://162.245.191.2:8123")]
    // clickhouse_url: String,

    // #[arg(long, default_value = "balance_portfolio")]
    // clickhouse_database: String,
    //
    // /// Base table name; code will create <base>_events, <base>_latest and <base>_latest_mv
    // #[arg(long, default_value = "balance")]
    // clickhouse_table: String,

    #[arg(long)]
    show_stats: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    dotenv().ok();
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .init();

    let Opts {
        config,
        show_stats,
    } = Opts::parse();
    let clickhouse_url = env::var("CLICKHOUSE_URL")
        .unwrap_or_else(|_| "http://localhost:8123".to_string());

    let clickhouse_database = env::var("CLICKHOUSE_DATABASE")
        .unwrap_or_else(|_| "balance_portfolio".to_string());
    let clickhouse_table = env::var("TABLE_NAME")
        .unwrap_or_else(|_| "balance".to_string());
    // Initialize ClickHouse handler (async inserts w/ flush ack)
    let ch = ClickHouseAsyncHandler::new(
        &clickhouse_url,
        &clickhouse_database,
        &clickhouse_table,
    );

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        ch.create_schema().await?;
        if show_stats {
            tracing::info!("Schema created; ready to ingest");
        }
        ch.start_background_ingestion().await?;
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    })?;

    tracing::info!(
    "Starting Vixen (DB: {}/{}_{{events,latest}})",
    clickhouse_database,
    clickhouse_table
);

    // Load Vixen config (your Yellowstone endpoint + token)
    let cfg_str = std::fs::read_to_string(config).expect("Error reading config file");
    let cfg = toml::from_str(&cfg_str).expect("Error parsing config");

    vixen::Runtime::builder()
        .transaction(Pipeline::new(TokenHoldingParser, [ch]))
        .source(YellowstoneGrpcSource::new())
        .build(cfg)
        .run();

    Ok(())
}
