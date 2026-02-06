use cloak_rust::proxy_lib::Proxy;
use cloak_rust::config::{Config, ConfigArgs};
use clap::Parser;
use std::time::{Instant};
use std::net::TcpListener;
use mini_moka::sync::Cache;
use env_logger::Builder;
use log::{LevelFilter, info};

#[derive(Parser, Debug)]
struct CliArgs {
    #[command(flatten)]
    config: ConfigArgs,
}

/// Main entry point for the ORAM proxy server
/// - Initializes proxy with configuration from TOML file
/// - Sets up storage server connection
/// - Accepts and handles client connections
fn main() {
    // Initialize logger with timestamp
    Builder::new()
        .format_timestamp_millis()
        .filter_level(LevelFilter::Off)
        .init();

    let cli_args = CliArgs::parse();
    info!("parsed cli args");

    let config = Config::from_args(&cli_args.config).expect("Failed to read config file");
    info!("loaded config from file");
    
    // Create LRU cache for frequently accessed objects
    let cache = Cache::new(config.proxy.cache_size as u64);
    info!("initialized LRU cache with size {}", config.proxy.cache_size);
    
    // Initialize proxy and storage
    let proxy = Proxy::new(config.clone());
    info!("initialized proxy with config");
    proxy.storage_init();
    info!("initialized storage server at {}", config.common.server_addr);
    
    // Start listening for client connections
    let mut start = Instant::now();
    let listener = TcpListener::bind(&config.common.proxy_addr_bind).unwrap();
    println!("listening for clients on {}", &config.common.proxy_addr);
    
    // Accept and handle one client connection
    // Note: Current implementation only handles one client session
    for stream in listener.incoming() {
        if let Ok(stream) = stream {
            start = Instant::now();
            Proxy::handle_client_requests(proxy, stream, cache);
        }
        break;
    }
    
    let duration = start.elapsed();
    println!("Time elapsed for 1_000_000 requests is: {:?}", duration);
}
