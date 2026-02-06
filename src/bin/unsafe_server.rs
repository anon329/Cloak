use std::io::{self, BufReader, BufWriter, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};

use clap::Parser;
use cloak_rust::config::{Config, ConfigArgs};
use cloak_rust::shared_types::{ClientRequest, ClientResponse, Shared, RequestType};

use env_logger::Builder;
use log::{LevelFilter, info};

/// Command line arguments for the unsafe storage server
#[derive(Parser, Debug)]
struct Args {
    #[command(flatten)]
    config: ConfigArgs,
}

/// Unsafe storage server that directly serves clients without ORAM protection.
/// This is a baseline implementation for performance comparison.
/// Data is stored in plaintext without any oblivious access patterns.
struct UnsafeStorage {
    /// Flat buffer storing all values contiguously
    data: Vec<u8>,
    /// Size of each object for index calculation
    object_size: usize,
}

impl UnsafeStorage {
    /// Creates a new unsafe storage instance with pre-allocated capacity
    fn new(object_size: usize, storage_size: usize) -> UnsafeStorage {
        UnsafeStorage {
            data: vec![0u8; object_size * storage_size],
            object_size,
        }
    }

    /// Handles client connections and processes read/write requests directly.
    /// Protocol:
    /// 1. Read ClientRequest (request_id, request_type, key, value)
    /// 2. Process READ/WRITE operation
    /// 3. Send ClientResponse back
    fn handle_client(&mut self, mut reader: BufReader<TcpStream>, mut writer: BufWriter<TcpStream>) -> io::Result<()> {
        loop {
            // Read the client request
            let request = match ClientRequest::read_one(&mut reader) {
                Ok(req) => req,
                Err(e) => {
                    info!("Client disconnected or error reading request: {}", e);
                    return Err(e);
                }
            };

            info!("Received request: id={}, type={:?}, key={}",
                request.request_id, request.request_type, request.key);

            let offset = request.key as usize * self.object_size;
            let response = match request.request_type {
                RequestType::READ => {
                    // Return the stored value
                    let value = self.data[offset..offset + self.object_size].to_vec();

                    ClientResponse {
                        request_id: request.request_id,
                        request_type: RequestType::READ,
                        key: request.key,
                        value,
                    }
                }
                RequestType::WRITE => {
                    // Store the value and return it as confirmation
                    self.data[offset..offset + self.object_size].copy_from_slice(&request.value);

                    ClientResponse {
                        request_id: request.request_id,
                        request_type: RequestType::WRITE,
                        key: request.key,
                        value: request.value,
                    }
                }
            };

            // Send the response
            let response_bytes = response.to_bytes();
            writer.write_all(&response_bytes)?;
            writer.flush()?;

            info!("Sent response for request_id={}", response.request_id);
        }
    }
}

fn main() -> io::Result<()> {
    Builder::new()
        .format_timestamp_millis()
        .filter_level(LevelFilter::Off)
        .init();

    let args = Args::parse();

    // Read and parse the configuration file, applying any overrides
    let config = Config::from_args(&args.config)?;

    // Listen on the proxy address since clients connect there
    let addr: SocketAddr = config.common.proxy_addr_bind.parse()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Invalid proxy address: {}", e)))?;

    let listener = TcpListener::bind(addr)?;

    println!("Unsafe server listening on '{}' (no ORAM protection)", addr);

    // Accept and handle client connections
    for stream in listener.incoming() {
        if let Ok(stream) = stream {
            println!("Client connected from {}", stream.peer_addr()?);

            let reader = BufReader::new(stream.try_clone()?);
            let writer = BufWriter::new(stream);

            // Create storage and start handling requests
            let mut storage = UnsafeStorage::new(config.common.object_size, config.common.storage_size);
            let _ = storage.handle_client(reader, writer)
                .map_err(|e| eprintln!("Error handling client: {}", e));

            println!("Client disconnected");
        }
    }

    Ok(())
}
