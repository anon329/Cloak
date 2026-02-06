use std::io::{self, BufReader, BufWriter, prelude::*};
use std::net::{SocketAddr, TcpListener, TcpStream};
use ndarray::{Array2, s};
use bytemuck::cast_slice;
use clap::Parser;
use cloak_rust::config::{Config, ConfigArgs};

use env_logger::Builder;
use log::{LevelFilter,info};

/// Command line arguments for configuring the storage server
#[derive(Parser, Debug)]
struct Args {
    #[command(flatten)]
    config: ConfigArgs,
}

enum RequestType {
    ReadWrite = 0,
    Read = 1,
    Write = 2,
}

/// Storage server that holds encrypted objects in memory
/// Provides batch read/write operations for the ORAM proxy
struct Storage {
    /// 2D array storing encrypted objects, each row is an object
    data: Array2<u8>,
    /// Temporary buffer for processing batch requests
    auxiliary_memory: Vec<u8>
}

impl Storage {
    /// Creates a new storage instance with given dimensions
    /// - storage_size: Number of objects to store
    /// - object_size: Size of each object in bytes (includes encryption overhead)
    /// - max_read_size: Maximum batch size for read operations
    fn new(storage_size: usize, object_size: usize, max_read_size: usize) -> Storage {
        Storage {
            data: Array2::<u8>::zeros((storage_size, object_size + 32)),
            auxiliary_memory: vec![0u8; max_read_size*4]
        }
    }

    /// Handles client (proxy) connections and processes read/write requests
    /// Protocol:
    /// 1. Read batch size (u32)
    /// 2. Read array of indices to access
    /// 3. Send requested objects
    /// 4. Receive new objects to write back
    /// 5. Send acknowledgment
    fn handle_client(&mut self, mut reader: BufReader<TcpStream>, mut writer: BufWriter<TcpStream>) -> io::Result<()> {

        loop {
            // Read request type
            let req_type = self.get_request_type(&mut reader)?;

            match req_type {
                RequestType::ReadWrite => {
                    info!("handling read-write request");
                    let req_length = self.get_request_size(&mut reader)?;
                    info!("request length: {}", req_length);
                    let index_array = self.read_indices(&mut reader, req_length)?;
                    info!("read indices");
                    self.send_requested_objects(&mut writer, &index_array)?;
                    info!("sent requested objects");
                    self.receive_and_write_objects(&mut reader, &index_array)?;
                    
                    writer.write_all(&[0u8])?;
                    writer.flush()?;
                },
                RequestType::Read => {
                    let req_length = self.get_request_size(&mut reader)?;
                    let index_array = self.read_indices(&mut reader, req_length)?;
                    self.send_requested_objects(&mut writer, &index_array)?;
                    
                    writer.write_all(&[0u8])?;
                    writer.flush()?;
                },
                RequestType::Write => {
                    let req_length = self.get_request_size(&mut reader)?;
                    let index_array = self.read_indices(&mut reader, req_length)?;
                    self.receive_and_write_objects(&mut reader, &index_array)?;
                    
                    writer.write_all(&[0u8])?;
                    writer.flush()?;
                }
            }

        }
    }

    fn get_request_type(&self, reader: &mut BufReader<TcpStream>) -> io::Result<RequestType> {
        let mut req_type = [0u8; 1];
        reader.read_exact(&mut req_type)?;
        match req_type[0] {
            0 => Ok(RequestType::ReadWrite),
            1 => Ok(RequestType::Read),
            2 => Ok(RequestType::Write),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid request type")),
        }
    }

    fn get_request_size(&self, reader: &mut BufReader<TcpStream>) -> io::Result<usize> {
        let mut req_size = [0u8; 4];
        reader.read_exact(&mut req_size)?;
        info!("received request size bytes: {:x?}", req_size);
        Ok(u32::from_ne_bytes(req_size) as usize)
    }

    fn read_indices(&mut self, reader: &mut BufReader<TcpStream>, req_length: usize) -> io::Result<Vec<u32>> {
        let mut index_array = &mut self.auxiliary_memory[..req_length*4];
        reader.read_exact(&mut index_array)?;
        Ok(cast_slice::<u8, u32>(&index_array).to_vec())
    }

    fn send_requested_objects(&self, writer: &mut BufWriter<TcpStream>, index_array: &[u32]) -> io::Result<()> {
        
        info!("length of requested indices: {}", index_array.len());
        for index in index_array {
            let object = self.data.slice(s![*index as usize, ..]);
            let object_slice = object.as_slice().unwrap();
            writer.write_all(object_slice)?;
        }
        writer.flush()?;
        Ok(())
    }

    fn receive_and_write_objects(&mut self, reader: &mut BufReader<TcpStream>, index_array: &[u32]) -> io::Result<()> {
        for index in index_array {
            let mut object = self.data.slice_mut(s![*index as usize, ..]);
            let mut object_slice = object.as_slice_mut().unwrap();
            reader.read_exact(&mut object_slice)?;
        }
        Ok(())
    }
}

/// Reads initial configuration from the proxy
/// Returns (storage_size, object_size)
fn get_config(reader: &mut BufReader<TcpStream>) -> io::Result<(usize, usize)> {
    let mut config = [0u8; 8];
    reader.read_exact(&mut config)?;
    let storage_size = u32::from_ne_bytes(config[..4].try_into().unwrap()) as usize;
    let object_size = u32::from_ne_bytes(config[4..].try_into().unwrap()) as usize;
    Ok((storage_size, object_size))
}

fn main() -> io::Result<()> {
    Builder::new()
        .format_timestamp_millis()
        .filter_level(LevelFilter::Off)
        .init();

    let args = Args::parse();
    
    // Read and parse the configuration file, applying any overrides
    let config = Config::from_args(&args.config)?;

    let addr: SocketAddr = config.common.server_addr_bind.parse()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Invalid server address: {}", e)))?;
    
    let listener = TcpListener::bind(addr)?;

    println!("Starting server on '{}'", addr);

    // Accept connections and handle them
    for stream in listener.incoming() {
        if let Ok(stream) = stream {
            // Get storage configuration from proxy
            let mut reader = BufReader::new(stream.try_clone()?);
            let writer = BufWriter::new(stream.try_clone()?);
            let (storage_size, object_size) = get_config(&mut reader)?;

            dbg!(storage_size);
            dbg!(object_size);
            
            // Create storage and start handling requests
            let mut storage = Storage::new(storage_size, object_size, config.server.max_read_size);
            let _ = storage.handle_client(reader, writer).map_err(|e| eprintln!("Error: {}", e));
        }
    }
    Ok(())
}
