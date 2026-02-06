# Cloak

Experimental implementation for the paper **"Cloak: Heuristic ORAM Optimization Through Fixed Temporal Distribution"**.

Cloak is an Oblivious RAM (ORAM) system that optimizes access pattern obfuscation by forcing a fixed temporal distribution to the accesses between proxy and the server. The system consists of three components: a **storage server**, an **proxy**, and a **client**.

## Architecture

```
Client  ──TCP──>  Proxy  ──TCP──>  Storage Server
                  (port 5050)            (port 4000)
```

- **Storage Server** -- Stores encrypted objects and serves batch read/write requests.
- **Proxy** -- Implements the Cloak protocol
- **Client** -- Sends read/write requests from a workload file and records latency and throughput metrics.

## Prerequisites

- Rust (stable toolchain)
- OpenSSL development libraries

### Installing dependencies

**Ubuntu/Debian:**
```bash
sudo apt install build-essential pkg-config libssl-dev
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

**macOS:**
```bash
brew install openssl pkg-config
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

## Building

```bash
cargo build --release
```

Binaries are placed in `target/release/`:
- `server`
- `proxy`
- `client`
- `create_workload` (workload generator)
- `create_workload_dist` (generates workloads for varying Zipf exponents)
- `budget_ratio` (post-experiment budget utilization analysis)

## Configuration

All components read from a shared TOML configuration file. The default is at `config/config-default.toml`:

```toml
[common]
server_addr = "127.0.0.1:4000"      # Address the proxy uses to connect to the server
server_addr_bind = "0.0.0.0:4000"   # Address the server listens on
proxy_addr = "127.0.0.1:5050"       # Address the client uses to connect to the proxy
proxy_addr_bind = "0.0.0.0:5050"    # Address the proxy listens on
object_size = 1024                  # Plaintext object size in bytes
crypto_overhead = 32                # Encryption overhead (IV + tag)
storage_size = 1000000              # Total number of objects in storage

[server]
max_read_size = 10000000

[proxy]
first_batch_size = 433              # Budget for the first time-set
queue_coefficient = 2               # Queue capacity = budget * coefficient
cache_size = 100                    # LRU cache size
num_threads = 16                    # Threads for parallel encryption/decryption
is_unsafe = false                   # Set true to disable ORAM (baseline mode)
batch_interval_ms = 20              # Batch trigger interval in milliseconds
enable_budget_log = true
budget_log_file = "benchmark/budget_log_file.txt"

[client]
num_clients = 1                     # Number of concurrent client connections
req_per_sec = 1000000               # Target request rate
input_file = "benchmark/s1.0-workload"
output_file = "benchmark/output.txt"
stats_file = "benchmark/stats.txt"
enable_output_log = true
```

All configuration values can be overridden via command-line flags using the pattern `--<section>-<field>`. For example:
```bash
--proxy-cache-size 2000 --client-req-per-sec 50000 --common-object-size 2048
```

## Running Locally

Components must be started in order: **server**, then **proxy**, then **client**.

### 1. Start the storage server

```bash
./target/release/server --config config/config-default.toml
```

Wait for the server to print that it is listening before starting the proxy.

### 2. Start the ORAM proxy

```bash
./target/release/proxy --config config/config-default.toml
```

The proxy connects to the server, initializes storage with encrypted dummy data, and then starts listening for client connections. Wait until initialization is complete before starting the client.

### 3. Start the client

```bash
./target/release/client --config config/config-default.toml
```

The client reads the workload file, sends requests to the proxy at the configured rate, and writes latency/throughput statistics to the configured output and stats files.

### Example with overrides

```bash
# Terminal 1
./target/release/server --config config/config-default.toml

# Terminal 2
./target/release/proxy --config config/config-default.toml \
    --proxy-first-batch-size 200 \
    --proxy-cache-size 1000

# Terminal 3
./target/release/client --config config/config-default.toml \
    --client-req-per-sec 50000 \
    --client-input-file benchmark/s1.0-workload
```

## Running Experiments with Ansible

The `ansible/` directory contains playbooks and scripts for running experiments on remote machines (e.g., AWS EC2 instances).

### Setup

1. **Configure the inventory file.** Edit `ansible/inventory` to specify the IP addresses of your machines:

    ```ini
    [server]
    <server-ip>

    [proxy]
    <proxy-ip>

    [client]
    <client-ip>
    ```

    Use `ansible/inventory-single` if all three components run on the same machine.

2. **Configure SSH access.** Edit `ansible/ansible.cfg` to set the SSH user and key:

    ```ini
    [defaults]
    inventory = inventory
    remote_user = ubuntu
    private_key_file = ~/.ssh/cloak.pem
    ```

3. **Deploy the project to remote machines:**

    ```bash
    cd ansible
    ansible-playbook deploy.yml
    ```

    This installs dependencies (build-essential, pkg-config, libssl-dev, Rust), copies the source code to each machine, and builds the project in release mode.

### Running a single experiment

Use the `experiment.yml` playbook. It handles the full lifecycle: copies config and workload files, starts the server/proxy/client in sequence, waits for completion, runs budget ratio analysis, and fetches results back to your local machine.

```bash
cd ansible
ansible-playbook experiment.yml -e "\
    config_file=config/config-default.toml \
    input_file=benchmark/s1.0-workload \
    output_dir=benchmark/results \
    extra_args='--client-req-per-sec 50000'" -vv
```

**Parameters:**
| Variable | Description |
|---|---|
| `config_file` | Path to the TOML config file (relative to project root) |
| `input_file` | Path to the workload file |
| `output_dir` | Local directory where results are saved |
| `extra_args` | Additional CLI flags passed to all components |

Results are saved as:
- `<output_dir>/res-stats-<workload><extra_args>.txt` -- client throughput and latency statistics
- `<output_dir>/res-ratio-<workload><extra_args>.txt` -- time-set budget utilization ratios

When the same experiment is run multiple times, subsequent results are appended to the existing files.

### Pre-built experiment scripts

The `ansible/` directory includes shell scripts for the experiments in the paper. Each script runs the `experiment.yml` playbook multiple times with different parameter configurations:

| Script | Parameter varied | Description |
|---|---|---|
| `exp-batch-size.sh` | `--proxy-first-batch-size` | First time-set budget (121 to 1537) |
| `exp-cache-size.sh` | `--proxy-cache-size` | LRU cache size (1,000 to 256,000) |
| `exp-object-size.sh` | `--common-object-size` | Object size (512B to 32KB) |
| `exp-zipf-exponent.sh` | workload file | Zipf exponent s (0.0 to 2.0) |
| `exp-diff-datasets.sh` | workload file | Different real-world datasets |

Run them from the `ansible/` directory:
```bash
cd ansible
bash exp-zipf-exponent.sh
```

Each script runs multiple iterations of each configuration. Results accumulate in the corresponding `benchmark/exp-*/results/` directory.

## Project Structure

```
.
├── src/
│   ├── bin/
│   │   ├── server.rs              # Storage server binary
│   │   ├── proxy.rs               # ORAM proxy binary
│   │   ├── client.rs              # Client binary
│   │   ├── create_workload.rs     # Workload generator
│   │   ├── create_workload_dist.rs # Multi-distribution workload generator
│   │   ├── budget_ratio.rs        # Budget utilization analysis
│   │   └── calculate_temporal_histogram.rs
│   ├── proxy_lib/
│   │   ├── mod.rs                 # ORAM protocol implementation
│   │   └── crypto_lib.rs          # AES-128-GCM encryption
│   ├── config.rs                  # Configuration parsing
│   ├── shared_types.rs            # Wire protocol types
│   ├── budget_queue.rs            # Max-heap with value-indexed deletion
│   └── lib.rs
├── config/
│   └── config-default.toml        # Default configuration
├── ansible/
│   ├── deploy.yml                 # Remote deployment playbook
│   ├── experiment.yml             # Experiment orchestration playbook
│   ├── inventory                  # Multi-machine inventory
│   ├── inventory-single           # Single-machine inventory
│   ├── ansible.cfg
│   └── exp-*.sh                   # Experiment scripts
├── benchmark/                     # Workloads, configs, and results per experiment
├── Cargo.toml
└── Cargo.lock
```

## Workload Format

Workload files are plain text with one request per line:

```
key              # READ request
key value        # WRITE request
```

Keys are integer object identifiers. For write requests, the value is used as a seed to generate test data.
