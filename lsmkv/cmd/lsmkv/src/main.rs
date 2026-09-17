use clap::{Parser, Subcommand};
use lsm::engine::LsmEngine;
use lsm::{Config, Request, Response, SERVER_ADDR};
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
#[derive(Parser)]
#[command(name = "lsmkv")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}
#[derive(Subcommand)]
enum Commands {
    Init {
        #[arg(long)]
        config: Option<String>,
    },
    Put {
        #[arg(long)]
        key: String,
        #[arg(long)]
        value: String,
    },
    Get {
        #[arg(long)]
        key: String,
    },
    Del {
        #[arg(long)]
        key: String,
    },
    Stats,
    MemtableDump,
    FlushNow,
    Close,
    ListSst,
    VerifySst {
        #[arg(long)]
        file: String,
    },
    SstInfo {
        #[arg(long)]
        file: String,
    },
    ManifestInfo,
    VersionInfo,
    CompactionRun {
        #[arg(long)]
        files: Option<String>,
    },
    CompactionStats,
    CompactionPause,
    CompactionResume,
    BgStatus,
    Shutdown {
        #[arg(long)]
        graceful: bool,
        #[arg(long)]
        fast: bool,
    },
    Configure {
        #[arg(long)]
        name: String,
        #[arg(long)]
        data_dir: Option<String>,
        #[arg(long)]
        memtable_max_bytes: Option<u64>,
        #[arg(long)]
        max_immutable_tables: Option<u32>,
        #[arg(long)]
        memtable_size_overhead_bytes_per_entry: Option<usize>,
        #[arg(long)]
        rotation_cooldown_ms: Option<u64>,
        #[arg(long)]
        block_size: Option<u32>,
        #[arg(long)]
        bloom_false_positive: Option<f64>,
        #[arg(long)]
        wal_fsync_every_n: Option<u32>,
        #[arg(long)]
        wal_segment_roll_bytes: Option<u64>,
        #[arg(long)]
        compression: Option<String>,
        #[arg(long)]
        log_level: Option<String>,
        #[arg(long)]
        sst_dir: Option<String>,
        #[arg(long)]
        restart_interval: Option<usize>,
        #[arg(long)]
        max_build_buffer_mb: Option<usize>,
        #[arg(long)]
        block_cache_mb: Option<usize>,
        #[arg(long)]
        cache_index_blocks: Option<bool>,
        #[arg(long)]
        max_open_files: Option<usize>,
        #[arg(long)]
        manifest_path: Option<String>,
        #[arg(long)]
        publish_log_level: Option<String>,
        #[arg(long)]
        event_log_path: Option<String>,
        #[arg(long)]
        size_tiered_fan_in: Option<u32>,
        #[arg(long)]
        size_tiered_size_ratio: Option<f64>,
        #[arg(long)]
        tombstone_grace_seconds: Option<u64>,
        #[arg(long)]
        compaction_max_concurrent: Option<u32>,
        #[arg(long)]
        compaction_io_mb_per_s: Option<u32>,
        #[arg(long)]
        l0_compaction_trigger: Option<u32>,
        #[arg(long)]
        l0_stop_writes: Option<u32>,
        #[arg(long)]
        bg_tick_ms: Option<u64>,
        #[arg(long)]
        shutdown_timeout_ms: Option<u64>,
    },
    WalVerify,
    WalTruncate {
        #[arg(long)]
        segment: String,
        #[arg(long)]
        offset: u64,
    },
    WalDump {
        #[arg(long)]
        segment: String,
    },
}
fn main() {
    let cli = Cli::parse();
    match cli.command {
        Commands::Init { config } => run_server(config),
        Commands::Put { key, value } => send_request(Request::Put { key, value }),
        Commands::Get { key } => send_request(Request::Get { key }),
        Commands::Del { key } => send_request(Request::Del { key }),
        Commands::Stats => send_request(Request::Stats),
        Commands::MemtableDump => send_request(Request::MemtableDump),
        Commands::FlushNow => send_request(Request::FlushNow),
        Commands::Close => send_request(Request::Close),
        Commands::ListSst => send_request(Request::ListSst),
        Commands::ManifestInfo => send_request(Request::ManifestInfo),
        Commands::VersionInfo => send_request(Request::VersionInfo),
        Commands::BgStatus => send_request(Request::BgStatus),
        Commands::Shutdown { graceful, fast } => {
            if fast {send_request(Request::Close);}
            else{send_request(Request::Shutdown { graceful });}
        }
        Commands::VerifySst { file } => {
            let cnf = Config::load_active_config();
            let sst_dir = std::path::Path::new(&cnf.sst_dir);
            let full_path = sst_dir.join(&file);
            match lsm::sstable::verify(&full_path) {
                Ok(report) => println!("{}", report),
                Err(e) => eprintln!("Greška pri verifikaciji (putanja: {}): {}", full_path.display(), e),
            }
        }
        Commands::SstInfo { file } => {
            let cnf = Config::load_active_config();
            let sst_dir = std::path::Path::new(&cnf.sst_dir);
            let full_path = sst_dir.join(&file);
            match lsm::sstable::sst_info(&full_path) {
                Ok(report) => println!("{}", report),
                Err(e) => eprintln!("Greška pri čitanju info-a (putanja: {}): {}", full_path.display(), e),
            }
        }
        Commands::Configure {
            name,
            data_dir,
            memtable_max_bytes,
            max_immutable_tables,
            memtable_size_overhead_bytes_per_entry,
            rotation_cooldown_ms,
            block_size,
            bloom_false_positive,
            wal_fsync_every_n,
            wal_segment_roll_bytes,
            compression,
            log_level,
            sst_dir,
            restart_interval,
            max_build_buffer_mb,
            block_cache_mb,
            cache_index_blocks,
            max_open_files,
            manifest_path,
            publish_log_level,
            event_log_path,
            size_tiered_fan_in,
            size_tiered_size_ratio,
            tombstone_grace_seconds,
            compaction_max_concurrent,
            compaction_io_mb_per_s,
            l0_compaction_trigger,
            l0_stop_writes,
            bg_tick_ms,
            shutdown_timeout_ms,
        } => {
            send_request(Request::Configure {
                name,
                data_dir,
                memtable_max_bytes,
                max_immutable_tables,
                memtable_size_overhead_bytes_per_entry,
                rotation_cooldown_ms,
                block_size,
                bloom_false_positive,
                wal_fsync_every_n,
                wal_segment_roll_bytes,
                compression,
                log_level,
                sst_dir,
                restart_interval,
                max_build_buffer_mb,
                block_cache_mb,
                cache_index_blocks,
                max_open_files,
                manifest_path,
                publish_log_level,
                event_log_path,
                size_tiered_fan_in,
                size_tiered_size_ratio,
                tombstone_grace_seconds,
                compaction_max_concurrent,
                compaction_io_mb_per_s,
                l0_compaction_trigger,
                l0_stop_writes,
                bg_tick_ms,
                shutdown_timeout_ms,
            });
        }
        Commands::WalVerify => {
            let cfg = Config::load_active_config();
            if let Err(e) = lsm::wal::Wal::verify(&cfg.data_dir) {
                eprintln!("error verifying WAL in '{}/': {}", cfg.data_dir, e);
            }
        }
        Commands::WalTruncate { segment, offset } => {
            let cfg = Config::load_active_config();
            if let Err(e) = lsm::wal::Wal::manual_truncate(&cfg.data_dir, &segment, offset) {
                eprintln!("error truncating WAL segment in '{}/': {}", cfg.data_dir, e);
            }
        }
        Commands::WalDump { segment } => {
            let cfg = Config::load_active_config();
            if let Err(e) = lsm::wal::Wal::dump(&cfg.data_dir, &segment) {
                eprintln!("greška pri čitanju WAL-a u '{}/': {}", cfg.data_dir, e);
            }
        }
        Commands::CompactionRun { files } => {
            let parsed = match files {
                Some(raw) => {
                    let mut ids = Vec::new();
                    let mut parse_ok = true;
                    for token in raw.split(',') {
                        let token = token.trim();
                        if token.is_empty() { continue; }
                        match token.parse::<u64>() {
                            Ok(id) => ids.push(id),
                            Err(_) => {
                                eprintln!(
                                    "error: invalid SSTable id '{token}' in --files \
                                    (expected comma-separated numbers, e.g. 000101,000103)"
                                );
                                parse_ok = false;
                                break;
                            }
                        }
                    }
                    if !parse_ok {
                        std::process::exit(1);
                    }
                    Some(ids)
                }
                None => None,
            };
            send_request(Request::CompactionRun { files: parsed });
        }
        Commands::CompactionStats => send_request(Request::CompactionStats),
        Commands::CompactionPause => send_request(Request::CompactionPause),
        Commands::CompactionResume => send_request(Request::CompactionResume),
    }
}
fn run_server(config_path: Option<String>) {
    let cfg = match config_path {
        Some(ref path) => match Config::load_config(path) {
            Ok(c) => {
                println!("loaded config from '{path}' ✓");
                c
            }
            Err(e) => {
                eprintln!("error: {e}");
                eprintln!("falling back to default config...");
                Config::load_default_config()
            }
        },
        None => {
            println!("loaded active config ✓");
            Config::load_default_config()
        }
    };
    cfg.set_as_active();
    println!("active data dir: '{}' ✓", cfg.data_dir);
    // arc omogucava da se LSMEngine vlasnistvo deli izmedju vise niti
    let engine = Arc::new(LsmEngine::new(cfg).expect("Failed to initialize LsmEngine"));
    println!("WAL and Memtable recovered ✓");
    {
        let e = Arc::clone(&engine);
        thread::spawn(move || LsmEngine::run_flush_worker(&e));
    }
    {
        let e = Arc::clone(&engine);
        thread::spawn(move || LsmEngine::run_compaction_worker(&e));
    }
    println!("background workers started (FlushWorker, CompactionWorker) ✓");
    let listener = TcpListener::bind(SERVER_ADDR)
        .expect("failed to bind server address — is lsmkv already running?");
    println!("lsmkv server listening on {SERVER_ADDR}");
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    thread::spawn(move || {
        let stdin = std::io::stdin();
        let mut line = String::new();
        while let Ok(n) = stdin.read_line(&mut line) {
            if n == 0 { break; }
            if line.trim().eq_ignore_ascii_case("close") {
                if let Ok(mut stream) = TcpStream::connect(SERVER_ADDR) {
                    let req = serde_json::to_string(&Request::Close).unwrap();
                    let _ = writeln!(stream, "{req}");
                }
                break;
            }
            line.clear();
        }
    });
    for stream in listener.incoming() {
        if shutdown_flag.load(Ordering::SeqCst) {
            println!("closed — shutting down");
            break;
        }
        match stream {
            Ok(stream) => {
                let engine = Arc::clone(&engine);
                let shutdown_flag = Arc::clone(&shutdown_flag);
                thread::spawn(move || {
                    handle_connection(stream, &engine, &shutdown_flag);
                });
            }
            Err(e) => eprintln!("connection failed: {e}"),
        }
    }
}
fn handle_connection(mut stream: TcpStream, engine: &Arc<LsmEngine>, shutdown_flag: &AtomicBool) {
    let mut reader = BufReader::new(stream.try_clone().expect("failed to clone stream"));
    let mut line = String::new();
    if reader.read_line(&mut line).unwrap_or(0) == 0 {
        return;
    }
    let request: Request = match serde_json::from_str(line.trim()) {
        Ok(r) => r,
        Err(e) => {
            send_response(&mut stream, &Response::Error(format!("bad request: {e}")));
            return;
        }
    };
    let (response, should_shutdown) = match request {
        Request::Put { key, value } => {
            match engine.put(key, value) {
                Ok(msg) => (Response::Ok(msg), false),
                Err(e) => (Response::Error(e), false),
            }
        }
        Request::Del { key } => {
            match engine.delete(key) {
                Ok(msg) => (Response::Ok(msg), false),
                Err(e) => (Response::Error(e), false),
            }
        }
        Request::Stats => {
            (Response::Ok(engine.get_stats_string()), false)
        }
        Request::MemtableDump => {
            (Response::Ok(engine.dump_memtables()), false)
        }
        Request::ListSst => {
            (Response::Ok(engine.list_ssts_string()), false)
        }
        Request::ManifestInfo => {
            (Response::Ok(engine.manifest_info_string()), false)
        }
        Request::VersionInfo => {
            (Response::Ok(engine.version_info_string()), false)
        }
        Request::BgStatus => {
            (Response::Ok(engine.bg_status_string()), false)
        }
        Request::FlushNow => {
            match engine.flush_immutable() {
                Ok(msg) => (Response::Ok(msg), false),
                Err(e) => (Response::Error(e), false),
            }
        }
        Request::Get { key } => {
            match engine.get(&key) {
                Ok(msg) => (Response::Ok(msg), false),
                Err(e) => (Response::Error(e), false),
            }
        }
        Request::Close => {
            match engine.shutdown(false) {
                Ok(msg) => (Response::Ok(msg), true),
                Err(e) => (Response::Error(e), true),
            }
        }
        Request::Shutdown { graceful } => {
            match engine.shutdown(graceful) {
                Ok(msg) => (Response::Ok(msg), true),
                Err(e) => (Response::Error(e), true),
            }
        }
        Request::Configure {
            name,
            data_dir,
            memtable_max_bytes,
            max_immutable_tables,
            memtable_size_overhead_bytes_per_entry,
            rotation_cooldown_ms,
            block_size,
            bloom_false_positive,
            wal_fsync_every_n,
            wal_segment_roll_bytes,
            compression,
            log_level,
            sst_dir,
            restart_interval,
            max_build_buffer_mb,
            block_cache_mb,
            cache_index_blocks,
            max_open_files,
            manifest_path,
            publish_log_level,
            event_log_path,
            size_tiered_fan_in,
            size_tiered_size_ratio,
            tombstone_grace_seconds,
            compaction_max_concurrent,
            compaction_io_mb_per_s,
            l0_compaction_trigger,
            l0_stop_writes,
            bg_tick_ms,
            shutdown_timeout_ms,
        } => {
            match engine.configure(
                name,
                data_dir,
                memtable_max_bytes,
                max_immutable_tables,
                memtable_size_overhead_bytes_per_entry,
                rotation_cooldown_ms,
                block_size,
                bloom_false_positive,
                wal_fsync_every_n,
                wal_segment_roll_bytes,
                compression,
                log_level,
                sst_dir,
                restart_interval,
                max_build_buffer_mb,
                block_cache_mb,
                cache_index_blocks,
                max_open_files,
                manifest_path,
                publish_log_level,
                event_log_path,
                size_tiered_fan_in,
                size_tiered_size_ratio,
                tombstone_grace_seconds,
                compaction_max_concurrent,
                compaction_io_mb_per_s,
                l0_compaction_trigger,
                l0_stop_writes,
                bg_tick_ms,
                shutdown_timeout_ms,
            ) {
                Ok(msg) => (Response::Ok(msg), false),
                Err(e) => (Response::Error(e), false),
            }
        }
        Request::CompactionRun { files } => {
            match engine.compaction_run(files) {
                Ok(msg) => (Response::Ok(msg), false),
                Err(e) => (Response::Error(e), false),
            }
        }
        Request::CompactionStats => {
            (Response::Ok(engine.compaction_stats_string()), false)
        }
        Request::CompactionPause => {
            match engine.compaction_pause() {
                Ok(msg) => (Response::Ok(msg), false),
                Err(e) => (Response::Error(e), false),
            }
        }
        Request::CompactionResume => {
            match engine.compaction_resume() {
                Ok(msg) => (Response::Ok(msg), false),
                Err(e) => (Response::Error(e), false),
            }
        }
    };
    send_response(&mut stream, &response);
    if should_shutdown {
        shutdown_flag.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(SERVER_ADDR);
    }
}
fn send_response(stream: &mut TcpStream, response: &Response) {
    let json_text = serde_json::to_string(response).expect("Response should always serialize");
    let _ = writeln!(stream, "{json_text}");
}
fn send_request(request: Request) {
    let mut stream = match TcpStream::connect(SERVER_ADDR) {
        Ok(s) => s,
        Err(_) => {
            eprintln!("error: lsmkv is not running — run 'lsmkv init' in another terminal first");
            std::process::exit(1);
        }
    };
    let json_text = serde_json::to_string(&request).expect("Request should always serialize");
    if let Err(e) = writeln!(stream, "{json_text}") {
        eprintln!("error: failed to send request: {e}");
        std::process::exit(1);
    }
    let mut reader = BufReader::new(&stream);
    let mut line = String::new();
    if reader.read_line(&mut line).unwrap_or(0) == 0 {
        eprintln!("error: server closed the connection without responding");
        std::process::exit(1);
    }
    match serde_json::from_str::<Response>(line.trim()) {
        Ok(Response::Ok(msg)) => println!("{msg}"),
        Ok(Response::Error(msg)) => {
            eprintln!("error: {msg}");
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("error: could not understand server response: {e}");
            std::process::exit(1);
        }
    }
}