pub mod wal;
pub mod engine;
pub mod memtable;
pub mod sstable;
pub mod sstable_reader;
pub mod manifest;
pub mod version;
pub mod compaction;
mod fsutil;
mod eventlog;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
pub const SERVER_ADDR: &str = "127.0.0.1:7878";
const ACTIVE_CONFIG_NAME: &str = ".active";
#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    Put { key: String, value: String },
    Get { key: String },
    Del { key: String },
    Stats,
    MemtableDump,
    Close,
    ListSst,
    FlushNow,
    Configure {
        name: String,
        data_dir: Option<String>,
        memtable_max_bytes: Option<u64>,
        max_immutable_tables: Option<u32>,
        memtable_size_overhead_bytes_per_entry: Option<usize>,
        rotation_cooldown_ms: Option<u64>,
        block_size: Option<u32>,
        bloom_false_positive: Option<f64>,
        wal_fsync_every_n: Option<u32>,
        wal_segment_roll_bytes: Option<u64>,
        compression: Option<String>,
        log_level: Option<String>,
        sst_dir: Option<String>,
        restart_interval: Option<usize>,
        max_build_buffer_mb: Option<usize>,
        block_cache_mb: Option<usize>,
        cache_index_blocks: Option<bool>,
        max_open_files: Option<usize>,
        manifest_path: Option<String>,
        publish_log_level: Option<String>,
        event_log_path: Option<String>,
        size_tiered_fan_in: Option<u32>,
        size_tiered_size_ratio: Option<f64>,
        tombstone_grace_seconds: Option<u64>,
        compaction_max_concurrent: Option<u32>,
        compaction_io_mb_per_s: Option<u32>,
        l0_compaction_trigger: Option<u32>,
        l0_stop_writes: Option<u32>,
        bg_tick_ms: Option<u64>,
        shutdown_timeout_ms: Option<u64>,
    },
    ManifestInfo,
    VersionInfo,
    CompactionRun { files: Option<Vec<u64>> },
    CompactionStats,
    CompactionPause,
    CompactionResume,
    BgStatus,
    Shutdown { graceful: bool },
}
#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Ok(String),
    Error(String),
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub data_dir: String,
    pub memtable_max_bytes: u64,
    pub max_immutable_tables: u32,
    pub memtable_size_overhead_bytes_per_entry: usize,
    pub rotation_cooldown_ms: u64,
    pub block_size: u32,
    pub bloom_false_positive: f64,
    pub wal_fsync_every_n: u32,
    pub wal_segment_roll_bytes: u64,
    pub compression: String,
    pub log_level: String,
    pub sst_dir: String,
    pub restart_interval: usize,
    pub max_build_buffer_mb: usize,
    pub block_cache_mb: usize,
    pub cache_index_blocks: bool,
    pub max_open_files: usize,
    pub manifest_path: String,
    pub publish_log_level: String,
    pub event_log_path: String,
    pub size_tiered_fan_in: u32,
    pub size_tiered_size_ratio: f64,
    pub tombstone_grace_seconds: u64,
    pub compaction_max_concurrent: u32,
    pub compaction_io_mb_per_s: u32,
    pub l0_compaction_trigger: u32,
    pub l0_stop_writes: u32,
    pub bg_tick_ms: u64,
    pub shutdown_timeout_ms: u64,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            data_dir: "./data".to_string(),
            memtable_max_bytes: 67_108_864,
            max_immutable_tables: 4,
            memtable_size_overhead_bytes_per_entry: 32,
            rotation_cooldown_ms: 0,
            block_size: 8192,
            bloom_false_positive: 0.01,
            wal_fsync_every_n: 1,
            wal_segment_roll_bytes: 134_217_728,
            compression: "off".to_string(),
            log_level: "info".to_string(),
            sst_dir: "data/sst".to_string(),
            restart_interval: 16,
            max_build_buffer_mb: 32,
            block_cache_mb: 64,
            cache_index_blocks: true,
            max_open_files: 1024,
            manifest_path: "data/manifest.json".to_string(),
            publish_log_level: "info".to_string(),
            event_log_path: "data/events.log".to_string(),
            size_tiered_fan_in: 4,
            size_tiered_size_ratio: 2.0,
            tombstone_grace_seconds: 86_400,
            compaction_max_concurrent: 1,
            compaction_io_mb_per_s: 0,
            l0_compaction_trigger: 8,
            l0_stop_writes: 20,
            bg_tick_ms: 500,
            shutdown_timeout_ms: 5000,
        }
    }
}
impl Config {
    pub fn emergency_default() -> Self {
        Self::default()
    }
    pub fn resolve_path(input: &str) -> Option<String> {
        let candidates = vec![
            input.to_string(),
            format!("{input}.json"),
            format!("config/{input}"),
            format!("config/{input}.json"),
        ];
        for path in candidates {
            if Path::new(&path).exists() && Path::new(&path).is_file() {
                return Some(path);
            }
        }
        None
    }
    fn try_load_file(path: &str) -> Option<Self> {
        let resolved = Self::resolve_path(path)?;
        let contents = fs::read_to_string(&resolved).ok()?;
        serde_json::from_str::<Config>(&contents).ok()
    }
    pub fn load_config(path: &str) -> Result<Self, String> {
        match Self::try_load_file(path) {
            Some(cfg) => Ok(cfg),
            None => Err(format!("Config file not found for input: '{path}'")),
        }
    }
    pub fn load_default_config() -> Self {
        if let Some(cfg) = Self::try_load_file("config/default.json") {
            cfg
        } else {
            Self::emergency_default()
        }
    }
    pub fn load_active_config() -> Self {
        let active_path = format!("config/{ACTIVE_CONFIG_NAME}.json");
        if let Some(cfg) = Self::try_load_file(&active_path) {
            return cfg;
        }
        Self::load_default_config()
    }
    pub fn set_as_active(&self) {
        let _ = self.save_config(ACTIVE_CONFIG_NAME);
    }
    pub fn save_config(&self, name: &str) -> std::io::Result<()> {
        let path = if name.ends_with(".json") || name.contains('/') {
            name.to_string()
        } else {
            format!("config/{name}.json")
        };
        if let Some(parent) = Path::new(&path).parent() {
            fs::create_dir_all(parent)?;
        }
        let json_text = serde_json::to_string_pretty(self)
            .expect("Config should always serialize to JSON");
        fs::write(path, json_text)
    }
}