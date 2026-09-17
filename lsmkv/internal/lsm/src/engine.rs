use crate::manifest::{Manifest, SsTableManifestEntry};
use crate::memtable::{Lookup, Memtable, MemtableOpResult};
use crate::sstable;
use crate::sstable_reader::{
    BlockCache, FileHandleCache, LookupTrace, ReadStats, TableHandle, search_sstables,
};
use crate::version::Version;
use crate::wal::{RecordType, Wal, WalRecord};
use crate::Config;
use std::collections::VecDeque;
use std::fs;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use crate::eventlog::EventLog;
use crate::compaction::{merge::MergeIterator, picker, CompactionManager, IoThrottle, JobStats};
use std::collections::HashSet;
use std::path::{Path, PathBuf};

/// Wake-up mehanizam za pozadinske workere: kombinuje periodični tick
/// (bg_tick_ms) sa trenutnim buđenjem preko notify(). "pending" flag rešava
/// lost-wakeup problem — ako notify() stigne pre nego što worker stigne da
/// pozove wait(), sledeći wait() se odmah vraća umesto da čeka pun tick.
struct WorkerSignal {
    pending: Mutex<bool>,
    condvar: Condvar,
}

impl WorkerSignal {
    fn new() -> Self {
        Self { pending: Mutex::new(false), condvar: Condvar::new() }
    }

    fn notify(&self) {
        let mut pending = self.pending.lock().unwrap();
        *pending = true;
        self.condvar.notify_one();
    }

    fn wait(&self, timeout: Duration) {
        let pending = self.pending.lock().unwrap();
        let mut pending = if *pending {
            pending
        } else {
            self.condvar.wait_timeout(pending, timeout).unwrap().0
        };
        *pending = false;
    }
}

#[derive(Clone)]
struct FlushSummary {
    sstable_id: u64,
    file_name: String,
    entries: u64,
    file_size: u64,
    duration_ms: u128,
    completed_at_ms: u64,
}

struct WriterState {
    wal: Wal,
    current_seq_no: u64,
    flush_requested_count: u64,
    grace_used: bool,
    writes_blocked: bool,
    last_rotation: Option<Instant>,
    stall_started_at: Option<Instant>,
}

struct PendingDeletion {
    file_name: String,
    path: PathBuf,
    handle: Arc<TableHandle>,   // drži se namerno, da bi strong_count imao smisla u sweep-u
    deferred_at: Instant,
}
pub struct LsmEngine {
    config: RwLock<Config>,
    active: RwLock<Arc<Memtable>>,
    immutables: RwLock<VecDeque<Arc<Memtable>>>,
    writer: Mutex<WriterState>,
    catalog: Mutex<Manifest>,
    flush_lock: Mutex<()>,
    is_closed: AtomicBool,
    shutting_down: AtomicBool,
    sstables: RwLock<Vec<Arc<TableHandle>>>,
    block_cache: BlockCache,
    fd_cache: FileHandleCache,
    read_stats: ReadStats,
    current_version: RwLock<Arc<Version>>,
    next_epoch: AtomicU64,
    publish_lock: Mutex<()>,
    next_sst_id: AtomicU64,
    event_log: EventLog,
    compaction: CompactionManager,
    flush_signal: WorkerSignal,
    compaction_signal: WorkerSignal,
    flush_running: AtomicBool,
    compact_running: AtomicBool,
    versions_published_total: AtomicU64,
    write_stalls_total: AtomicU64,
    stall_time_ms_total: AtomicU64,
    last_flush: Mutex<Option<FlushSummary>>,
    pending_deletions: Mutex<Vec<PendingDeletion>>,
}
impl LsmEngine {
    pub fn new(config: Config) -> std::io::Result<Self> {
        let (wal, max_seq_no, recovered_records) = Wal::new(&config.data_dir, config.wal_segment_roll_bytes)?;
        let wal_records_count = recovered_records.len();
        let overhead = config.memtable_size_overhead_bytes_per_entry;
        let max_bytes = config.memtable_max_bytes;
        let max_immutables = config.max_immutable_tables as usize;
        let mut active_memtable = Memtable::new(overhead);
        let mut immutables = VecDeque::new();
        let mut grace_used = false;
        let mut writes_blocked = false;
        for record in recovered_records {
            if writes_blocked {
                break;
            }
            let WalRecord { record_type, seq_no, key, value } = record;
            match record_type {
                RecordType::Put => {
                    active_memtable.put(key, value.unwrap_or_default(), seq_no);
                }
                RecordType::Delete => {
                    active_memtable.delete(key, seq_no);
                }
            }
            let effective_max = if grace_used {
                (max_bytes as f64 * 1.25) as u64
            } else {
                max_bytes
            };
            if active_memtable.approx_size_bytes() >= effective_max {
                if immutables.len() >= max_immutables {
                    if !grace_used {
                        grace_used = true;
                    } else {
                        writes_blocked = true;
                    }
                    continue;
                }
                immutables.push_back(Arc::new(active_memtable));
                active_memtable = Memtable::new(overhead);
                grace_used = false;
            }
        }
        let flush_requested_count = immutables.len() as u64;
        fs::create_dir_all(&config.sst_dir)?;
        let event_log = EventLog::open(&config.event_log_path)?;
        if let Ok(dir_entries) = fs::read_dir(&config.sst_dir) {
            for entry in dir_entries.filter_map(Result::ok) {
                let path = entry.path();
                if path.extension().map_or(false, |ext| ext == "tmp") {
                    let _ = fs::remove_file(&path);
                }
            }
        }
        let manifest_path = config.manifest_path.clone();
        let _ = fs::remove_file(Manifest::tmp_path(Path::new(&manifest_path)));
        let manifest = Manifest::load(&manifest_path)?;
        if let Ok(dir_entries) = fs::read_dir(&config.sst_dir) {
            let known_files: HashSet<String> =
                manifest.tables.iter().map(|t| t.file_name.clone()).collect();
            let mut orphans_removed = Vec::new();
            for entry in dir_entries.filter_map(Result::ok) {
                let path = entry.path();
                if path.extension().map_or(false, |ext| ext == "sst") {
                    let file_name = match path.file_name() {
                        Some(n) => n.to_string_lossy().into_owned(),
                        None => continue,
                    };
                    if !known_files.contains(&file_name) {
                        match fs::remove_file(&path) {
                            Ok(_) => orphans_removed.push(file_name),
                            Err(e) => eprintln!(
                                "warning: failed to remove orphan SSTable '{}' \
                                (not referenced by manifest): {e}",
                                path.display()
                            ),
                        }
                    }
                }
            }
            if !orphans_removed.is_empty() {
                println!(
                    "startup: removed {} orphan SSTable(s) not referenced by manifest -> [{}]",
                    orphans_removed.len(),
                    orphans_removed.join(", ")
                );
            }
        }
        for table in &manifest.tables {
            let sst_path = Path::new(&config.sst_dir).join(&table.file_name);
            if let Err(e) = sstable::quick_validate(&sst_path) {
                let msg = format!(
                    "CorruptionDetected: SSTabela '{}' (id={}) iz manifesta nije prošla validaciju: {e}",
                    table.file_name, table.id
                );
                eprintln!("{msg}");
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, msg));
            }
        }
        match wal.delete_segments_below_watermark(manifest.flushed_seq_no_watermark) {
            Ok(removed) if !removed.is_empty() => {
                println!(
                    "wal cleanup: removed segments below watermark={} on startup -> [{}]",
                    manifest.flushed_seq_no_watermark,
                    removed.join(", ")
                );
            }
            Ok(_) => {}
            Err(e) => eprintln!("wal cleanup: failed to remove old segments on startup: {e}"),
        }
        let sst_dir_path = Path::new(&config.sst_dir);
        let mut opened_tables = Vec::with_capacity(manifest.tables.len());
        for table_meta in &manifest.tables {
            let handle = TableHandle::open(sst_dir_path, table_meta).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "failed to open SSTable '{}' (id={}) listed in manifest: {e}",
                        table_meta.file_name, table_meta.id
                    ),
                )
            })?;
            opened_tables.push(Arc::new(handle));
        }
        let block_cache = BlockCache::new(config.block_cache_mb);
        let fd_cache = FileHandleCache::new(config.max_open_files);
        let manifest_tables_count = manifest.tables.len();
        let loaded_epoch = manifest.epoch;
        let initial_next_sst_id = manifest.next_sst_id;
        let version_published_epoch = loaded_epoch + 1;
        let initial_active = Arc::new(active_memtable);
        let initial_version = Version::from_parts(
            version_published_epoch,
            Arc::clone(&initial_active),
            &immutables,
            &opened_tables,
        );
        println!(
            "startup: manifest_tables={} epoch={} wal_records={} version_published={}",
            manifest_tables_count, loaded_epoch, wal_records_count, version_published_epoch
        );
        println!(
            "startup: manifest_tables={} epoch={} wal_records={} version_published={}",
            manifest_tables_count, loaded_epoch, wal_records_count, version_published_epoch
        );
        event_log.record_with_block(
            "INIT",
            &[
                ("manifest_tables", manifest_tables_count.to_string()),
                ("manifest_epoch", loaded_epoch.to_string()),
                ("wal_records_replayed", wal_records_count.to_string()),
                ("version_published", version_published_epoch.to_string()),
            ],
            "config",
            &serde_json::to_string_pretty(&config)
                .unwrap_or_else(|_| "<failed to serialize config>".to_string()),
        );
        Ok(Self {
            config: RwLock::new(config),
            active: RwLock::new(initial_active),
            immutables: RwLock::new(immutables),
            writer: Mutex::new(WriterState {
                wal,
                current_seq_no: max_seq_no,
                flush_requested_count,
                grace_used,
                writes_blocked,
                last_rotation: None,
                stall_started_at: None,
            }),
            catalog: Mutex::new(manifest),
            flush_lock: Mutex::new(()),
            is_closed: AtomicBool::new(false),
            shutting_down: AtomicBool::new(false),
            sstables: RwLock::new(opened_tables),
            block_cache,
            fd_cache,
            read_stats: ReadStats::new(),
            current_version: RwLock::new(Arc::new(initial_version)),
            next_epoch: AtomicU64::new(version_published_epoch + 1),
            publish_lock: Mutex::new(()),
            next_sst_id: AtomicU64::new(initial_next_sst_id),
            event_log,
            compaction: CompactionManager::new(),
            flush_signal: WorkerSignal::new(),
            compaction_signal: WorkerSignal::new(),
            flush_running: AtomicBool::new(false),
            compact_running: AtomicBool::new(false),
            versions_published_total: AtomicU64::new(0),
            write_stalls_total: AtomicU64::new(0),
            stall_time_ms_total: AtomicU64::new(0),
            last_flush: Mutex::new(None),
            pending_deletions: Mutex::new(Vec::new()),
        })
    }
    fn lookup_key(&self, key: &str) -> Lookup {
        let (result, _trace) = self.lookup_key_traced(key);
        result
    }
    fn lookup_key_traced(&self, key: &str) -> (Lookup, LookupTrace) {
        let mut trace = LookupTrace {
            checked_memtable: true,
            ..Default::default()
        };
        let active_snapshot = self.active.read().unwrap().clone();
        match active_snapshot.get(key) {
            Lookup::Found(val) => return (Lookup::Found(val), trace),
            Lookup::Tombstone => return (Lookup::Tombstone, trace),
            Lookup::NotFound => {}
        }
        let version = self.current_version.read().unwrap().clone();
        for memtable in version.immutables.iter() {
            trace.immutables_consulted += 1;
            match memtable.get(key) {
                Lookup::Found(val) => return (Lookup::Found(val), trace),
                Lookup::Tombstone => return (Lookup::Tombstone, trace),
                Lookup::NotFound => continue,
            }
        }
        let cache_index_blocks = self.config.read().unwrap().cache_index_blocks;
        match search_sstables(
            &version.sstables,
            key,
            &self.fd_cache,
            &self.block_cache,
            cache_index_blocks,
            &self.read_stats,
            &mut trace,
        ) {
            Ok(result) => (result, trace),
            Err(e) => {
                eprintln!("get: {e}");
                (Lookup::NotFound, trace)
            }
        }
    }
    pub fn put(&self, key: String, value: String) -> Result<String, String> {
        if key.is_empty() { return Err("InvalidArgument: Key cannot be empty".to_string()); }
        self.write_op(RecordType::Put, key, Some(value))
    }
    pub fn delete(&self, key: String) -> Result<String, String> {
        if key.is_empty() { return Err("InvalidArgument: Key cannot be empty".to_string()); }
        match self.lookup_key(&key) {
            Lookup::Found(_) => self.write_op(RecordType::Delete, key, None),
            Lookup::Tombstone | Lookup::NotFound => Ok("Key not found".to_string()),
        }
    }
    pub fn get(&self, key: &str) -> Result<String, String> {
        let debug_trace_enabled = self.config.read().unwrap().log_level.eq_ignore_ascii_case("debug");
        let (result, trace) = self.lookup_key_traced(key);
        if debug_trace_enabled {
            eprintln!(
                "debug: get key='{}' memtable_checked={} immutables_consulted={} sstables_consulted={} blooms_skipped={} block_reads={} cache_hits={}",
                key,
                trace.checked_memtable,
                trace.immutables_consulted,
                trace.sstables_consulted,
                trace.blooms_skipped,
                trace.block_reads,
                trace.cache_hits,
            );
        }
        match result {
            Lookup::Found(value) => Ok(value),
            Lookup::Tombstone | Lookup::NotFound => Ok("Key not found".to_string()),
        }
    }
    pub fn dump_memtables(&self) -> String {
        let mut out = String::from("=== MEMTABLE DUMP ===\n");
        let active = self.active.read().unwrap();
        out.push_str(&active.dump_contents("ACTIVE MEMTABLE"));
        let immutables = self.immutables.read().unwrap();
        if immutables.is_empty() {
            out.push_str("\n--- IMMUTABLE MEMTABLES (0) ---\n  (none)\n");
        } else {
            out.push_str(&format!("\n--- IMMUTABLE MEMTABLES ({}) ---\n", immutables.len()));
            for (i, imm) in immutables.iter().enumerate() {
                out.push_str(&imm.dump_contents(&format!("Immutable #{}", i + 1)));
            }
        }
        out
    }
    fn write_op(&self, record_type: RecordType, key: String, value: Option<String>) -> Result<String, String> {
        if self.is_closed.load(Ordering::SeqCst) || self.shutting_down.load(Ordering::SeqCst) {
            return Err("StoreClosed: Engine is shut down".to_string());
        }
        let l0_stop_writes = self.config.read().unwrap().l0_stop_writes as usize;
        if l0_stop_writes > 0 {
            let sst_count_now = self.sstables.read().unwrap().len();
            if sst_count_now >= l0_stop_writes {
                self.write_stalls_total.fetch_add(1, Ordering::SeqCst);
                eprintln!(
                    "WARNING: write stall! sstable_count={sst_count_now} >= l0_stop_writes={l0_stop_writes}"
                );
                return Err(format!(
                    "Backpressure: too many SSTables on disk (sst_count={sst_count_now}, l0_stop_writes={l0_stop_writes}); \
                    writes blocked until compaction catches up"
                ));
            }
        }
        let mut writer = self.writer.lock().unwrap();
        if writer.writes_blocked {
            self.write_stalls_total.fetch_add(1, Ordering::SeqCst);
            let immutable_count = self.immutables.read().unwrap().len();
            let max_immutables = self.config.read().unwrap().max_immutable_tables;
            return Err(format!(
                "Backpressure: too many immutable memtables pending flush (immutables={}, max={}); writes blocked",
                immutable_count, max_immutables
            ));
        }
        writer.current_seq_no += 1;
        let seq_no = writer.current_seq_no;
        let record = WalRecord::new(record_type.clone(), seq_no, key.clone(), value.clone());
        let fsync_every_n = self.config.read().unwrap().wal_fsync_every_n;
        writer.wal.append(&record, fsync_every_n)
            .map_err(|e| format!("IOFailure: {:?}", e))?;
        let op_result: MemtableOpResult = {
            let mut active_guard = self.active.write().unwrap();
            let table = Arc::make_mut(&mut active_guard);
            match record_type {
                RecordType::Put => table.put(key.clone(), value.unwrap_or_default(), seq_no),
                RecordType::Delete => table.delete(key.clone(), seq_no),
            }
        };
        self.check_rotation(&mut writer);
        let max_bytes = self.config.read().unwrap().memtable_max_bytes;
        let op_type = if record_type == RecordType::Put { "PUT" } else { "DEL" };
        let response = format!(
            "Success: {} key='{}' (seqNo: {})\n\
            • Memtable Delta: +{} B / -{} B\n\
            • Memtable Memory: {} / {} B ({:.2}%)\n\
            • Memtable Contents:\n{}",
            op_type, key, seq_no,
            op_result.bytes_added, op_result.bytes_removed,
            op_result.total_bytes, max_bytes,
            (op_result.total_bytes as f64 / max_bytes as f64) * 100.0,
            op_result.contents
        );
        Ok(response)
    }
    fn check_rotation(&self, writer: &mut WriterState) {
        let (memtable_max_bytes, max_immutables, cooldown_ms, overhead) = {
            let config = self.config.read().unwrap();
            (
                config.memtable_max_bytes,
                config.max_immutable_tables as usize,
                config.rotation_cooldown_ms,
                config.memtable_size_overhead_bytes_per_entry,
            )
        };
        if cooldown_ms > 0 {
            if let Some(last) = writer.last_rotation {
                if last.elapsed().as_millis() < cooldown_ms as u128 {
                    return;
                }
            }
        }
        let effective_max = if writer.grace_used {
            (memtable_max_bytes as f64 * 1.25) as u64
        } else {
            memtable_max_bytes
        };
        let active_size = self.active.read().unwrap().approx_size_bytes();
        if active_size < effective_max {
            return;
        }
        let immutable_count_before = self.immutables.read().unwrap().len();
        if immutable_count_before >= max_immutables {
            if !writer.grace_used {
                writer.grace_used = true;
            } else {
                if !writer.writes_blocked {
                    writer.stall_started_at = Some(Instant::now());
                    println!(
                        "stall: immutables={} (max={}) blocking writes",
                        immutable_count_before, max_immutables
                    );
                    self.event_log.record(
                        "WRITE_STALL_BEGIN",
                        &[
                            ("immutables_count", immutable_count_before.to_string()),
                            ("max_immutable_tables", max_immutables.to_string()),
                        ],
                    );
                }
                writer.writes_blocked = true;
            }
            return;
        }
        let frozen = self.active.read().unwrap().clone();
        {
            let mut immutables_guard = self.immutables.write().unwrap();
            immutables_guard.push_back(frozen);
        }
        writer.flush_requested_count += 1;
        writer.grace_used = false;
        writer.last_rotation = Some(Instant::now());
        let published = self.publish_version();
        self.event_log.record(
            "ROTATE",
            &[
                ("trigger", "memtable_max_bytes_reached".to_string()),
                ("active_size_bytes", active_size.to_string()),
                ("memtable_max_bytes", memtable_max_bytes.to_string()),
                ("immutables_pending", (immutable_count_before + 1).to_string()),
                ("flush_requested_count", writer.flush_requested_count.to_string()),
                ("published_epoch", published.epoch.to_string()),
            ],
        );
        {
            let mut active_guard = self.active.write().unwrap();
            *active_guard = Arc::new(Memtable::new(overhead));
        }
        // Probudi FlushWorker odmah — ne čekaj pun bg_tick_ms.
        self.flush_signal.notify();
    }
    fn build_and_swap_version(&self, epoch: u64) -> Arc<Version> {
        let active_snapshot = self.active.read().unwrap().clone();
        let immutables_snapshot: VecDeque<Arc<Memtable>> = self.immutables.read().unwrap().clone();
        let sstables_snapshot: Vec<Arc<TableHandle>> = self.sstables.read().unwrap().clone();
        let new_version = Arc::new(Version::from_parts(
            epoch,
            active_snapshot,
            &immutables_snapshot,
            &sstables_snapshot,
        ));
        {
            let mut current = self.current_version.write().unwrap();
            *current = Arc::clone(&new_version);
        }
        self.versions_published_total.fetch_add(1, Ordering::SeqCst);
        if self.config.read().unwrap().publish_log_level.eq_ignore_ascii_case("debug") {
            eprintln!(
                "debug: publish epoch={} active_entries={} immutables={} sstables={}",
                new_version.epoch,
                new_version.active.len(),
                new_version.immutables.len(),
                new_version.sstables.len(),
            );
        }
        new_version
    }
    fn publish_version(&self) -> Arc<Version> {
        let _publish_guard = self.publish_lock.lock().unwrap();
        let epoch = self.next_epoch.fetch_add(1, Ordering::SeqCst);
        self.build_and_swap_version(epoch)
    }
    fn immutables_is_empty(&self) -> bool {
        self.immutables.read().unwrap().is_empty()
    }
    /// Ako je immutable queue ispod max-a i pisac je bio blokiran zbog
    /// backpressure-a, oslobodi ga i saberi koliko je stall trajao.
    fn clear_stall_if_room(&self) {
        let max_immutables = self.config.read().unwrap().max_immutable_tables as usize;
        let immutable_count = self.immutables.read().unwrap().len();
        if immutable_count < max_immutables {
            let mut writer = self.writer.lock().unwrap();
            if writer.writes_blocked {
                writer.writes_blocked = false;
                if let Some(start) = writer.stall_started_at.take() {
                    let elapsed_ms = start.elapsed().as_millis() as u64;
                    self.stall_time_ms_total.fetch_add(elapsed_ms, Ordering::SeqCst);
                    println!(
                        "stall: cleared after {}ms (immutables={} < max={})",
                        elapsed_ms, immutable_count, max_immutables
                    );
                    self.event_log.record(
                        "WRITE_STALL_CLEARED",
                        &[
                            ("stall_duration_ms", elapsed_ms.to_string()),
                            ("immutables_count", immutable_count.to_string()),
                            ("max_immutable_tables", max_immutables.to_string()),
                        ],
                    );
                }
            }
        }
    }
    pub fn flush_immutable(&self) -> Result<String, String> {
        if self.is_closed.load(Ordering::SeqCst) {
            return Err("StoreClosed: Engine is shut down".to_string());
        }
        let _flush_guard = self.flush_lock.lock().unwrap();
        let memtable = {
            let immutables = self.immutables.read().unwrap();
            immutables.front().cloned()
        };
        let memtable = match memtable {
            Some(m) => m,
            None => return Ok("info: no immutable memtable is waiting to be flushed".to_string()),
        };
        if memtable.is_empty() {
            self.immutables.write().unwrap().pop_front();
            self.publish_version();
            self.event_log.record(
                "FLUSH_SKIPPED_EMPTY",
                &[("reason", "immutable memtable was empty, nothing persisted".to_string())],
            );
            self.clear_stall_if_room();
            return Ok("info: dropped an empty immutable memtable (nothing to flush)".to_string());
        }
        let flush_start = Instant::now();
        let (sst_dir, manifest_path, block_size, restart_interval, bloom_fpr, build_buffer_bytes) = {
            let config = self.config.read().unwrap();
            (
                config.sst_dir.clone(),
                config.manifest_path.clone(),
                config.block_size,
                config.restart_interval,
                config.bloom_false_positive,
                config.max_build_buffer_mb.saturating_mul(1024 * 1024),
            )
        };
        let sst_path = Path::new(&sst_dir);
        let id = self.next_sst_id.fetch_add(1, Ordering::SeqCst);
        let meta = sstable::flush_memtable_to_sstable(
            sst_path,
            id,
            &memtable,
            block_size,
            restart_interval,
            bloom_fpr,
            build_buffer_bytes,
        )
        .map_err(|e| format!("IOFailure: failed to flush SSTable: {e}"))?;
        let _publish_guard = self.publish_lock.lock().unwrap();
        let epoch_for_this_flush = self.next_epoch.fetch_add(1, Ordering::SeqCst);
        let mut candidate = {
            let catalog = self.catalog.lock().unwrap();
            catalog.clone()
        };
        candidate.next_sst_id = candidate.next_sst_id.max(id + 1);
        candidate.flushed_seq_no_watermark = candidate.flushed_seq_no_watermark.max(meta.max_seq_no);
        candidate.epoch = epoch_for_this_flush;
        candidate.tables.insert(0, meta.clone());
        candidate.save(&manifest_path).map_err(|e| {
            format!(
                "IOFailure: SSTable '{}' was written and fsynced, but updating the manifest failed: {e}. \
                Engine state left unchanged; a future flush will retry.",
                meta.file_name
            )
        })?;
        {
            let mut catalog = self.catalog.lock().unwrap();
            *catalog = candidate;
        }
        let opened_new_table = match TableHandle::open(sst_path, &meta) {
            Ok(handle) => {
                self.sstables.write().unwrap().insert(0, Arc::new(handle));
                true
            }
            Err(e) => {
                eprintln!(
                    "warning: flushed SSTable '{}' but failed to open it for reads immediately: {e}. \
                    Immutable memtable je namerno ostavljena u memoriji da čitanja ostanu ispravna; \
                    sledeći 'flush-now' pokušaj će ponovo pokušati da je persistuje pod novim id-jem.",
                    meta.file_name
                );
                false
            }
        };
        if opened_new_table {
            let mut immutables = self.immutables.write().unwrap();
            if let Some(front) = immutables.front() {
                if Arc::ptr_eq(front, &memtable) {
                    immutables.pop_front();
                }
            }
        }
        self.build_and_swap_version(epoch_for_this_flush);
        drop(_publish_guard);
        let watermark = {
            let catalog = self.catalog.lock().unwrap();
            catalog.flushed_seq_no_watermark
        };
        self.cleanup_wal_segments(watermark);
        self.event_log.record(
            "FLUSH",
            &[
                ("sstable_id", meta.id.to_string()),
                ("file_name", meta.file_name.clone()),
                ("entries", meta.num_entries.to_string()),
                ("min_key", meta.min_key.clone()),
                ("max_key", meta.max_key.clone()),
                ("min_seq_no", meta.min_seq_no.to_string()),
                ("max_seq_no", meta.max_seq_no.to_string()),
                ("file_size_bytes", meta.file_size.to_string()),
                ("manifest_epoch", epoch_for_this_flush.to_string()),
                ("wal_watermark", watermark.to_string()),
                ("opened_immediately", opened_new_table.to_string()),
            ],
        );
        let duration_ms = flush_start.elapsed().as_millis();
        let completed_at_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        *self.last_flush.lock().unwrap() = Some(FlushSummary {
            sstable_id: meta.id,
            file_name: meta.file_name.clone(),
            entries: meta.num_entries,
            file_size: meta.file_size,
            duration_ms,
            completed_at_ms,
        });
        self.clear_stall_if_room();
        // Skup SSTabela se promenio — probudi CompactionWorker da proveri picker-a.
        self.compaction_signal.notify();
        Ok(format!(
            "success: flushed immutable memtable to '{}'\n\
            • Entries: {}\n\
            • Key range: '{}' .. '{}'\n\
            • SeqNo range: {} .. {}\n\
            • File size: {} B\n\
            • Bloom filter: {} bits, {} hash functions (target FPR {:.4})\n\
            • WAL watermark advanced to: {}\n\
            • Manifest epoch / published version: {}",
            meta.file_name,
            meta.num_entries,
            meta.min_key,
            meta.max_key,
            meta.min_seq_no,
            meta.max_seq_no,
            meta.file_size,
            meta.bloom_params.num_bits,
            meta.bloom_params.num_hashes,
            meta.bloom_params.false_positive_rate,
            watermark,
            epoch_for_this_flush,
        ))
    }
    fn cleanup_wal_segments(&self, watermark: u64) {
        let writer = self.writer.lock().unwrap();
        match writer.wal.delete_segments_below_watermark(watermark) {
            Ok(removed) if !removed.is_empty() => {
                println!(
                    "wal cleanup: removed segments below watermark={} -> [{}]",
                    watermark,
                    removed.join(", ")
                );
            }
            Ok(_) => {}
            Err(e) => eprintln!("wal cleanup: failed to remove old segments: {e}"),
        }
    }
    pub fn get_stats_string(&self) -> String {
        let writer = self.writer.lock().unwrap();
        let (active_id, bytes, total_segs) = writer.wal.get_stats();
        let active_snapshot = self.active.read().unwrap().clone();
        let active_len = active_snapshot.len();
        let active_bytes = active_snapshot.approx_size_bytes();
        let (immutable_count, immutable_entries, immutable_bytes) = {
            let immutables_guard = self.immutables.read().unwrap();
            let count = immutables_guard.len();
            let entries: usize = immutables_guard.iter().map(|t| t.len()).sum();
            let bytes: u64 = immutables_guard.iter().map(|t| t.approx_size_bytes()).sum();
            (count, entries, bytes)
        };
        let (sst_count, sst_total_bytes, sst_total_entries, next_sst_id, watermark, manifest_version, manifest_epoch) = {
            let catalog = self.catalog.lock().unwrap();
            let total_bytes: u64 = catalog.tables.iter().map(|t| t.file_size).sum();
            let total_entries: u64 = catalog.tables.iter().map(|t| t.num_entries).sum();
            (
                catalog.tables.len(),
                total_bytes,
                total_entries,
                catalog.next_sst_id,
                catalog.flushed_seq_no_watermark,
                catalog.manifest_version,
                catalog.epoch,
            )
        };
        let version = self.current_version.read().unwrap().clone();
        let sstables_open = self.sstables.read().unwrap().len();
        let rs = self.read_stats.snapshot();
        let cache_total = rs.block_cache_hits + rs.block_cache_misses;
        let hit_rate = if cache_total > 0 {
            (rs.block_cache_hits as f64 / cache_total as f64) * 100.0
        } else {
            0.0
        };
        let config = self.config.read().unwrap();
        format!(
            "--- LSM Engine Stats ---\n\
            Active Segment ID: {:06}.wal\n\
            Bytes written in segment: {}\n\
            Total WAL segments: {}\n\
            Last SeqNo: {}\n\
            Fsync Policy: Every {} writes\n\
            --- Memtable ---\n\
            Overhead per entry: {} bytes\n\
            Rotation Cooldown: {} ms\n\
            Active memtable: {} entries, {} bytes (max: {} bytes)\n\
            Immutable memtables waiting: {} / {} max, {} entries, {} bytes total\n\
            Flush requests queued: {}\n\
            Backpressure grace in effect: {}\n\
            Writes blocked: {}\n\
            --- SSTables ---\n\
            SSTable count: {}\n\
            SSTable total size: {} bytes\n\
            SSTable total entries: {}\n\
            Next SSTable id: {:06}.sst\n\
            Flushed seqNo watermark: {}\n\
            --- Read path (Section 4) ---\n\
            SSTables open: {}\n\
            Block cache: {} MB budget\n\
            Cache index blocks: {}\n\
            Max open files: {}\n\
            Blooms checked: {}\n\
            Blooms negative (skipped table): {}\n\
            Block cache hits/misses: {} / {} (hit rate {:.2}%)\n\
            Disk block reads: {}\n\
            --- Section 5: Manifest & Version ---\n\
            Manifest path: {}\n\
            Manifest format version: {}\n\
            Manifest epoch (on disk): {}\n\
            Current version epoch/id: {}\n\
            active_memtable_bytes: {}\n\
            immutables_count: {}\n\
            sst_count: {}\n\
            Publish log level: {}\n\
            --- Section 7: Concurrency & Scheduling ---\n\
            flush_running: {}\n\
            compact_running: {}\n\
            compaction_paused: {}\n\
            bg_tick_ms: {}\n\
            shutdown_timeout_ms: {}\n\
            versions_published_total: {}\n\
            write_stalls_total: {}\n\
            stall_time_ms_total: {}\n\
            ------------------------",
            active_id, bytes, total_segs, writer.current_seq_no, config.wal_fsync_every_n,
            config.memtable_size_overhead_bytes_per_entry, config.rotation_cooldown_ms,
            active_len, active_bytes, config.memtable_max_bytes,
            immutable_count, config.max_immutable_tables, immutable_entries, immutable_bytes,
            writer.flush_requested_count,
            writer.grace_used,
            writer.writes_blocked,
            sst_count, sst_total_bytes, sst_total_entries, next_sst_id, watermark,
            sstables_open,
            config.block_cache_mb,
            config.cache_index_blocks,
            config.max_open_files,
            rs.blooms_checked,
            rs.blooms_negative,
            rs.block_cache_hits, rs.block_cache_misses, hit_rate,
            rs.disk_block_reads,
            config.manifest_path,
            manifest_version,
            manifest_epoch,
            version.epoch,
            active_bytes,
            version.immutable_count(),
            version.sstable_count(),
            config.publish_log_level,
            self.flush_running.load(Ordering::SeqCst),
            self.compact_running.load(Ordering::SeqCst),
            self.compaction.is_paused(),
            config.bg_tick_ms,
            config.shutdown_timeout_ms,
            self.versions_published_total.load(Ordering::SeqCst),
            self.write_stalls_total.load(Ordering::SeqCst),
            self.stall_time_ms_total.load(Ordering::SeqCst),
        )
    }
    pub fn bg_status_string(&self) -> String {
        let flush_running = self.flush_running.load(Ordering::SeqCst);
        let compact_running = self.compact_running.load(Ordering::SeqCst);
        let compaction_paused = self.compaction.is_paused();
        let shutting_down = self.shutting_down.load(Ordering::SeqCst);
        let is_closed = self.is_closed.load(Ordering::SeqCst);
        let (immutables_count, max_immutables, bg_tick_ms, shutdown_timeout_ms, l0_trigger) = {
            let cfg = self.config.read().unwrap();
            (
                self.immutables.read().unwrap().len(),
                cfg.max_immutable_tables,
                cfg.bg_tick_ms,
                cfg.shutdown_timeout_ms,
                cfg.l0_compaction_trigger,
            )
        };
        let sst_count = self.sstable_count();
        let writes_blocked = self.writer.lock().unwrap().writes_blocked;
        let in_progress = self.compaction.in_progress_snapshot();
        let running_jobs = self.compaction.running_jobs();
        let total_jobs = self.compaction.total_jobs_run();
        let versions_published = self.versions_published_total.load(Ordering::SeqCst);
        let write_stalls = self.write_stalls_total.load(Ordering::SeqCst);
        let stall_time_ms = self.stall_time_ms_total.load(Ordering::SeqCst);
        let last_flush_display = match self.last_flush.lock().unwrap().clone() {
            Some(f) => format!(
                "sstable_id={} file={} entries={} bytes={} dur_ms={} completed_at_ms={}",
                f.sstable_id, f.file_name, f.entries, f.file_size, f.duration_ms, f.completed_at_ms
            ),
            None => "(none yet)".to_string(),
        };
        let last_compaction_display = match self.compaction.last_job_snapshot() {
            Some(j) => format!(
                "job_id={} inputs={:?} output={:?} bytes_in={} bytes_out={} dur_ms={}",
                j.job_id, j.inputs, j.output, j.bytes_in, j.bytes_out, j.duration_ms
            ),
            None => "(none yet)".to_string(),
        };
        let (pending_deletions_count, oldest_pending_secs) = {
        let pending = self.pending_deletions.lock().unwrap();
        let count = pending.len();
        let oldest = pending.iter().map(|p| p.deferred_at.elapsed().as_secs()).max();
        (count, oldest)
        };
        format!(
            "--- Background Worker Status ---\n\
            shutting_down: {}\n\
            is_closed: {}\n\
            bg_tick_ms: {} | shutdown_timeout_ms: {}\n\
            --- FlushWorker ---\n\
            flush_running: {}\n\
            immutables queued: {} / {} max\n\
            writes_blocked (backpressure): {}\n\
            last_flush: {}\n\
            --- CompactionWorker ---\n\
            compact_running: {} (paused: {})\n\
            running_jobs: {} | files_in_progress: {:?}\n\
            sstable_count: {} (l0_compaction_trigger: {})\n\
            total_jobs_run (since start): {}\n\
            last_compaction: {}\n\
            pending_deletions: {} (oldest waiting {}s)\n\
            --- Publish / Backpressure Metrics ---\n\
            versions_published_total: {}\n\
            write_stalls_total: {}\n\
            stall_time_ms_total: {}\n\
            --------------------------------",
            shutting_down, is_closed, bg_tick_ms, shutdown_timeout_ms,
            flush_running, immutables_count, max_immutables, writes_blocked, last_flush_display,
            compact_running, compaction_paused, running_jobs, in_progress,
            sst_count, l0_trigger, total_jobs, last_compaction_display,
            pending_deletions_count, oldest_pending_secs.unwrap_or(0),
            versions_published, write_stalls, stall_time_ms,
        )
    }
    #[allow(clippy::too_many_arguments)]
    pub fn configure(
        &self,
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
    ) -> Result<String, String> {
        let mut new_config = self.config.read().unwrap().clone();
        if let Some(v) = data_dir { new_config.data_dir = v; }
        if let Some(v) = memtable_max_bytes { new_config.memtable_max_bytes = v; }
        if let Some(v) = max_immutable_tables { new_config.max_immutable_tables = v; }
        if let Some(v) = memtable_size_overhead_bytes_per_entry { new_config.memtable_size_overhead_bytes_per_entry = v; }
        if let Some(v) = rotation_cooldown_ms { new_config.rotation_cooldown_ms = v; }
        if let Some(v) = block_size { new_config.block_size = v; }
        if let Some(v) = bloom_false_positive { new_config.bloom_false_positive = v; }
        if let Some(v) = wal_fsync_every_n { new_config.wal_fsync_every_n = v; }
        if let Some(v) = wal_segment_roll_bytes { new_config.wal_segment_roll_bytes = v; }
        if let Some(v) = compression { new_config.compression = v; }
        if let Some(v) = log_level { new_config.log_level = v; }
        if let Some(v) = sst_dir { new_config.sst_dir = v; }
        if let Some(v) = restart_interval { new_config.restart_interval = v; }
        if let Some(v) = max_build_buffer_mb { new_config.max_build_buffer_mb = v; }
        if let Some(v) = block_cache_mb { new_config.block_cache_mb = v; }
        if let Some(v) = cache_index_blocks { new_config.cache_index_blocks = v; }
        if let Some(v) = max_open_files { new_config.max_open_files = v; }
        if let Some(v) = manifest_path { new_config.manifest_path = v; }
        if let Some(v) = publish_log_level { new_config.publish_log_level = v; }
        if let Some(v) = event_log_path { new_config.event_log_path = v; }
        if let Some(v) = size_tiered_fan_in { new_config.size_tiered_fan_in = v; }
        if let Some(v) = size_tiered_size_ratio { new_config.size_tiered_size_ratio = v; }
        if let Some(v) = tombstone_grace_seconds { new_config.tombstone_grace_seconds = v; }
        if let Some(v) = compaction_max_concurrent { new_config.compaction_max_concurrent = v; }
        if let Some(v) = compaction_io_mb_per_s { new_config.compaction_io_mb_per_s = v; }
        if let Some(v) = l0_compaction_trigger { new_config.l0_compaction_trigger = v; }
        if let Some(v) = l0_stop_writes { new_config.l0_stop_writes = v; }
        if let Some(v) = bg_tick_ms { new_config.bg_tick_ms = v; }
        if let Some(v) = shutdown_timeout_ms { new_config.shutdown_timeout_ms = v; }
        new_config.save_config(&name)
            .map(|_| format!(
                "success: config written to config/{name}.json \
                restart with `lsmkv init --config {name}` to activate it"
            ))
            .map_err(|e| format!("failed to write config: {e}"))
    }
    pub fn close(&self) -> Result<(), String> {
        if self.is_closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let mut writer = self.writer.lock().unwrap();
        writer.wal.close().map_err(|e| format!("IOFailure: {:?}", e))
    }
    /// Section 7.7 — shutdown u dva režima.
    ///
    /// Fast: pauzira compaction odmah (nema novih job-ova; ako je jedan već u
    /// toku, ne čekamo ga — proces se gasi i taj posao ostaje незавршен, a
    /// njegovi ulazi ostaju Live u manifestu pa će biti ponovo pokupljeni na
    /// sledećem restartu). Flush dobija best-effort grace period od
    /// shutdown_timeout_ms; ako ne stigne, immutable ostaje u RAM-u i biće
    /// obnovljen replay-om WAL-a na sledećem startu.
    ///
    /// Graceful: pušta writer da se ispразни, čeka da SVI flush-evi završe,
    /// pa dozvoljava da se trenutni compaction job završi (ali ne pušta nove),
    /// pa publikuje finalnu verziju i tek onda zatvara WAL.
    pub fn shutdown(&self, graceful: bool) -> Result<String, String> {
        if self.shutting_down.swap(true, Ordering::SeqCst) {
            return Ok("info: shutdown already in progress or completed".to_string());
        }
        let mode = if graceful { "graceful" } else { "fast" };
        println!("shutdown: mode={mode} starting");
        self.event_log.record("SHUTDOWN_BEGIN", &[("mode", mode.to_string())]);
        if graceful {
            // 1) drain writer: kratko uzmi writer lock da bilo koji upis koji
            // je već "u letu" stigne da završi; novi upisi se odbijaju preko
            // shutting_down flaga u write_op.
            { let _writer = self.writer.lock().unwrap(); }
            // 2) završi sve flush-eve u redu
            self.flush_signal.notify();
            while !self.immutables_is_empty() || self.flush_running.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_millis(20));
                self.flush_signal.notify();
            }
            // 3) pusti trenutni compaction job da se završi, ali bez novih
            self.compaction.pause();
            self.compaction_signal.notify();
            while self.compact_running.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_millis(20));
            }
            // 4) finalna verzija — manifest je već transakciono persistovan
            // uz svaki flush/compaction, tako da nema ničeg dodatnog za upis.
            self.publish_version();
        } else {
            // Fast: odmah zabrani nove compaction job-ove; postojeći (ako
            // ima) se ne čeka — proces gasimo, njegovi ulazi ostaju Live.
            self.compaction.pause();
            let timeout_ms = self.config.read().unwrap().shutdown_timeout_ms;
            let deadline = Instant::now() + Duration::from_millis(timeout_ms);
            self.flush_signal.notify();
            while !self.immutables_is_empty() || self.flush_running.load(Ordering::SeqCst) {
                if Instant::now() >= deadline {
                    let remaining = self.immutables.read().unwrap().len();
                    println!(
                        "shutdown: fast timeout reached, {remaining} immutable(s) left in RAM \
                        (will replay from WAL on next start)"
                    );
                    self.event_log.record(
                        "SHUTDOWN_FAST_FLUSH_TIMEOUT",
                        &[("immutables_remaining", remaining.to_string())],
                    );
                    break;
                }
                thread::sleep(Duration::from_millis(20));
            }
        }
        let close_result = self.close();
        self.event_log.record("SHUTDOWN_COMPLETE", &[("mode", mode.to_string())]);
        match close_result {
            Ok(_) => {
                println!("shutdown: mode={mode} complete");
                Ok(format!("success: {mode} shutdown complete"))
            }
            Err(e) => Err(e),
        }
    }
    pub fn sstable_count(&self) -> usize {
        self.sstables.read().unwrap().len()
    }
    pub fn should_auto_compact(&self) -> bool {
        if self.compaction.is_paused() {
            return false;
        }
        let (l0_trigger, max_concurrent) = {
            let config = self.config.read().unwrap();
            (config.l0_compaction_trigger as usize, config.compaction_max_concurrent)
        };
        self.sstable_count() >= l0_trigger && self.compaction.running_jobs() < max_concurrent
    }
    pub fn list_ssts_string(&self) -> String {
        let catalog = self.catalog.lock().unwrap();
        if catalog.tables.is_empty() {
            return "--- SSTables (0) ---\nNema aktivnih SSTabela.".to_string();
        }
        let mut out = format!("--- Aktivne SSTabele ({}) ---\n", catalog.tables.len());
        out.push_str(&format!(
            "{:<15} | {:<10} | {:<10} | {:<20} | {:<20} | {:<15}\n",
            "File Name", "Size (B)", "Entries", "Min Key", "Max Key", "SeqNo Range"
        ));
        out.push_str(&"-".repeat(100));
        out.push('\n');
        for table in &catalog.tables {
            let seq_range = format!("{}..{}", table.min_seq_no, table.max_seq_no);
            out.push_str(&format!(
                "{:<15} | {:<10} | {:<10} | {:<20} | {:<20} | {:<15}\n",
                table.file_name,
                table.file_size,
                table.num_entries,
                table.min_key,
                table.max_key,
                seq_range
            ));
        }
        let total_size: u64 = catalog.tables.iter().map(|t| t.file_size).sum();
        out.push_str(&"-".repeat(100));
        out.push('\n');
        out.push_str(&format!("Total tables size: {} bytes\n", total_size));
        out
    }
    pub fn manifest_info_string(&self) -> String {
        let catalog = self.catalog.lock().unwrap();
        let manifest_path = self.config.read().unwrap().manifest_path.clone();
        let mut out = format!(
            "--- Manifest Info ---\n\
            Path: {}\n\
            manifest_version: {}\n\
            epoch: {}\n\
            next_sst_id: {:06}.sst\n\
            flushed_seq_no_watermark: {}\n\
            tables: {} (newest first)\n",
            manifest_path,
            catalog.manifest_version,
            catalog.epoch,
            catalog.next_sst_id,
            catalog.flushed_seq_no_watermark,
            catalog.tables.len(),
        );
        if catalog.tables.is_empty() {
            out.push_str("  (no tables)\n");
            return out;
        }
        out.push_str(&format!(
            "{:<10} | {:<10} | {:<20} | {:<20} | {:<15}\n",
            "ID", "Size (B)", "Min Key", "Max Key", "SeqNo Range"
        ));
        out.push_str(&"-".repeat(85));
        out.push('\n');
        for table in &catalog.tables {
            out.push_str(&format!(
                "{:<10} | {:<10} | {:<20} | {:<20} | {:<15}\n",
                format!("{:06}", table.id),
                table.file_size,
                table.min_key,
                table.max_key,
                format!("{}..{}", table.min_seq_no, table.max_seq_no),
            ));
        }
        out
    }
    pub fn version_info_string(&self) -> String {
        let version = self.current_version.read().unwrap().clone();
        let active_now = self.active.read().unwrap().clone();
        let mut out = format!(
            "--- Version Info ---\n\
            epoch / version_id: {}\n\
            active memtable: {} entries, {} bytes\n\
            immutables (newest -> oldest): {}\n",
            version.epoch,
            active_now.len(),
            active_now.approx_size_bytes(),
            version.immutables.len(),
        );
        if version.immutables.is_empty() {
            out.push_str("  (none)\n");
        } else {
            for (i, imm) in version.immutables.iter().enumerate() {
                out.push_str(&format!(
                    "  #{:<3} entries={} bytes={}\n",
                    i + 1,
                    imm.len(),
                    imm.approx_size_bytes(),
                ));
            }
        }
        out.push_str(&format!("sstables (newest -> oldest): {}\n", version.sstables.len()));
        if version.sstables.is_empty() {
            out.push_str("  (none)\n");
        } else {
            for (i, table) in version.sstables.iter().enumerate() {
                out.push_str(&format!(
                    "  #{:<3} id={:06} file={} min='{}' max='{}' entries={} size={} B\n",
                    i + 1,
                    table.id(),
                    table.file_name(),
                    table.min_key(),
                    table.max_key(),
                    table.num_entries(),
                    table.file_size(),
                ));
            }
        }
        out
    }
    pub fn compaction_run(&self, files: Option<Vec<u64>>) -> Result<String, String> {
        if self.is_closed.load(Ordering::SeqCst) {
            return Err("StoreClosed: Engine is shut down".to_string());
        }
        if self.compaction.is_paused() {
            return Err("Paused: compaction is paused (resume with `compaction-resume`)".to_string());
        }
        let (fan_in, size_ratio, max_concurrent) = {
            let config = self.config.read().unwrap();
            (
                config.size_tiered_fan_in as usize,
                config.size_tiered_size_ratio,
                config.compaction_max_concurrent,
            )
        };
        let catalog_snapshot = self.catalog.lock().unwrap().tables.clone();
        let in_progress_snapshot = self.compaction.in_progress_snapshot();
        let (ids, selection_source) = match files {
            Some(explicit) => (explicit, "manual"),
            None => match picker::pick(
                &catalog_snapshot,
                &in_progress_snapshot,
                &picker::PickerConfig { fan_in, size_ratio },
            ) {
                Some(ids) => (ids, "picker"),
                None => {
                    return Ok(
                        "info: no compaction candidates available right now (need at least \
                        `size_tiered_fan_in` eligible SSTables)"
                        .to_string(),
                    )
                }
            },
        };
        println!("compaction: selected input SSTables (source={selection_source}) -> {ids:?}");
        if ids.len() < 2 {
            return Err("InvalidArgument: compaction requires at least 2 input SSTables".to_string());
        }
        let mut input_metas = Vec::with_capacity(ids.len());
        for id in &ids {
            match catalog_snapshot.iter().find(|t| t.id == *id) {
                Some(meta) => input_metas.push(meta.clone()),
                None => return Err(format!("InvalidArgument: SSTable id={id} not found in manifest")),
            }
        }
        if !self.compaction.try_mark_in_progress(&ids) {
            return Err("Conflict: one or more selected SSTables are already being compacted".to_string());
        }
        if !self.compaction.try_reserve_slot(max_concurrent) {
            self.compaction.clear_in_progress(&ids);
            return Err(format!(
                "Backpressure: compaction_max_concurrent={max_concurrent} already reached; try again shortly"
            ));
        }
        let result = self.run_compaction_job(input_metas);
        self.compaction.release_slot();
        self.compaction.clear_in_progress(&ids);
        // Možda ostaje još kandidata (npr. mnogo malih L0 tabela) — probudi
        // CompactionWorker da odmah proveri ponovo umesto da čeka tick.
        self.compaction_signal.notify();
        result
    }
    fn run_compaction_job(&self, inputs: Vec<SsTableManifestEntry>) -> Result<String, String> {
        let job_id = self.compaction.next_job_id();
        let start = Instant::now();
        let sstables_snapshot = self.sstables.read().unwrap().clone();
        let mut handles = Vec::with_capacity(inputs.len());
        for meta in &inputs {
            match sstables_snapshot.iter().find(|h| h.id() == meta.id) {
                Some(h) => handles.push(Arc::clone(h)),
                None => {
                    return Err(format!(
                        "InternalError: SSTable id={} je u manifestu ali nije otvorena u memoriji",
                        meta.id
                    ))
                }
            }
        }
        let bytes_in: u64 = inputs.iter().map(|m| m.file_size).sum();
        let keys_in: u64 = inputs.iter().map(|m| m.num_entries).sum();
        let mut scanners = Vec::with_capacity(handles.len());
        for h in &handles {
            match h.scan() {
                Ok(s) => scanners.push(s),
                Err(e) => {
                    return Err(format!(
                        "IOFailure: ne mogu da otvorim sken za SSTabelu id={}: {e}",
                        h.id()
                    ))
                }
            }
        }
        let mut merged_entries: Vec<sstable::SstEntryInput> = Vec::new();
        let mut tombstones_kept: u64 = 0;
        let tombstones_dropped: u64 = 0;
        for item in MergeIterator::new(scanners) {
            let entry = item.map_err(|e| {
                format!("IOFailure: greška pri čitanju ulazne SSTabele tokom kompakcije: {e}")
            })?;
            if entry.is_tombstone {
                tombstones_kept += 1;
            }
            merged_entries.push(entry);
        }
        let keys_out = merged_entries.len() as u64;
        let keys_dropped = keys_in.saturating_sub(keys_out);
        let (sst_dir, manifest_path, block_size, restart_interval, bloom_fpr, build_buffer_bytes, io_mb_per_s) = {
            let config = self.config.read().unwrap();
            (
                config.sst_dir.clone(),
                config.manifest_path.clone(),
                config.block_size,
                config.restart_interval,
                config.bloom_false_positive,
                config.max_build_buffer_mb.saturating_mul(1024 * 1024),
                config.compaction_io_mb_per_s,
            )
        };
        let sst_path = Path::new(&sst_dir);
        let output_id = self.next_sst_id.fetch_add(1, Ordering::SeqCst);
        let io_throttle = IoThrottle::new(io_mb_per_s);
        let output_meta = sstable::write_sstable_from_entries(
            sst_path,
            output_id,
            merged_entries,
            block_size,
            restart_interval,
            bloom_fpr,
            build_buffer_bytes,
            keys_out as usize,
            Some(&io_throttle),
        )
        .map_err(|e| format!("IOFailure: pisanje kompakcione izlazne SSTabele nije uspelo: {e}"))?;
        let _publish_guard = self.publish_lock.lock().unwrap();
        let epoch_for_this_job = self.next_epoch.fetch_add(1, Ordering::SeqCst);
        let input_ids: HashSet<u64> = inputs.iter().map(|m| m.id).collect();
        let mut candidate = {
            let catalog = self.catalog.lock().unwrap();
            catalog.clone()
        };
        let manifest_insert_pos = candidate
            .tables
            .iter()
            .position(|t| input_ids.contains(&t.id))
            .unwrap_or(0);
        candidate.tables.retain(|t| !input_ids.contains(&t.id));
        let manifest_insert_pos = manifest_insert_pos.min(candidate.tables.len());
        candidate.tables.insert(manifest_insert_pos, output_meta.clone());
        candidate.next_sst_id = candidate.next_sst_id.max(output_id + 1);
        candidate.epoch = epoch_for_this_job;
        candidate.save(&manifest_path).map_err(|e| {
            format!(
                "IOFailure: kompakciona izlazna SSTabela '{}' je upisana i fsync-ovana, ali update \
                manifesta nije uspeo: {e}. Izlazni fajl ostaje na disku kao orphan -- sledeći restart \
                će ga počistiti ako zaista nije referenciran.",
                output_meta.file_name
            )
        })?;
        {
            let mut catalog = self.catalog.lock().unwrap();
            *catalog = candidate;
        }
        let opened_output_table = match TableHandle::open(sst_path, &output_meta) {
            Ok(handle) => {
                let mut sstables = self.sstables.write().unwrap();
                let insert_pos = sstables
                    .iter()
                    .position(|h| input_ids.contains(&h.id()))
                    .unwrap_or(0);
                sstables.retain(|h| !input_ids.contains(&h.id()));
                let insert_pos = insert_pos.min(sstables.len());
                sstables.insert(insert_pos, Arc::new(handle));
                true
            }
            Err(e) => {
                eprintln!(
                    "warning: kompakcija je upisala izlaznu SSTabelu '{}' ali nije uspela odmah da je \
                    otvori za čitanje: {e}. Ulazne SSTabele ostaju otvorene i čitljive; restart je \
                    potreban da se stanje uskladi sa manifestom.",
                    output_meta.file_name
                );
                false
            }
        };
        self.build_and_swap_version(epoch_for_this_job);
    drop(_publish_guard);

    // Ulazne SSTabele se NE brišu ovde sinhrono — u ovom trenutku `handles`
    // (i eventualno još neka privremena kopija) veštački drže Arc reference,
    // pa bi strong_count skoro uvek bio pogrešno > 1. Umesto toga, stavljamo
    // ih u red za odloženo brisanje; stvarnu proveru i brisanje radi
    // `sweep_pending_deletions`, pozvan iz pozadinskog compaction workera,
    // tek kad prođe `tombstone_grace_seconds` I kad fajl više niko ne referenciše.
    let grace_seconds = self.config.read().unwrap().tombstone_grace_seconds;
    let deferred_at = Instant::now();
    let retired_files: Vec<String> = inputs.iter().map(|m| m.file_name.clone()).collect();
    if opened_output_table {
        let mut pending = self.pending_deletions.lock().unwrap();
        for (meta, handle) in inputs.iter().zip(handles.iter()) {
            pending.push(PendingDeletion {
                file_name: meta.file_name.clone(),
                path: sst_path.join(&meta.file_name),
                handle: Arc::clone(handle),
                deferred_at,
            });
        }
    }

    let duration_ms = start.elapsed().as_millis();
    let bytes_out = output_meta.file_size;
    let drop_pct = if bytes_in > 0 {
        100.0 * (1.0 - (bytes_out as f64 / bytes_in as f64))
    } else {
        0.0
    };
    let stats = JobStats {
        job_id,
        inputs: inputs.iter().map(|m| m.id).collect(),
        output: Some(output_meta.id),
        bytes_in,
        bytes_out,
        keys_in,
        keys_out,
        keys_dropped,
        tombstones_kept,
        tombstones_dropped,
        duration_ms,
    };
    self.compaction.record_job(stats);
    println!(
        "compact job={} inputs={} bytes_in={} bytes_out={} drop={:.1}% dur={}ms",
        job_id, inputs.len(), bytes_in, bytes_out, drop_pct, duration_ms
    );
    self.event_log.record(
        "COMPACTION",
        &[
            ("job_id", job_id.to_string()),
            ("input_ids", format!("{:?}", inputs.iter().map(|m| m.id).collect::<Vec<_>>())),
            ("output_id", output_meta.id.to_string()),
            ("output_file", output_meta.file_name.clone()),
            ("bytes_in", bytes_in.to_string()),
            ("bytes_out", bytes_out.to_string()),
            ("keys_in", keys_in.to_string()),
            ("keys_out", keys_out.to_string()),
            ("keys_dropped", keys_dropped.to_string()),
            ("tombstones_kept", tombstones_kept.to_string()),
            ("tombstones_dropped", tombstones_dropped.to_string()),
            ("retired_input_files", format!("{:?}", retired_files)),
            ("cleanup_grace_seconds", grace_seconds.to_string()),
            ("manifest_epoch", epoch_for_this_job.to_string()),
            ("duration_ms", duration_ms.to_string()),
        ],
    );
    Ok(format!(
        "success: compaction job={} merged {} input SSTable(s) into '{}'\n\
        • Inputs: {:?}\n\
        • Keys: in={} out={} dropped(duplicates)={}\n\
        • Tombstones: kept={} dropped={}\n\
        • Bytes: in={} out={} (drop {:.1}%)\n\
        • Retired inputs queued for cleanup: {:?}\n\
        •   (deleted after {}s grace period, once no longer referenced by any in-flight read)\n\
        • Manifest epoch / published version: {}\n\
        • Duration: {} ms",
        job_id, inputs.len(), output_meta.file_name,
        inputs.iter().map(|m| m.id).collect::<Vec<_>>(),
        keys_in, keys_out, keys_dropped,
        tombstones_kept, tombstones_dropped,
        bytes_in, bytes_out, drop_pct,
        retired_files, grace_seconds,
        epoch_for_this_job, duration_ms,
    ))
    }
    pub fn compaction_stats_string(&self) -> String {
        let running = self.compaction.running_jobs();
        let total_jobs = self.compaction.total_jobs_run();
        let paused = self.compaction.is_paused();
        let in_progress = self.compaction.in_progress_snapshot();
        let (sst_count, backlog_bytes) = {
            let catalog = self.catalog.lock().unwrap();
            let bytes: u64 = catalog.tables.iter().map(|t| t.file_size).sum();
            (catalog.tables.len(), bytes)
        };
        let (fan_in, size_ratio, tombstone_grace, max_concurrent, l0_trigger, l0_stop) = {
            let config = self.config.read().unwrap();
            (
                config.size_tiered_fan_in,
                config.size_tiered_size_ratio,
                config.tombstone_grace_seconds,
                config.compaction_max_concurrent,
                config.l0_compaction_trigger,
                config.l0_stop_writes,
            )
        };
        let wa_ratio_display = self
            .compaction
            .cumulative_wa_ratio()
            .map(|r| format!("{r:.3}"))
            .unwrap_or_else(|| "n/a (no jobs run yet)".to_string());
        let last_compaction_display = self
            .compaction
            .last_completed_at_ms()
            .map(|ms| ms.to_string())
            .unwrap_or_else(|| "n/a (no jobs run yet)".to_string());
        let mut out = format!(
            "--- Compaction Stats ---\n\
            Paused: {}\n\
            Running jobs: {} / {} max concurrent\n\
            Files currently in-progress: {:?}\n\
            Total jobs run (since server start): {}\n\
            Live SSTable count: {}\n\
            compaction_backlog_bytes (approx, all live SSTables): {}\n\
            wa_ratio (cumulative, bytes_out/bytes_in across all jobs): {}\n\
            last_compaction_ms (unix millis of most recent completed job): {}\n\
            --- Config ---\n\
            size_tiered_fan_in: {}\n\
            size_tiered_size_ratio: {}\n\
            tombstone_grace_seconds: {}\n\
            compaction_max_concurrent: {}\n\
            l0_compaction_trigger: {}\n\
            l0_stop_writes: {}\n",
            paused, running, max_concurrent, in_progress, total_jobs, sst_count, backlog_bytes,
            wa_ratio_display, last_compaction_display,
            fan_in, size_ratio, tombstone_grace, max_concurrent, l0_trigger, l0_stop,
        );
        match self.compaction.last_job_snapshot() {
            Some(job) => {
                out.push_str(&format!(
                    "--- Last Job ---\n\
                    job_id: {}\n\
                    inputs: {:?}\n\
                    output: {:?}\n\
                    bytes_in / bytes_out: {} / {}\n\
                    keys_in / keys_out / keys_dropped: {} / {} / {}\n\
                    tombstones kept / dropped: {} / {}\n\
                    duration_ms: {}\n",
                    job.job_id, job.inputs, job.output,
                    job.bytes_in, job.bytes_out,
                    job.keys_in, job.keys_out, job.keys_dropped,
                    job.tombstones_kept, job.tombstones_dropped,
                    job.duration_ms,
                ));
            }
            None => out.push_str("--- Last Job ---\n  (none yet)\n"),
        }
        out.push_str("------------------------\n");
        out
    }
    pub fn compaction_pause(&self) -> Result<String, String> {
        self.compaction.pause();
        self.event_log.record("COMPACTION_PAUSE", &[]);
        Ok("success: compaction paused".to_string())
    }
    pub fn compaction_resume(&self) -> Result<String, String> {
        self.compaction.resume();
        self.compaction_signal.notify();
        self.event_log.record("COMPACTION_RESUME", &[]);
        Ok("success: compaction resumed".to_string())
    }

    fn sweep_pending_deletions(&self) {
        let grace = Duration::from_secs(self.config.read().unwrap().tombstone_grace_seconds);

        // Uzmi ceo trenutni red i isprazni ga pod lock-om (brzo, bez I/O),
        // pa radi stvaran fajl I/O VAN lock-a.
        let candidates: Vec<PendingDeletion> = {
            let mut pending = self.pending_deletions.lock().unwrap();
            std::mem::take(&mut *pending)
        };

        let mut still_pending = Vec::with_capacity(candidates.len());
        for entry in candidates {
            let unreferenced = Arc::strong_count(&entry.handle) == 1;
            let grace_elapsed = entry.deferred_at.elapsed() >= grace;

            if unreferenced && grace_elapsed {
                match fs::remove_file(&entry.path) {
                    Ok(_) => {
                        println!(
                            "cleanup: obrisana odložena SSTabela '{}' (grace period od {}s istekao)",
                            entry.file_name, grace.as_secs()
                        );
                        self.event_log.record(
                            "DEFERRED_SST_DELETED",
                            &[
                                ("file_name", entry.file_name.clone()),
                                ("waited_secs", entry.deferred_at.elapsed().as_secs().to_string()),
                            ],
                        );
                    }
                    Err(e) => {
                        eprintln!(
                            "cleanup: brisanje odložene SSTabele '{}' nije uspelo: {e} (pokušaću ponovo)",
                            entry.file_name
                        );
                        still_pending.push(entry);   // probaj ponovo sledećeg tick-a
                    }
                }
            } else {
                still_pending.push(entry);   // ili nije prošao grace, ili je i dalje referenciran
            }
        }

        if !still_pending.is_empty() {
            let mut pending = self.pending_deletions.lock().unwrap();
            pending.extend(still_pending);  // extend, ne overwrite — da ne izgubimo nove unose
        }
    }

    /// FlushWorker — jedna stalna pozadinska nit. Prazni ImmutableQueue kad
    /// god ima posla (budi se odmah preko flush_signal, ili na svaki
    /// bg_tick_ms ako nema signala), i ni u kom slučaju ne čeka compaction.
    pub fn run_flush_worker(engine: &Arc<LsmEngine>) {
        loop {
            if engine.is_closed.load(Ordering::SeqCst) {
                return;
            }
            while !engine.is_closed.load(Ordering::SeqCst) && !engine.immutables_is_empty() {
                engine.flush_running.store(true, Ordering::SeqCst);
                match engine.flush_immutable() {
                    Ok(msg) => println!("flush-worker: {msg}"),
                    Err(e) => {
                        eprintln!("flush-worker: {e}");
                        // ne spinuj vrelo na uporne IO greške
                        thread::sleep(Duration::from_millis(200));
                    }
                }
            }
            engine.flush_running.store(false, Ordering::SeqCst);
            if engine.is_closed.load(Ordering::SeqCst) {
                return;
            }
            let tick_ms = engine.config.read().unwrap().bg_tick_ms.max(1);
            engine.flush_signal.wait(Duration::from_millis(tick_ms));
        }
    }
    /// CompactionWorker — jedna stalna pozadinska nit. Pravilo "flush beats
    /// compaction": ako ima immutable-a koji čekaju flush, ovaj krug se
    /// preskače bez diranja compaction stanja; FlushWorker će nas probuditi
    /// svojim notify() kad završi.
    pub fn run_compaction_worker(engine: &Arc<LsmEngine>) {
        loop {
            if engine.is_closed.load(Ordering::SeqCst) {
                return;
            }
            engine.sweep_pending_deletions();
            if !engine.immutables_is_empty() {
                // flush ima prioritet — ništa ne radimo ovaj krug
            } else if !engine.compaction.is_paused() && engine.should_auto_compact() {
                engine.compact_running.store(true, Ordering::SeqCst);
                match engine.compaction_run(None) {
                    Ok(msg) => println!("compaction-worker: {msg}"),
                    Err(e) => eprintln!("compaction-worker: {e}"),
                }
                engine.compact_running.store(false, Ordering::SeqCst);
                // možda ima još kandidata — proveri odmah, ne čekaj tick
                continue;
            }
            if engine.is_closed.load(Ordering::SeqCst) {
                return;
            }
            let tick_ms = engine.config.read().unwrap().bg_tick_ms.max(1);
            engine.compaction_signal.wait(Duration::from_millis(tick_ms));
        }
    }
}