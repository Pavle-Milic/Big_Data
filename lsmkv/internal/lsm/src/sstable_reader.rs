//! SSTable reader (Sekcija 4): otvara SSTabele nabrojane u manifestu,
//! drži njihove lagane metapodatke (`TableHandle`) rezidentne u memoriji,
//! i implementira ugovor pretrage jedne tabele (range check -> Bloom
//! check -> index search -> data block scan) plus prateći block cache i
//! file-descriptor cache. Pisanje SSTabela nije u opsegu ovog modula (v.
//! `sstable.rs`); ovaj modul samo *čita* fajlove koje taj modul
//! proizvodi, pa znanje o rasporedu na disku (footer/index/blok
//! enkodiranje) živi u `sstable` i ovde se koristi preko `pub(crate)`
//! helpera.

use crate::manifest::SsTableManifestEntry;
use crate::memtable::Lookup;
use crate::sstable;
use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Zajednička konvencija poruke greške za korupciju otkrivenu tokom read
/// path-a (spec §4.3.D). Jedno mesto umesto ponavljanja istog `format!` na
/// više poziva — manje rizika od greške u pisanju.
fn corrupt_err(path: &Path, e: impl std::fmt::Display) -> String {
    format!("CorruptionDetected: '{}': {e}", path.display())
}

// =======================================================================
// TableHandle (§4.2)
// =======================================================================

/// In-memory metapodaci za jedan `.sst` fajl na disku: footer pokazivači
/// plus sve što je potrebno da se lookup jeftino odbije (opseg ključeva,
/// Bloom filter) bez ikakvog dodirivanja data blokova. Učitava se jednom
/// kad se tabela otvori — na startu engine-a za svaku tabelu već u
/// manifestu, i ponovo odmah posle svakog flush-a — i ostaje rezidentan
/// dok je tabela živa; dovoljno je mali da nikad ne mora da se izbaci iz
/// memorije (za razliku od block cache-a i FD cache-a ispod, kojima
/// eviction itekako treba).
pub struct TableHandle {
    id: u64,
    file_path: PathBuf,
    file_size: u64,
    min_key: String,
    max_key: String,
    min_seq_no: u64,
    max_seq_no: u64,
    num_entries: u64,
    index_offset: u64,
    index_size: u64,
    bloom: sstable::BloomFilter,
}

impl TableHandle {
    pub fn open(sst_dir: &Path, entry: &SsTableManifestEntry) -> std::io::Result<Self> {
        // ... (nepromenjeno, isto kao pre)
        let file_path = sst_dir.join(&entry.file_name);
        let mut file = File::open(&file_path)?;
        let file_size = file.metadata()?.len();

        let footer = sstable::read_footer_at(&mut file, file_size, &file_path)?;

        let filter_body = sstable::read_verified_block(
            &mut file,
            footer.filter_offset,
            footer.filter_size,
            file_size,
            &file_path,
            "Filter",
        )?;
        let bloom = sstable::BloomFilter::deserialize(&filter_body).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("'{}' has a corrupt Bloom filter block: {e}", file_path.display()),
            )
        })?;

        Ok(Self {
            id: entry.id,
            file_path,
            file_size,
            min_key: entry.min_key.clone(),
            max_key: entry.max_key.clone(),
            min_seq_no: footer.min_seq_no,
            max_seq_no: footer.max_seq_no,
            num_entries: footer.num_entries,
            index_offset: footer.index_offset,
            index_size: footer.index_size,
            bloom,
        })
    }
    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn file_name(&self) -> String {
        self.file_path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default()
    }

    pub fn min_key(&self) -> &str {
        &self.min_key
    }

    pub fn max_key(&self) -> &str {
        &self.max_key
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    pub fn num_entries(&self) -> u64 {
        self.num_entries
    }

    pub fn min_seq_no(&self) -> u64 {
        self.min_seq_no
    }

    pub fn max_seq_no(&self) -> u64 {
        self.max_seq_no
    }
    /// Otvara sekvencijalni, punotabelarni iterator preko OVE SSTabele
    /// (§6.5 "streams multiple SSTables ... via per-table iterators"):
    /// prolazi kroz SVAKI unos, rastuće po ključu, zaobilazeći Bloom/range
    /// proveru iz `lookup_in_table` (koje imaju smisla samo za pretragu
    /// jednog ključa, ne za pun sken).
    ///
    /// Namerno otvara SOPSTVENI `File` handle, mimo `FileHandleCache`/
    /// `BlockCache`: kompakcija čita svaki blok tačno jednom, pa bi
    /// prolazak kroz deljeni block cache samo isterao "toplije" blokove
    /// koje foreground `get` saobraćaj aktivno koristi, bez ikakve koristi
    /// za samu kompakciju.
    pub fn scan(&self) -> std::io::Result<SsTableScanIterator> {
        SsTableScanIterator::open(self)
    }
}

// =======================================================================
// Sekcija 6 priprema: pun (sekvencijalni) sken jedne SSTabele -- gradivni
// blok kompakcionog merge-a (§6.5). Bez Bloom/range logike iz
// `lookup_in_table`: ovde nam trebaju BAŠ SVI unosi, po redu.
// =======================================================================

/// Sekvencijalni iterator preko svih unosa jedne SSTabele, rastuće po
/// ključu (isti redosled u kom su i upisani, v. `sstable::BlockBuilder`).
/// Čita index blok jednom pri otvaranju, pa zatim data blokove jedan po
/// jedan, tek kad su prethodno pušteni unosi potrošeni -- cela tabela
/// NIKAD nije učitana u memoriju odjednom (§6.5 "streams ... SSTables").
pub struct SsTableScanIterator {
    file: File,
    file_path: PathBuf,
    file_size: u64,
    index_entries: Vec<sstable::IndexEntry>,
    next_index_pos: usize,
    pending: VecDeque<sstable::DecodedEntry>,
}

impl SsTableScanIterator {
    fn open(handle: &TableHandle) -> std::io::Result<Self> {
        let mut file = File::open(&handle.file_path)?;
        let index_body = sstable::read_verified_block(
            &mut file,
            handle.index_offset,
            handle.index_size,
            handle.file_size,
            &handle.file_path,
            "Index",
        )?;
        let index_entries = sstable::parse_index_block(&index_body)?;

        Ok(Self {
            file,
            file_path: handle.file_path.clone(),
            file_size: handle.file_size,
            index_entries,
            next_index_pos: 0,
            pending: VecDeque::new(),
        })
    }

    /// Učitava sledeći data blok (ako postoji) i puni `pending` njegovim
    /// dekodiranim unosima. Vraća `Ok(false)` kad su svi blokovi potrošeni.
    fn load_next_block(&mut self) -> std::io::Result<bool> {
        if self.next_index_pos >= self.index_entries.len() {
            return Ok(false);
        }

        let index_entry = &self.index_entries[self.next_index_pos];
        let block_bytes = sstable::read_verified_block(
            &mut self.file,
            index_entry.offset,
            index_entry.length,
            self.file_size,
            &self.file_path,
            "Data",
        )?;
        let parsed = sstable::parse_data_block(&block_bytes)?;
        let decoded = sstable::decode_all_entries(&parsed)?;

        self.pending = decoded.into_iter().collect();
        self.next_index_pos += 1;
        Ok(true)
    }
}

impl Iterator for SsTableScanIterator {
    type Item = std::io::Result<sstable::DecodedEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(entry) = self.pending.pop_front() {
                return Some(Ok(entry));
            }
            match self.load_next_block() {
                Ok(true) => continue,
                Ok(false) => return None,
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

// =======================================================================
// Block cache (§4.4): LRU deljen između svih otvorenih tabela.
// =======================================================================

#[derive(Hash, Eq, PartialEq, Clone, Copy)]
struct BlockKey {
    table_id: u64,
    offset: u64,
}

struct BlockCacheInner {
    map: HashMap<BlockKey, Arc<Vec<u8>>>,
    /// Front = least-recently-used, back = most-recently-used. Obična
    /// `VecDeque` umesto intrusive linked liste: pomeranje unosa na MRU
    /// je O(n) po broju *keširanih blokova*, što je za kurs-razmerni
    /// block cache dovoljno malo da ne bude bitno, a mnogo je manje koda
    /// (i rizika) nego ručno pisanje prave LRU liste.
    order: VecDeque<BlockKey>,
    current_bytes: u64,
}

/// LRU keš dekompresovanih data blokova (i, kad je `cache_index_blocks`
/// uključen, index blokova takođe) deljen između svakog otvorenog
/// `TableHandle`-a (spec §4.4). Eviction je čisto vođen bajt-budžetom iz
/// `block_cache_mb`.
pub struct BlockCache {
    capacity_bytes: u64,
    inner: Mutex<BlockCacheInner>,
}

impl BlockCache {
    pub fn new(capacity_mb: usize) -> Self {
        Self {
            capacity_bytes: (capacity_mb as u64).saturating_mul(1024 * 1024),
            inner: Mutex::new(BlockCacheInner {
                map: HashMap::new(),
                order: VecDeque::new(),
                current_bytes: 0,
            }),
        }
    }

    fn get(&self, key: &BlockKey) -> Option<Arc<Vec<u8>>> {
        let mut inner = self.inner.lock().unwrap();
        let hit = inner.map.get(key).cloned();
        if hit.is_some() {
            if let Some(pos) = inner.order.iter().position(|k| k == key) {
                inner.order.remove(pos);
            }
            inner.order.push_back(*key);
        }
        hit
    }

    fn insert(&self, key: BlockKey, bytes: Arc<Vec<u8>>) {
        let mut inner = self.inner.lock().unwrap();
        let size = bytes.len() as u64;

        if let Some(old) = inner.map.remove(&key) {
            inner.current_bytes = inner.current_bytes.saturating_sub(old.len() as u64);
            if let Some(pos) = inner.order.iter().position(|k| k == &key) {
                inner.order.remove(pos);
            }
        }

        inner.map.insert(key, bytes);
        inner.order.push_back(key);
        inner.current_bytes += size;

        while inner.current_bytes > self.capacity_bytes {
            match inner.order.pop_front() {
                Some(evict_key) => {
                    if let Some(evicted) = inner.map.remove(&evict_key) {
                        inner.current_bytes = inner.current_bytes.saturating_sub(evicted.len() as u64);
                    }
                }
                None => break,
            }
        }
    }
}

// =======================================================================
// File-descriptor cache (§4.4)
// =======================================================================

struct FileHandleCacheInner {
    map: HashMap<u64, Arc<Mutex<File>>>,
    order: VecDeque<u64>,
}

/// Ograničeni pool otvorenih `File` handle-ova, po jedan po id-u tabele,
/// tako da mnogo preklapajućih L0 SSTabela ne probije OS limit otvorenih
/// fajlova (spec §4.4). Handle-ovi se otvaraju lenjo pri prvoj upotrebi i
/// izbacuju least-recently-used kad se pređe `max_open_files`; tabeli
/// čiji je handle izbačen se prosto ponovo otvara fajl sledeći put kad
/// zatreba.
pub struct FileHandleCache {
    max_open_files: usize,
    inner: Mutex<FileHandleCacheInner>,
}

impl FileHandleCache {
    pub fn new(max_open_files: usize) -> Self {
        Self {
            max_open_files: max_open_files.max(1),
            inner: Mutex::new(FileHandleCacheInner {
                map: HashMap::new(),
                order: VecDeque::new(),
            }),
        }
    }

    fn get_or_open(&self, table_id: u64, path: &Path) -> std::io::Result<Arc<Mutex<File>>> {
        {
            let mut inner = self.inner.lock().unwrap();
            if let Some(handle) = inner.map.get(&table_id).cloned() {
                if let Some(pos) = inner.order.iter().position(|id| *id == table_id) {
                    inner.order.remove(pos);
                }
                inner.order.push_back(table_id);
                return Ok(handle);
            }
        }

        // Otvaramo van brave -- spor disk ne treba da blokira lookup-e
        // drugih niti u kešu.
        let file = File::open(path)?;
        let handle = Arc::new(Mutex::new(file));

        let mut inner = self.inner.lock().unwrap();
        // Neka druga nit je možda već otvorila (i kеširala) istu tabelu
        // dok mi nismo držali bravu; zadrži koji god handle je prvi ušao,
        // da ne procuri duplirani deskriptor.
        if let Some(existing) = inner.map.get(&table_id).cloned() {
            return Ok(existing);
        }

        inner.map.insert(table_id, Arc::clone(&handle));
        inner.order.push_back(table_id);

        while inner.map.len() > self.max_open_files {
            match inner.order.pop_front() {
                Some(evict_id) => {
                    inner.map.remove(&evict_id);
                }
                None => break,
            }
        }

        Ok(handle)
    }
}

// =======================================================================
// Statistika (§4.1/§4.7 `stats`) i po-poziv trag za debug `get` (§4.7)
// =======================================================================

/// Kumulativni brojači kroz ceo život engine-a, štampaju se u `stats`.
#[derive(Default)]
pub struct ReadStats {
    blooms_checked: AtomicU64,
    blooms_negative: AtomicU64,
    block_cache_hits: AtomicU64,
    block_cache_misses: AtomicU64,
    disk_block_reads: AtomicU64,
}

impl ReadStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> ReadStatsSnapshot {
        ReadStatsSnapshot {
            blooms_checked: self.blooms_checked.load(Ordering::Relaxed),
            blooms_negative: self.blooms_negative.load(Ordering::Relaxed),
            block_cache_hits: self.block_cache_hits.load(Ordering::Relaxed),
            block_cache_misses: self.block_cache_misses.load(Ordering::Relaxed),
            disk_block_reads: self.disk_block_reads.load(Ordering::Relaxed),
        }
    }
}

pub struct ReadStatsSnapshot {
    pub blooms_checked: u64,
    pub blooms_negative: u64,
    pub block_cache_hits: u64,
    pub block_cache_misses: u64,
    pub disk_block_reads: u64,
}

/// Brojači specifični za JEDAN `get` poziv, korišćeni za debug trace liniju
/// iz spec §4.7 ("memtable?, immutables consulted, sstables_consulted,
/// blooms_skipped, block_reads, cache_hits").
#[derive(Default)]
pub struct LookupTrace {
    pub checked_memtable: bool,
    pub immutables_consulted: usize,
    pub sstables_consulted: usize,
    pub blooms_skipped: usize,
    pub block_reads: usize,
    pub cache_hits: usize,
}

// =======================================================================
// Lookup ugovor (§4.3)
// =======================================================================

/// Range check + Bloom check + index search + data block scan za jednu
/// tabelu (spec §4.3.B). Vraća `Lookup::NotFound` ako ključ sigurno nije
/// u ovoj tabeli; inače odgovor same tabele (koji može biti i tombstone).
fn lookup_in_table(
    handle: &TableHandle,
    key: &str,
    fd_cache: &FileHandleCache,
    block_cache: &BlockCache,
    cache_index_blocks: bool,
    stats: &ReadStats,
    trace: &mut LookupTrace,
) -> Result<Lookup, String> {
    // A) Range check -- jeftino, bez I/O.
    if key < handle.min_key.as_str() || key > handle.max_key.as_str() {
        return Ok(Lookup::NotFound);
    }

    // B) Bloom check -- Blomovi mogu lagati pozitivno, nikad negativno.
    stats.blooms_checked.fetch_add(1, Ordering::Relaxed);
    if !handle.bloom.may_contain(key.as_bytes()) {
        stats.blooms_negative.fetch_add(1, Ordering::Relaxed);
        trace.blooms_skipped += 1;
        return Ok(Lookup::NotFound);
    }

    // C) Index search: učitaj (mali) index blok, pa ga binarno pretraži.
    let index_entries = load_index_block(handle, fd_cache, block_cache, cache_index_blocks, stats, trace)?;
    let (block_offset, block_length) = match find_block_for_key(&index_entries, key) {
        Some(entry) => (entry.offset, entry.length),
        None => return Ok(Lookup::NotFound),
    };

    // D) Čitanje data bloka (dekompresija za sad no-op dok compression=off).
    let block_bytes = load_data_block(handle, block_offset, block_length, fd_cache, block_cache, stats, trace)?;

    // E) In-block pretraga preko restart tačaka.
    match search_data_block(&block_bytes, key).map_err(|e| corrupt_err(&handle.file_path, e))? {
        Some(entry) if entry.is_tombstone => Ok(Lookup::Tombstone),
        Some(entry) => Ok(Lookup::Found(entry.value.unwrap_or_default())),
        None => Ok(Lookup::NotFound),
    }
}

/// Pretražuje `tables` u datom redosledu -- spec §4.3.C zahteva
/// newest-to-oldest -- i vraća odgovor prve tabele koji nije `NotFound`.
/// Tombstone zaustavlja pretragu isto kao i stvarna vrednost: u oba
/// slučaja, odgovor te tabele je konačan (§4.3.C).
pub fn search_sstables(
    tables: &[Arc<TableHandle>],
    key: &str,
    fd_cache: &FileHandleCache,
    block_cache: &BlockCache,
    cache_index_blocks: bool,
    stats: &ReadStats,
    trace: &mut LookupTrace,
) -> Result<Lookup, String> {
    for table in tables {
        trace.sstables_consulted += 1;
        match lookup_in_table(table, key, fd_cache, block_cache, cache_index_blocks, stats, trace)? {
            Lookup::NotFound => continue,
            found => return Ok(found),
        }
    }
    Ok(Lookup::NotFound)
}

/// Binarna pretraga za najveći index unos čiji je `first_key <= key`
/// (spec §4.3.B): samo taj blok može eventualno sadržati `key`, pošto su
/// unosi sortirani i svaki unos označava gde blok *počinje*.
fn find_block_for_key<'a>(entries: &'a [sstable::IndexEntry], key: &str) -> Option<&'a sstable::IndexEntry> {
    let idx = entries.partition_point(|e| e.first_key.as_str() <= key);
    if idx == 0 {
        None
    } else {
        Some(&entries[idx - 1])
    }
}

fn load_index_block(
    handle: &TableHandle,
    fd_cache: &FileHandleCache,
    block_cache: &BlockCache,
    cache_index_blocks: bool,
    stats: &ReadStats,
    trace: &mut LookupTrace,
) -> Result<Vec<sstable::IndexEntry>, String> {
    let cache_key = BlockKey { table_id: handle.id, offset: handle.index_offset };

    if cache_index_blocks {
        if let Some(bytes) = block_cache.get(&cache_key) {
            stats.block_cache_hits.fetch_add(1, Ordering::Relaxed);
            trace.cache_hits += 1;
            return sstable::parse_index_block(&bytes).map_err(|e| corrupt_err(&handle.file_path, e));
        }
        stats.block_cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    let bytes = Arc::new(read_block_from_disk(handle, handle.index_offset, handle.index_size, fd_cache, "Index")?);
    stats.disk_block_reads.fetch_add(1, Ordering::Relaxed);
    trace.block_reads += 1;

    let parsed = sstable::parse_index_block(&bytes).map_err(|e| corrupt_err(&handle.file_path, e))?;

    if cache_index_blocks {
        block_cache.insert(cache_key, bytes);
    }

    Ok(parsed)
}

fn load_data_block(
    handle: &TableHandle,
    offset: u64,
    length: u64,
    fd_cache: &FileHandleCache,
    block_cache: &BlockCache,
    stats: &ReadStats,
    trace: &mut LookupTrace,
) -> Result<Arc<Vec<u8>>, String> {
    let cache_key = BlockKey { table_id: handle.id, offset };

    if let Some(bytes) = block_cache.get(&cache_key) {
        stats.block_cache_hits.fetch_add(1, Ordering::Relaxed);
        trace.cache_hits += 1;
        return Ok(bytes);
    }
    stats.block_cache_misses.fetch_add(1, Ordering::Relaxed);

    let bytes = Arc::new(read_block_from_disk(handle, offset, length, fd_cache, "Data")?);
    stats.disk_block_reads.fetch_add(1, Ordering::Relaxed);
    trace.block_reads += 1;

    block_cache.insert(cache_key, Arc::clone(&bytes));

    Ok(bytes)
}

/// Otvara (ili ponovo koristi keširan) file handle preko `fd_cache` i čita
/// jedan verifikovan blok sa diska (spec §4.3.D: checksum mismatch ili
/// out-of-bounds pristup postaje `CorruptionDetected`, propraćen logom
/// imena fajla i offseta unutar `sstable::read_verified_block`).
fn read_block_from_disk(
    handle: &TableHandle,
    offset: u64,
    length: u64,
    fd_cache: &FileHandleCache,
    block_name: &str,
) -> Result<Vec<u8>, String> {
    let file_handle = fd_cache
        .get_or_open(handle.id, &handle.file_path)
        .map_err(|e| format!("IOFailure: failed to open '{}': {e}", handle.file_path.display()))?;

    let mut file = file_handle.lock().unwrap();
    sstable::read_verified_block(&mut file, offset, length, handle.file_size, &handle.file_path, block_name)
        .map_err(|e| corrupt_err(&handle.file_path, e))
}

/// In-block pretraga preko restart tačaka (spec §4.3.B): binarno skoči na
/// restart tačku čiji je ključ <= `key`, pa linearno rekonstruiši ključeve
/// (delta-dekodiranje) odatle dok se `key` ne pronađe ili ne pretekne.
fn search_data_block(
    block_bytes: &[u8],
    key: &str,
) -> std::io::Result<Option<sstable::DecodedEntry>> {
    let parsed = sstable::parse_data_block(block_bytes)?;
    if parsed.restarts.is_empty() {
        return Ok(None);
    }

    // Binarna pretraga po restart tačkama: svaka restart tačka čuva pun
    // ključ (shared_len == 0), pa ga dekodiramo iz prazne "last_key"
    // isečke da dobijemo taj puni ključ bez efekata na susedne unose.
    let mut lo = 0usize;
    let mut hi = parsed.restarts.len(); // pretraga na [lo, hi)
    while lo + 1 < hi {
        let mid = lo + (hi - lo) / 2;
        let restart_offset = parsed.restarts[mid] as usize;
        let (entry, _) = sstable::decode_entry_at(&parsed.entries_buf, restart_offset, &[])?;
        if entry.key.as_str() <= key {
            lo = mid;
        } else {
            hi = mid;
        }
    }

    // Linearno skeniranje od restart tačke `lo`, rekonstruišući pune
    // ključeve preko delta-dekodiranja dok ne pretečemo `key` ili ne
    // dođemo do kraja bloka.
    let mut pos = parsed.restarts[lo] as usize;
    let mut last_key: Vec<u8> = Vec::new();

    while pos < parsed.entries_buf.len() {
        let (entry, next_pos) = sstable::decode_entry_at(&parsed.entries_buf, pos, &last_key)?;

        match entry.key.as_str().cmp(key) {
            std::cmp::Ordering::Equal => return Ok(Some(entry)),
            std::cmp::Ordering::Greater => return Ok(None), // pretekli smo -- ključa nema u ovom bloku
            std::cmp::Ordering::Less => {
                last_key.clear();
                last_key.extend_from_slice(entry.key.as_bytes());
                pos = next_pos;
            }
        }
    }

    Ok(None)
}