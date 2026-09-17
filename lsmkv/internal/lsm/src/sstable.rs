//! SSTable writer (Sekcija 3): pretvara jednu immutable `Memtable` u
//! sortiran, immutable fajl na disku. Čitanje SSTabela je dodato u
//! Sekciji 4 (v. blok "Sekcija 4: reader support" niže i modul
//! `sstable_reader`) — ovaj fajl i dalje samo *proizvodi* fajlove, dok
//! `sstable_reader` iznad njega gradi `TableHandle`/block-cache/lookup
//! logiku; ovde žive samo funkcije koje razumeju tačan bajt-raspored
//! (footer/index/data-block/bloom enkodiranje), da bi ostale sinhronizovane
//! sa writer-om na jednom mestu.
//!
//! Raspored na disku (spec §3.5):
//!
//!   [data block 0][data block 1]...[data block N-1]
//!   [filter block]
//!   [index block]
//!   [footer]                                   <- fiksne veličine, na kraju fajla
//!
//! Svaki blok (data/filter/index) se završava sopstvenim checksum-om.
//! Footer je fiksne veličine tako da se uvek može naći seek-ovanjem na
//! `file_len - FOOTER_SIZE`, i sadrži eksplicitne offsete/dužine filter i
//! index blokova (data blokovi se nikad ne skeniraju linearno, samo preko
//! indexa).

use crate::fsutil;
use crate::manifest::{BloomParams, SsTableManifestEntry};
use crate::memtable::Memtable;
use std::collections::hash_map::DefaultHasher;
use std::fs::{self, File};
use std::hash::{Hash, Hasher};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use crate::compaction::IoThrottle;

const SST_MAGIC: &[u8; 8] = b"LSMSSTB1";
const SST_FOOTER_VERSION: u8 = 1;
/// version(1) + padding(3) + 7 x u64(56) + checksum(4) + magic(8) = 72
const FOOTER_SIZE: usize = 1 + 3 + 8 * 7 + 4 + 8;

/// Ista tehnika kao `wal::calculate_checksum_32`, ali samostalna kopija —
/// ovaj modul ne zavisi od unutrašnjosti WAL-a.
pub(crate) fn calculate_checksum_32(payload: &[u8]) -> u32 {
    let mut hasher = DefaultHasher::new();
    payload.hash(&mut hasher);
    hasher.finish() as u32
}

// ---------------------------------------------------------------------
// Varint (unsigned LEB128) — koristi se unutar data blokova.
// ---------------------------------------------------------------------

fn write_varint(buf: &mut Vec<u8>, mut value: u64) {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if value == 0 {
            break;
        }
    }
}

/// Inverse od `write_varint`: dekodira jedan unsigned LEB128 varint sa
/// početka `buf`-a. Vraća dekodiranu vrednost i broj pročitanih bajtova,
/// da pozivalac može da pomeri sopstveni kursor.
pub(crate) fn read_varint(buf: &[u8]) -> std::io::Result<(u64, usize)> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    for (i, &byte) in buf.iter().enumerate() {
        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok((result, i + 1));
        }
        shift += 7;
        if shift >= 64 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "varint predugačak (prekoračen 64-bitni opseg)",
            ));
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "neočekivan kraj bafera pri čitanju varint-a",
    ))
}

fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

// ---------------------------------------------------------------------
// Data block builder: prefix-kompresovani unosi sa periodičnim restart
// tačkama (§3.5 / §3.6).
// ---------------------------------------------------------------------

/// Jedan unos u data bloku, na disku, izgleda ovako:
///
///   shared_len:     varint  (broj bajtova deljenih sa prethodnim ključem;
///                            0 na restart tački)
///   non_shared_len: varint  (preostali, doslovni bajtovi ključa)
///   record_type:    u8      (0 = Put, 1 = Delete/tombstone)
///   value_len:      varint  (uvek 0 za Delete)
///   seq_no:         8 bajtova LE
///   key_delta:      non_shared_len bajtova
///   value:          value_len bajtova (potpuno izostavljeno za Delete)
///
/// Restart tačka čuva pun ključ (shared_len == 0), tako da čitalac može
/// binarno da pretraži listu restart tačaka i samo od najbliže restart
/// tačke linearno rekonstruiše ključeve, umesto od samog početka bloka.
struct BlockBuilder {
    buffer: Vec<u8>,
    restarts: Vec<u32>,
    restart_interval: usize,
    entries_since_restart: usize,
    last_key: Vec<u8>,
}

impl BlockBuilder {
    fn new(restart_interval: usize) -> Self {
        Self {
            buffer: Vec::new(),
            restarts: Vec::new(),
            restart_interval: restart_interval.max(1),
            entries_since_restart: 0,
            last_key: Vec::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Približna veličina kad bi se blok završio upravo sad (koristi se da
    /// se odluči kada preseći novi blok u odnosu na konfigurisani
    /// `block_size`).
    fn estimated_size(&self) -> usize {
        self.buffer.len() + self.restarts.len() * 4 + 4 + 4
    }

    fn add(&mut self, key: &str, is_tombstone: bool, seq_no: u64, value: Option<&str>) {
        let key_bytes = key.as_bytes();

        let is_restart =
            self.entries_since_restart == 0 || self.entries_since_restart >= self.restart_interval;

        let shared_len = if is_restart {
            0
        } else {
            common_prefix_len(&self.last_key, key_bytes)
        };

        if is_restart {
            self.restarts.push(self.buffer.len() as u32);
            self.entries_since_restart = 0;
        }

        let non_shared = &key_bytes[shared_len..];
        let value_bytes: &[u8] = if is_tombstone {
            &[]
        } else {
            value.unwrap_or("").as_bytes()
        };

        write_varint(&mut self.buffer, shared_len as u64);
        write_varint(&mut self.buffer, non_shared.len() as u64);
        self.buffer.push(if is_tombstone { 1 } else { 0 });
        write_varint(&mut self.buffer, value_bytes.len() as u64);
        self.buffer.extend_from_slice(&seq_no.to_le_bytes());
        self.buffer.extend_from_slice(non_shared);
        self.buffer.extend_from_slice(value_bytes);

        self.last_key.clear();
        self.last_key.extend_from_slice(key_bytes);
        self.entries_since_restart += 1;
    }

    /// Konzumira builder i proizvodi finalne bajtove bloka: unosi, pa
    /// tabela restart offseta, pa broj restart tačaka, pa checksum preko
    /// svega ispred njega.
    fn finish(self) -> Vec<u8> {
        let mut buf = self.buffer;
        for restart_offset in &self.restarts {
            buf.extend_from_slice(&restart_offset.to_le_bytes());
        }
        buf.extend_from_slice(&(self.restarts.len() as u32).to_le_bytes());
        let checksum = calculate_checksum_32(&buf);
        buf.extend_from_slice(&checksum.to_le_bytes());
        buf
    }
}

// ---------------------------------------------------------------------
// Bloom filter
// ---------------------------------------------------------------------

/// FNV-1a sa seed-om, da dobijemo dva jeftina, dovoljno nezavisna hash-a
/// bez oslanjanja na `DefaultHasher`-ov nespecifikovan algoritam (na koji
/// se već oslanjaju WAL-ovi checksum-i, ali na koji ne bismo hteli da se
/// oslonimo i ovde na drugačiji način).
fn fnv1a_64(seed: u64, data: &[u8]) -> u64 {
    let mut hash = seed ^ 0xcbf29ce484222325;
    for &byte in data {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

/// Standardni bit-array Bloom filter sa double hashing-om
/// (`h_i = h1 + i*h2`) za izvođenje `num_hashes` bit pozicija iz dva
/// osnovna hash-a po ključu. `pub(crate)` jer je Sekcija 4 (`sstable_reader`)
/// deserijalizuje i pretražuje isti tip pri čitanju.
pub(crate) struct BloomFilter {
    bits: Vec<u8>,
    num_bits: u64,
    num_hashes: u32,
}

impl BloomFilter {
    /// Dimenzioniše filter na osnovu očekivanog broja ključeva i ciljanog
    /// false-positive rate-a, po formulama iz spec §3.5:
    ///   bitsPerKey ~= -ln(fpr) / (ln 2)^2
    ///   k          ~= ln 2 * bitsPerKey
    fn new(expected_keys: usize, false_positive_rate: f64) -> Self {
        let fpr = false_positive_rate.clamp(0.000_001, 0.5);
        let n = expected_keys.max(1) as f64;

        let bits_per_key = (-fpr.ln()) / std::f64::consts::LN_2.powi(2);
        let mut num_bits = (n * bits_per_key).ceil() as u64;
        num_bits = num_bits.max(64);
        num_bits = ((num_bits + 7) / 8) * 8; // zaokruži na ceo broj bajtova

        let num_hashes = (std::f64::consts::LN_2 * bits_per_key).round().max(1.0) as u32;
        let num_hashes = num_hashes.min(30);

        let num_bytes = (num_bits / 8) as usize;
        Self {
            bits: vec![0u8; num_bytes],
            num_bits,
            num_hashes,
        }
    }

    fn bit_indices(key: &[u8], num_bits: u64, num_hashes: u32) -> impl Iterator<Item = u64> {
        let h1 = fnv1a_64(0x1234_5678_9abc_def0, key);
        let h2 = fnv1a_64(0x0fed_cba9_8765_4321, key).max(1);
        (0..num_hashes).map(move |i| h1.wrapping_add((i as u64).wrapping_mul(h2)) % num_bits)
    }

    fn add(&mut self, key: &[u8]) {
        for idx in Self::bit_indices(key, self.num_bits, self.num_hashes) {
            let byte_idx = (idx / 8) as usize;
            let bit_idx = (idx % 8) as u8;
            self.bits[byte_idx] |= 1 << bit_idx;
        }
    }

    /// Koristi se u Sekciji 4 (read path): "definitely not" ako vrati
    /// `false`; Blomovi mogu lagati pozitivno, nikad negativno.
    pub(crate) fn may_contain(&self, key: &[u8]) -> bool {
        Self::bit_indices(key, self.num_bits, self.num_hashes).all(|idx| {
            let byte_idx = (idx / 8) as usize;
            let bit_idx = (idx % 8) as u8;
            (self.bits[byte_idx] & (1 << bit_idx)) != 0
        })
    }

    fn num_bits(&self) -> u64 {
        self.num_bits
    }

    fn num_hashes(&self) -> u32 {
        self.num_hashes
    }

    fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8 + 4 + self.bits.len() + 4);
        buf.extend_from_slice(&self.num_bits.to_le_bytes());
        buf.extend_from_slice(&self.num_hashes.to_le_bytes());
        buf.extend_from_slice(&self.bits);
        let checksum = calculate_checksum_32(&buf);
        buf.extend_from_slice(&checksum.to_le_bytes());
        buf
    }

    /// Parsira telo Bloom filter bloka (već pročitano i verifikovano preko
    /// `read_verified_block`, tj. bez trailing checksum-a) nazad u
    /// pretraživ `BloomFilter`. Ogledalo `serialize`-a iznad.
    pub(crate) fn deserialize(body: &[u8]) -> std::io::Result<Self> {
        if body.len() < 12 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "bloom filter blok je premali",
            ));
        }
        let num_bits = u64::from_le_bytes(body[0..8].try_into().unwrap());
        let num_hashes = u32::from_le_bytes(body[8..12].try_into().unwrap());
        let expected_bytes = ((num_bits + 7) / 8) as usize;
        if body.len() != 12 + expected_bytes {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "bloom filter blok ima neusaglašenu dužinu",
            ));
        }
        let bits = body[12..12 + expected_bytes].to_vec();
        Ok(Self { bits, num_bits, num_hashes })
    }
}

// ---------------------------------------------------------------------
// Index block: sparse (firstKeyInBlock -> offset/length), sortiran po ključu.
// ---------------------------------------------------------------------

/// `pub(crate)` (i polja takođe) jer `sstable_reader` direktno čita
/// `first_key`/`offset`/`length` pri binarnoj pretrazi indeksa (§4.3.B).
pub(crate) struct IndexEntry {
    pub(crate) first_key: String,
    pub(crate) offset: u64,
    pub(crate) length: u64,
}

fn serialize_index_block(entries: &[IndexEntry]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for entry in entries {
        let key_bytes = entry.first_key.as_bytes();
        buf.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(key_bytes);
        buf.extend_from_slice(&entry.offset.to_le_bytes());
        buf.extend_from_slice(&entry.length.to_le_bytes());
    }
    let checksum = calculate_checksum_32(&buf);
    buf.extend_from_slice(&checksum.to_le_bytes());
    buf
}

// ---------------------------------------------------------------------
// Footer
// ---------------------------------------------------------------------

struct Footer {
    index_offset: u64,
    index_size: u64,
    filter_offset: u64,
    filter_size: u64,
    num_entries: u64,
    min_seq_no: u64,
    max_seq_no: u64,
}

impl Footer {
    fn serialize(&self) -> [u8; FOOTER_SIZE] {
        let mut body = Vec::with_capacity(FOOTER_SIZE - 4 - 8);
        body.push(SST_FOOTER_VERSION);
        body.extend_from_slice(&[0, 0, 0]); // padding, rezervisano za budućnost
        body.extend_from_slice(&self.index_offset.to_le_bytes());
        body.extend_from_slice(&self.index_size.to_le_bytes());
        body.extend_from_slice(&self.filter_offset.to_le_bytes());
        body.extend_from_slice(&self.filter_size.to_le_bytes());
        body.extend_from_slice(&self.num_entries.to_le_bytes());
        body.extend_from_slice(&self.min_seq_no.to_le_bytes());
        body.extend_from_slice(&self.max_seq_no.to_le_bytes());

        let checksum = calculate_checksum_32(&body);

        let mut out = [0u8; FOOTER_SIZE];
        out[..body.len()].copy_from_slice(&body);
        out[body.len()..body.len() + 4].copy_from_slice(&checksum.to_le_bytes());
        out[body.len() + 4..].copy_from_slice(SST_MAGIC);
        out
    }
}

// ---------------------------------------------------------------------
// Javni ulaz: pisanje SSTabele iz generičkog streama unosa.
// ---------------------------------------------------------------------

/// Jedan generički unos za SSTable writer (§6.5 "merge pipeline ... writes
/// one new SSTable"): i memtable flush (Sekcija 3) i kompakcioni merge
/// (Sekcija 6) hrane isti writer kroz ovaj tip, tako da format/fsync/footer
/// logika postoji na TAČNO jednom mestu.
///
/// Vlasništvo nad `String`-ovima (umesto pozajmljivanja `&str`) je namerna
/// cena: merge iterator (v. `compaction::merge`, sledeći korak) proizvodi
/// tranzijentne, već-spojene vrednosti iz VIŠE ulaznih tabela odjednom, bez
/// jednog zajedničkog izvora za posudbu -- bilo koji `&str`-zasnovan API bi
/// ovde zahtevao nezgodnu lifetime akrobatiku za marginalnu uštedu
/// kopiranja (student-projekat, ne firma -- v. §6.14 "keep it boring").
pub struct SstEntryInput {
    pub key: String,
    pub value: Option<String>,
    pub seq_no: u64,
    pub is_tombstone: bool,
}

/// Zajednička srž pisanja SSTabele (§3.5 raspored na disku + §3.9 crash-safe
/// publish): prima BILO KOJI iterator unosa koji su VEĆ sortirani rastuće
/// po ključu i sadrže NAJVIŠE JEDAN (najnoviji) unos po ključu -- pozivalac
/// garantuje oba svojstva (memtable to radi prirodno preko `BTreeMap`;
/// kompakcioni merge to mora sam da obezbedi, v. `compaction::merge`).
///
/// `expected_key_count_hint` dimenzioniše Bloom filter (§3.5 formule) --
/// za memtable flush ovo je tačan broj (`memtable.len()`); za kompakcioni
/// merge to je gornja granica (zbir ulaznih tabela PRE dedup-a), što je
/// bezbedno: preterana procena samo pravi filter malo veći/tačniji nego što
/// mora biti, nikad ne uvodi false negative.
/// `io_throttle` (Sekcija 6, §6.8): kad je `Some`, uspavljuje nit posle
/// svakog upisanog data bloka da bi se ispoštovao `compaction_io_mb_per_s`
/// budžet (v. `compaction::IoThrottle`). Memtable flush (Sekcija 3) prosleđuje
/// `None` -- throttling se odnosi isključivo na kompakciju (§6.8 naslov
/// "don't kill foreground work", a flush IJESTE foreground rad).
pub fn write_sstable_from_entries<I>(
    sst_dir: &Path,
    id: u64,
    entries: I,
    block_size: u32,
    restart_interval: usize,
    bloom_false_positive_rate: f64,
    build_buffer_bytes: usize,
    expected_key_count_hint: usize,
    io_throttle: Option<&IoThrottle>,
) -> std::io::Result<SsTableManifestEntry>
where
    I: IntoIterator<Item = SstEntryInput>,
{
    fs::create_dir_all(sst_dir)?;

    let file_name = format!("{:06}.sst", id);
    let tmp_file_name = format!("{:06}.sst.tmp", id);
    let tmp_path = sst_dir.join(&tmp_file_name);
    let final_path = sst_dir.join(&file_name);

    let file = File::create(&tmp_path)?;
    let mut writer = BufWriter::with_capacity(build_buffer_bytes.max(4096), file);

    let restart_interval = restart_interval.max(1);
    let block_size = block_size as usize;

    let mut offset: u64 = 0;
    let mut index_entries: Vec<IndexEntry> = Vec::new();
    let mut block_builder = BlockBuilder::new(restart_interval);
    let mut current_block_first_key: Option<String> = None;

    let mut bloom = BloomFilter::new(expected_key_count_hint, bloom_false_positive_rate);

    let mut min_key: Option<String> = None;
    let mut max_key: Option<String> = None;
    let mut min_seq_no = u64::MAX;
    let mut max_seq_no = 0u64;
    let mut num_entries: u64 = 0;

    for entry in entries {
        if current_block_first_key.is_none() {
            current_block_first_key = Some(entry.key.clone());
        }
        if min_key.is_none() {
            min_key = Some(entry.key.clone());
        }
        max_key = Some(entry.key.clone());
        min_seq_no = min_seq_no.min(entry.seq_no);
        max_seq_no = max_seq_no.max(entry.seq_no);
        num_entries += 1;

        bloom.add(entry.key.as_bytes());
        block_builder.add(&entry.key, entry.is_tombstone, entry.seq_no, entry.value.as_deref());

        if block_builder.estimated_size() >= block_size {
            let prev_offset = offset;
            offset = flush_current_block(
                &mut writer,
                &mut block_builder,
                &mut index_entries,
                &mut current_block_first_key,
                offset,
                restart_interval,
            )?;
            if let Some(throttle) = io_throttle {
                throttle.throttle(offset - prev_offset);
            }
        }
    }

    if !block_builder.is_empty() {
        let prev_offset = offset;
        offset = flush_current_block(
            &mut writer,
            &mut block_builder,
            &mut index_entries,
            &mut current_block_first_key,
            offset,
            restart_interval,
        )?;
        if let Some(throttle) = io_throttle {
            throttle.throttle(offset - prev_offset);
        }
    }

    let filter_offset = offset;
    let filter_bytes = bloom.serialize();
    writer.write_all(&filter_bytes)?;
    offset += filter_bytes.len() as u64;
    let filter_size = filter_bytes.len() as u64;

    let index_offset = offset;
    let index_bytes = serialize_index_block(&index_entries);
    writer.write_all(&index_bytes)?;
    let index_size = index_bytes.len() as u64;

    let effective_min_seq_no = if num_entries == 0 { 0 } else { min_seq_no };

    let footer = Footer {
        index_offset,
        index_size,
        filter_offset,
        filter_size,
        num_entries,
        min_seq_no: effective_min_seq_no,
        max_seq_no,
    };
    writer.write_all(&footer.serialize())?;

    // Crash-safety (§3.9): ništa ispod ove linije ne sme da učini tabelu
    // "vidljivom" dok bajtovi zaista nisu trajni na disku.
    writer.flush()?;
    writer.get_ref().sync_all()?;
    drop(writer);

    fs::rename(&tmp_path, &final_path)?;
    fsutil::sync_dir(sst_dir);

    let file_size = fs::metadata(&final_path)?.len();
    let created_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    Ok(SsTableManifestEntry {
        id,
        file_name,
        min_key: min_key.unwrap_or_default(),
        max_key: max_key.unwrap_or_default(),
        min_seq_no: effective_min_seq_no,
        max_seq_no,
        created_at,
        file_size,
        num_entries,
        bloom_params: BloomParams {
            num_bits: bloom.num_bits(),
            num_hashes: bloom.num_hashes(),
            false_positive_rate: bloom_false_positive_rate,
        },
    })
}

/// Upisuje sadržaj `memtable`-a (već je najnovija verzija po ključu, jer
/// tako `Memtable` čuva podatke -- v. spec §3.6) kao novu SSTabelu pod
/// `sst_dir`, imenovanu `{id:06}.sst`. Tanak wrapper oko
/// `write_sstable_from_entries` (v. gore) -- sva writer/footer/fsync
/// logika sada živi tamo, deljena sa kompakcionim output writer-om
/// (Sekcija 6, uskoro).
pub fn flush_memtable_to_sstable(
    sst_dir: &Path,
    id: u64,
    memtable: &Memtable,
    block_size: u32,
    restart_interval: usize,
    bloom_false_positive_rate: f64,
    build_buffer_bytes: usize,
) -> std::io::Result<SsTableManifestEntry> {
    let expected_key_count = memtable.len();
    let entries = memtable.iter().map(|e| SstEntryInput {
        key: e.key.to_string(),
        value: e.value.map(str::to_string),
        seq_no: e.seq_no,
        is_tombstone: e.is_tombstone,
    });

        write_sstable_from_entries(
        sst_dir,
        id,
        entries,
        block_size,
        restart_interval,
        bloom_false_positive_rate,
        build_buffer_bytes,
        expected_key_count,
        None, // flush nije throttlovan -- v. doc na `write_sstable_from_entries`
    )
}

fn flush_current_block(
    writer: &mut BufWriter<File>,
    block_builder: &mut BlockBuilder,
    index_entries: &mut Vec<IndexEntry>,
    current_block_first_key: &mut Option<String>,
    offset: u64,
    restart_interval: usize,
) -> std::io::Result<u64> {
    let finished = std::mem::replace(block_builder, BlockBuilder::new(restart_interval));
    let block_bytes = finished.finish();
    writer.write_all(&block_bytes)?;

    let first_key = current_block_first_key
        .take()
        .expect("neprazan blok mora imati zabeležen prvi ključ");
    index_entries.push(IndexEntry {
        first_key,
        offset,
        length: block_bytes.len() as u64,
    });

    Ok(offset + block_bytes.len() as u64)
}

/// Minimalna, read-only provera zdravlja objavljene SSTabele: seek-uje na
/// mesto gde footer mora biti i validira magic bajtove i checksum. Ovo
/// *nije* pun reader (nema parsiranja index/data blokova); ovo je "quick
/// open check" crash-safety validacija koju spec §3.9 traži, korišćena pri
/// startu da uhvati očigledno pokvarene ili skraćene fajlove na koje
/// manifest referencira.
pub fn quick_validate(path: &Path) -> std::io::Result<()> {
    let mut file = File::open(path)?;
    let file_len = file.metadata()?.len();

    if file_len < FOOTER_SIZE as u64 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "'{}' je manji od validnog footera ({} bajtova)",
                path.display(),
                FOOTER_SIZE
            ),
        ));
    }

    file.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;
    let mut footer_bytes = [0u8; FOOTER_SIZE];
    file.read_exact(&mut footer_bytes)?;

    let magic = &footer_bytes[FOOTER_SIZE - 8..];
    if magic != SST_MAGIC {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("'{}' ima pogrešan footer magic (nije validna SSTabela)", path.display()),
        ));
    }

    let checksum_bytes = &footer_bytes[FOOTER_SIZE - 12..FOOTER_SIZE - 8];
    let expected_checksum = u32::from_le_bytes(checksum_bytes.try_into().unwrap());
    let body = &footer_bytes[..FOOTER_SIZE - 12];
    if calculate_checksum_32(body) != expected_checksum {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("'{}' ima pokvaren footer (checksum mismatch)", path.display()),
        ));
    }

    Ok(())
}

// =======================================================================
// Sekcija 4: reader support — čisto parsiranje formata, bez I/O politike
// (cache, FD budžet, lookup ugovor). To sve živi u `sstable_reader`, koji
// poziva funkcije ispod preko `pub(crate)` granice.
// =======================================================================

/// Strukturisan pogled na parsiran-i-verifikovan SSTable footer.
pub(crate) struct FooterInfo {
    pub(crate) index_offset: u64,
    pub(crate) index_size: u64,
    pub(crate) filter_offset: u64,
    pub(crate) filter_size: u64,
    pub(crate) num_entries: u64,
    pub(crate) min_seq_no: u64,
    pub(crate) max_seq_no: u64,
}

/// Seek-uje na footer na kraju `file`-a, validira magic bajtove, verziju i
/// checksum (spec §4.3.A "quick open check"), i vraća offsete/veličine
/// index i filter blokova. Loguje `file name + offset` na svaku detekciju
/// korupcije (spec §4.3.D) pre nego što vrati grešku.
pub(crate) fn read_footer_at(
    file: &mut File,
    file_len: u64,
    path_for_errors: &Path,
) -> std::io::Result<FooterInfo> {
    if file_len < FOOTER_SIZE as u64 {
        let msg = format!(
            "'{}' je manji od validnog footera ({} bajtova)",
            path_for_errors.display(),
            FOOTER_SIZE
        );
        eprintln!("CorruptionDetected: file='{}' offset=0: {}", path_for_errors.display(), msg);
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, msg));
    }

    let footer_offset = file_len - FOOTER_SIZE as u64;
    file.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;
    let mut footer_bytes = [0u8; FOOTER_SIZE];
    file.read_exact(&mut footer_bytes)?;

    let magic = &footer_bytes[FOOTER_SIZE - 8..];
    if magic != SST_MAGIC {
        let msg = format!("'{}' ima pogrešan footer magic (nije validna SSTabela)", path_for_errors.display());
        eprintln!("CorruptionDetected: file='{}' offset={}: {}", path_for_errors.display(), footer_offset, msg);
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, msg));
    }

    let checksum_bytes = &footer_bytes[FOOTER_SIZE - 12..FOOTER_SIZE - 8];
    let expected_checksum = u32::from_le_bytes(checksum_bytes.try_into().unwrap());
    let body = &footer_bytes[..FOOTER_SIZE - 12];
    if calculate_checksum_32(body) != expected_checksum {
        let msg = format!("'{}' ima pokvaren footer (checksum mismatch)", path_for_errors.display());
        eprintln!("CorruptionDetected: file='{}' offset={}: {}", path_for_errors.display(), footer_offset, msg);
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, msg));
    }

    let version = body[0];
    if version != SST_FOOTER_VERSION {
        let msg = format!(
            "'{}' ima nepodržanu verziju footera ({}, očekivano {})",
            path_for_errors.display(),
            version,
            SST_FOOTER_VERSION
        );
        eprintln!("CorruptionDetected: file='{}' offset={}: {}", path_for_errors.display(), footer_offset, msg);
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, msg));
    }

    // body layout: version(1) + padding(3) + index_offset(8) + index_size(8)
    // + filter_offset(8) + filter_size(8) + num_entries(8) + min_seq_no(8)
    // + max_seq_no(8) -- ogledalo `Footer::serialize` iznad.
    let index_offset = u64::from_le_bytes(body[4..12].try_into().unwrap());
    let index_size = u64::from_le_bytes(body[12..20].try_into().unwrap());
    let filter_offset = u64::from_le_bytes(body[20..28].try_into().unwrap());
    let filter_size = u64::from_le_bytes(body[28..36].try_into().unwrap());
    let num_entries = u64::from_le_bytes(body[36..44].try_into().unwrap());
    let min_seq_no = u64::from_le_bytes(body[44..52].try_into().unwrap());
    let max_seq_no = u64::from_le_bytes(body[52..60].try_into().unwrap());

    Ok(FooterInfo {
        index_offset,
        index_size,
        filter_offset,
        filter_size,
        num_entries,
        min_seq_no,
        max_seq_no,
    })
}

/// Čita `size` bajtova na `offset` iz `file`-a i verifikuje trailing
/// 4-bajtni checksum koji upisuje svaki block builder u ovom modulu
/// (data, filter i index blokovi dele isti trailer format). Vraća telo
/// bloka bez checksum-a. Loguje i vraća `CorruptionDetected`-stil grešku
/// (`io::ErrorKind::InvalidData`) na svaki mismatch ili out-of-bounds
/// pristup (spec §4.3.D).
pub(crate) fn read_verified_block(
    file: &mut File,
    offset: u64,
    size: u64,
    file_len: u64,
    path_for_errors: &Path,
    block_name: &str,
) -> std::io::Result<Vec<u8>> {
    if size < 4 {
        let msg = format!("{} blok (offset {}) je premali ({} bajtova)", block_name, offset, size);
        eprintln!("CorruptionDetected: file='{}' offset={}: {}", path_for_errors.display(), offset, msg);
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, msg));
    }
    if offset.saturating_add(size) > file_len {
        let msg = format!("{} blok (offset {}) prelazi granice fajla", block_name, offset);
        eprintln!("CorruptionDetected: file='{}' offset={}: {}", path_for_errors.display(), offset, msg);
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, msg));
    }

    let mut block_data = vec![0u8; size as usize];
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(&mut block_data)?;

    let body_len = block_data.len() - 4;
    let expected_checksum = u32::from_le_bytes(block_data[body_len..].try_into().unwrap());
    let actual_checksum = calculate_checksum_32(&block_data[..body_len]);
    if actual_checksum != expected_checksum {
        let msg = format!(
            "{} blok (offset {}) CHECKSUM MISMATCH! Očekivano: {}, Dobijeno: {}",
            block_name, offset, expected_checksum, actual_checksum
        );
        eprintln!("CorruptionDetected: file='{}' offset={}: {}", path_for_errors.display(), offset, msg);
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, msg));
    }

    block_data.truncate(body_len);
    Ok(block_data)
}

/// Parsira telo index bloka (već verifikovano preko `read_verified_block`)
/// u sortiranu listu `IndexEntry`, spremnu za binarnu pretragu (§4.3.B).
pub(crate) fn parse_index_block(body: &[u8]) -> std::io::Result<Vec<IndexEntry>> {
    if body.len() < 4 {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "index blok je premali"));
    }
    let num_entries = u32::from_le_bytes(body[0..4].try_into().unwrap()) as usize;
    let mut cursor = 4usize;
    let mut entries = Vec::with_capacity(num_entries);
    for _ in 0..num_entries {
        if cursor + 4 > body.len() {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "index blok prekinut (dužina ključa)"));
        }
        let key_len = u32::from_le_bytes(body[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += 4;
        if cursor + key_len + 16 > body.len() {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "index blok prekinut (nepotpun unos)"));
        }
        let first_key = String::from_utf8_lossy(&body[cursor..cursor + key_len]).into_owned();
        cursor += key_len;
        let offset = u64::from_le_bytes(body[cursor..cursor + 8].try_into().unwrap());
        cursor += 8;
        let length = u64::from_le_bytes(body[cursor..cursor + 8].try_into().unwrap());
        cursor += 8;
        entries.push(IndexEntry { first_key, offset, length });
    }
    Ok(entries)
}

/// Data blok posle skidanja trailing checksum-a: bafer unosa plus tabela
/// restart offseta (oba relativna na `entries_buf`).
pub(crate) struct ParsedDataBlock {
    pub(crate) entries_buf: Vec<u8>,
    pub(crate) restarts: Vec<u32>,
}

pub(crate) fn parse_data_block(body: &[u8]) -> std::io::Result<ParsedDataBlock> {
    if body.len() < 4 {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "data blok je premali"));
    }
    let num_restarts_off = body.len() - 4;
    let num_restarts = u32::from_le_bytes(body[num_restarts_off..].try_into().unwrap()) as usize;
    let restarts_bytes = num_restarts.checked_mul(4).ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "data blok ima nevalidan broj restart tačaka")
    })?;
    if restarts_bytes > num_restarts_off {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "data blok: tabela restart tačaka je prekinuta",
        ));
    }
    let restarts_start = num_restarts_off - restarts_bytes;
    let mut restarts = Vec::with_capacity(num_restarts);
    for i in 0..num_restarts {
        let off = restarts_start + i * 4;
        restarts.push(u32::from_le_bytes(body[off..off + 4].try_into().unwrap()));
    }
    let entries_buf = body[..restarts_start].to_vec();
    Ok(ParsedDataBlock { entries_buf, restarts })
}

/// Jedan dekodiran unos iz data bloka.
#[derive(Debug)]
pub struct DecodedEntry {
    pub key: String,
    pub seq_no: u64,
    pub is_tombstone: bool,
    pub value: Option<String>,
}

/// Dekodira jedan unos iz bafera unosa data bloka na poziciji `offset`,
/// rekonstruišući pun ključ iz `last_key` (pun ključ prethodno dekodiranog
/// unosa — prosledi praznu isečku kad je `offset` restart tačka, jer
/// restart unosi uvek čuvaju ključ u potpunosti, v. `BlockBuilder::add`).
/// Vraća dekodirani unos i offset odmah posle njega.
pub(crate) fn decode_entry_at(
    buf: &[u8],
    offset: usize,
    last_key: &[u8],
) -> std::io::Result<(DecodedEntry, usize)> {
    let bad = || std::io::Error::new(std::io::ErrorKind::InvalidData, "pokvaren unos u data bloku");
    let mut pos = offset;

    let (shared_len, n) = read_varint(buf.get(pos..).ok_or_else(bad)?)?;
    pos += n;
    let (non_shared_len, n) = read_varint(buf.get(pos..).ok_or_else(bad)?)?;
    pos += n;
    let record_type = *buf.get(pos).ok_or_else(bad)?;
    pos += 1;
    let (value_len, n) = read_varint(buf.get(pos..).ok_or_else(bad)?)?;
    pos += n;

    let seq_no = u64::from_le_bytes(buf.get(pos..pos + 8).ok_or_else(bad)?.try_into().unwrap());
    pos += 8;

    let shared_len = shared_len as usize;
    let non_shared_len = non_shared_len as usize;
    let value_len = value_len as usize;

    if shared_len > last_key.len() {
        return Err(bad());
    }

    let key_delta = buf.get(pos..pos + non_shared_len).ok_or_else(bad)?;
    pos += non_shared_len;

    let mut key_bytes = Vec::with_capacity(shared_len + non_shared_len);
    key_bytes.extend_from_slice(&last_key[..shared_len]);
    key_bytes.extend_from_slice(key_delta);
    let key = String::from_utf8_lossy(&key_bytes).into_owned();

    let value_bytes = buf.get(pos..pos + value_len).ok_or_else(bad)?;
    pos += value_len;

    let is_tombstone = record_type == 1;
    let value = if is_tombstone {
        None
    } else {
        Some(String::from_utf8_lossy(value_bytes).into_owned())
    };

    Ok((
        DecodedEntry {
            key,
            seq_no,
            is_tombstone,
            value,
        },
        pos,
    ))
}

pub(crate) fn decode_all_entries(parsed: &ParsedDataBlock) -> std::io::Result<Vec<DecodedEntry>> {
    let mut out = Vec::new();
    let mut last_key: Vec<u8> = Vec::new();
    let mut pos = 0usize;

    while pos < parsed.entries_buf.len() {
        let (entry, next_pos) = decode_entry_at(&parsed.entries_buf, pos, &last_key)?;
        last_key.clear();
        last_key.extend_from_slice(entry.key.as_bytes());
        pos = next_pos;
        out.push(entry);
    }

    Ok(out)
}

// ---------------------------------------------------------------------
// CLI / Dijagnostika: Kompletna verifikacija svih blokova u SSTabeli
// ---------------------------------------------------------------------

/// Dubinska verifikacija celog SSTable fajla.
/// Za razliku od `quick_validate` koji proverava samo footer, ova funkcija
/// parsira footer, pronalazi Index i Filter blokove, a zatim kroz Index 
/// pronalazi sve Data blokove i proverava njihov `calculate_checksum_32`.
pub fn verify(path: &Path) -> Result<String, String> {
    // 1. Prvo radimo brzu validaciju (proverava magic number i checksum footera)
    if let Err(e) = quick_validate(path) {
        return Err(format!("Osnovna validacija footera pala: {}", e));
    }

    let mut file = File::open(path).map_err(|e| format!("Greška pri otvaranju fajla: {}", e))?;
    let file_len = file.metadata().map_err(|e| e.to_string())?.len();

    // 2. Footer je validan, učitavamo ga da bismo izvukli offsete
    file.seek(SeekFrom::End(-(FOOTER_SIZE as i64))).unwrap();
    let mut footer_bytes = [0u8; FOOTER_SIZE];
    file.read_exact(&mut footer_bytes).unwrap();

    // Struktura footera: version(1) + padding(3) + offseti...
    let index_offset = u64::from_le_bytes(footer_bytes[4..12].try_into().unwrap());
    let index_size = u64::from_le_bytes(footer_bytes[12..20].try_into().unwrap());
    let filter_offset = u64::from_le_bytes(footer_bytes[20..28].try_into().unwrap());
    let filter_size = u64::from_le_bytes(footer_bytes[28..36].try_into().unwrap());
    let num_entries = u64::from_le_bytes(footer_bytes[36..44].try_into().unwrap());

    // Helper closure za učitavanje i proveru checksuma bilo kog bloka
    let mut verify_block = |offset: u64, size: u64, block_name: &str| -> Result<Vec<u8>, String> {
        if size < 4 {
            return Err(format!("{} blok (offset {}) je premali ({} bajtova).", block_name, offset, size));
        }
        if offset + size > file_len {
            return Err(format!("{} blok prelazi granice fajla.", block_name));
        }
        
        let mut block_data = vec![0u8; size as usize];
        file.seek(SeekFrom::Start(offset)).map_err(|e| e.to_string())?;
        file.read_exact(&mut block_data).map_err(|e| e.to_string())?;

        let body_len = block_data.len() - 4;
        let body = &block_data[..body_len];
        let expected_checksum = u32::from_le_bytes(block_data[body_len..].try_into().unwrap());
        let actual_checksum = calculate_checksum_32(body);

        if expected_checksum != actual_checksum {
            return Err(format!(
                "KORUPCIJA DETEKTOVANA! {} blok (offset {}) CHECKSUM MISMATCH! Očekivano: {}, Dobijeno: {}", 
                block_name, offset, expected_checksum, actual_checksum
            ));
        }
        
        Ok(body.to_vec()) // Vraćamo telo bloka (bez checksuma) zbog daljeg parsiranja
    };

    // 3. Provera Filter bloka
    verify_block(filter_offset, filter_size, "Filter")?;

    // 4. Provera Index bloka i parsiranje offseta Data blokova
    let index_body = verify_block(index_offset, index_size, "Index")?;
    
    let mut cursor = 0;
    if index_body.len() < 4 {
        return Err("Index blok telo je premalo".into());
    }
    
    // Čitamo broj unosa u index bloku (svaki unos pokazuje na jedan Data blok)
    let num_index_entries = u32::from_le_bytes(index_body[cursor..cursor+4].try_into().unwrap());
    cursor += 4;

    let mut data_blocks = Vec::new();
    for _ in 0..num_index_entries {
        if cursor + 4 > index_body.len() { return Err("Index blok prekinut pre vremena".into()); }
        let key_len = u32::from_le_bytes(index_body[cursor..cursor+4].try_into().unwrap()) as usize;
        cursor += 4;

        if cursor + key_len + 16 > index_body.len() { return Err("Index blok unos nekompletan".into()); }
        cursor += key_len; // Preskačemo `first_key` bajtove, nisu nam bitni za validaciju strukture

        let data_offset = u64::from_le_bytes(index_body[cursor..cursor+8].try_into().unwrap());
        cursor += 8;
        let data_length = u64::from_le_bytes(index_body[cursor..cursor+8].try_into().unwrap());
        cursor += 8;

        data_blocks.push((data_offset, data_length));
    }

    // 5. Provera svih Data blokova
    for (i, (d_offset, d_len)) in data_blocks.iter().enumerate() {
        verify_block(*d_offset, *d_len, &format!("Data blok #{}", i))?;
    }

    // 6. Formiranje izveštaja o uspehu
    Ok(format!(
        "SSTable '{}' je potpuno validan!\n  - Unosa: {}\n  - Filter blok: OK\n  - Index blok: OK\n  - Data blokovi: OK (ukupno provereno: {})",
        path.display(),
        num_entries,
        data_blocks.len()
    ))
}

/// Read-only dijagnostički ispis za jednu SSTabelu (spec §4.7,
/// `lsmkv sst-info`): sadržaj footera, broj index unosa, Bloom filter
/// parametri, i jednostavne statistike veličina data blokova. Parsira
/// fajl direktno preko Sekcije 4 helpera, mimo `TableHandle`/cache
/// mašinerije iz `sstable_reader` (slično kao `verify()`).
pub fn sst_info(path: &Path) -> Result<String, String> {
    let mut file = File::open(path).map_err(|e| format!("Greška pri otvaranju fajla: {}", e))?;
    let file_len = file.metadata().map_err(|e| e.to_string())?.len();

    let footer = read_footer_at(&mut file, file_len, path).map_err(|e| e.to_string())?;

    let filter_body = read_verified_block(&mut file, footer.filter_offset, footer.filter_size, file_len, path, "Filter")
        .map_err(|e| e.to_string())?;
    let bloom = BloomFilter::deserialize(&filter_body).map_err(|e| e.to_string())?;

    let index_body = read_verified_block(&mut file, footer.index_offset, footer.index_size, file_len, path, "Index")
        .map_err(|e| e.to_string())?;
    let index_entries = parse_index_block(&index_body).map_err(|e| e.to_string())?;

    let block_lengths: Vec<u64> = index_entries.iter().map(|e| e.length).collect();
    let (min_block, max_block, avg_block) = if block_lengths.is_empty() {
        (0u64, 0u64, 0.0f64)
    } else {
        let min = *block_lengths.iter().min().unwrap();
        let max = *block_lengths.iter().max().unwrap();
        let avg = block_lengths.iter().sum::<u64>() as f64 / block_lengths.len() as f64;
        (min, max, avg)
    };

    let mut out = format!(
        "--- SSTable Info: {} ---\n\
         File size: {} bytes\n\
         Entries: {}\n\
         SeqNo range: {} .. {}\n\
         --- Footer ---\n\
         Index block: offset={} size={} bytes\n\
         Filter block: offset={} size={} bytes\n\
         --- Bloom filter ---\n\
         Bits: {}\n\
         Hash functions: {}\n\
         --- Index ---\n\
         Data blocks: {}\n\
         Block size (bytes): min={} max={} avg={:.1}\n",
        path.display(),
        file_len,
        footer.num_entries,
        footer.min_seq_no,
        footer.max_seq_no,
        footer.index_offset,
        footer.index_size,
        footer.filter_offset,
        footer.filter_size,
        bloom.num_bits(),
        bloom.num_hashes(),
        index_entries.len(),
        min_block,
        max_block,
        avg_block,
    );

    out.push_str("--- Data block key ranges ---\n");
    for (i, entry) in index_entries.iter().enumerate() {
        out.push_str(&format!(
            "  Block #{:<4} first_key='{}' offset={} length={}\n",
            i, entry.first_key, entry.offset, entry.length
        ));
    }

    Ok(out)
}