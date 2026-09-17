main.rs — CLI + TCP server. init pokreće server (bind na 127.0.0.1:7878), sve ostale komande su klijenti koji šalju JSON Request i čitaju JSON Response preko jedne linije (newline-delimited JSON protokol).

lib.rs — Config (sve podesive vrednosti) i Request/Response enumi. Config se čita iz config/<name>.json, a "aktivna" konfiguracija se čuva u config/.active.json.

engine.rs — srce sistema, LsmEngine. Drži aktivni memtable, red immutable memtable-ova, listu otvorenih SSTable handle-ova, manifest (katalog), trenutnu Version (immutable snapshot za čitanje), i dva pozadinska radnika (flush, compaction).

wal.rs — write-ahead log, segmentiran, sa checksum-ovanim rekordima i replay/truncate logikom pri startu.

memtable.rs — in-memory BTreeMap<String, MemEntry>, prati približnu veličinu u bajtovima.

sstable.rs — pisanje SSTable fajlova (block-based format, prefix-compressed keys, bloom filter, index blok, footer sa checksumom) + verifikacija/inspekcija.

sstable_reader.rs — TableHandle (otvorena SSTable za čitanje), BlockCache (LRU keš blokova), FileHandleCache (LRU keš file descriptor-a), i search_sstables — read path kroz L0 tabele.

manifest.rs — perzistentni katalog SSTable-ova (atomic write: tmp fajl + rename + fsync dir-a).

version.rs — immutable snapshot (active + immutables + sstables) koji se "publikuje" pri svakoj promeni stanja — ovo je MVCC-ish mehanizam da čitanja ne vide inkonzistentno stanje.

compaction.rs + merge.rs + picker.rs — size-tiered kompakcija: picker bira kandidate, MergeIterator radi k-way merge po najnovijem seq_no, engine piše izlaznu SSTable i ažurira manifest.

eventlog.rs — append-only human-readable log svih bitnih događaja (INIT, ROTATE, FLUSH, COMPACTION, SHUTDOWN...).


# LSM-Tree Key-Value Store

This is a minimal Log-Structured Merge-tree (LSM-tree) key-value engine written in Rust. Currently, it implements a robust Write-Ahead Log (WAL) to ensure data durability across process crashes and power losses.

## WAL Architecture & Policies
**Sync Policy:** The engine synchronizes data to disk based on the `wal_fsync_every_n` configuration parameter; if set to 1, it fsyncs after every record (safest), but if set to $N > 1$, it batches writes and fsyncs only every $N$th record for higher throughput (meaning the last $N-1$ records may be lost in a hard crash).
**Roll Policy:** The active WAL segment rolls over into a new file automatically once it exceeds the byte limit defined by `wal_segment_roll_bytes` (default ~128MB). 

### File Layout & Corruption Handling
- **Header**: Every segment starts with an 8-byte header (Magic bytes + Version).
- **Records**: Encoded as binary payloads `[Length (4B) | Type (1B) | SeqNo (8B) | KeyLen (4B) | ValLen (4B) | Key | Value | Checksum (4B)]`.
- **Truncation**: On startup, the engine verifies the 32-bit checksum of every record. If tail corruption or incomplete writes are detected (e.g., due to power loss or random garbage bytes), the engine stops reading the corrupted file, truncates it to the last valid byte offset, and safely resumes operations.

## Backpressure policy: Option B. 
Kada bi rotacija aktivnog memtable-a napravila (N+1)-vi immutable (preko max_immutable_tables, default 4), engine prvo jednokratno podigne prag rotacije za 1.25x umesto da odmah blokira pisanje. Ako se prag ponovo dostigne dok je immutable red i dalje pun, dalji put/del zahtevi se odbijaju sa Backpressure greškom dok se ne oslobodi mesto (Sekcija 3 — flush na disk).

## CLI Usage

Start the server (creates data/wal directories and reads config):
```bash
cargo run -- init

## Client commands (run in a separate terminal while init is running)

cargo run -- put --key <K> --value <V>
cargo run -- del --key <K>
cargo run -- get --key <K>
cargo run -- stats
cargo run -- close
cargo run -- memtable-dump
cargo run -- wal-verify
cargo run -- wal-truncate --segment 000001.wal --offset 1024