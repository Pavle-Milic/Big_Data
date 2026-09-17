use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

const WAL_MAGIC: &[u8; 4] = b"LSMW";
const WAL_VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq)]
pub enum RecordType {
    Put,
    Delete,
}

#[derive(Debug)]
pub struct WalRecord {
    pub record_type: RecordType,
    pub seq_no: u64,
    pub key: String,
    pub value: Option<String>,
}

fn calculate_checksum_32(payload: &[u8]) -> u32 {
    let mut hasher = DefaultHasher::new();
    payload.hash(&mut hasher);
    hasher.finish() as u32 
}

impl WalRecord {
    pub fn new(record_type: RecordType, seq_no: u64, key: String, value: Option<String>) -> Self {
        WalRecord { record_type, seq_no, key, value }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut payload = Vec::new();
        
        payload.push(match self.record_type { RecordType::Put => 0, RecordType::Delete => 1 });
        payload.extend_from_slice(&self.seq_no.to_le_bytes());
        
        let key_bytes = self.key.as_bytes();
        let val_bytes = self.value.as_deref().unwrap_or("").as_bytes();
        
        let key_len = key_bytes.len() as u32;
        let val_len = if self.record_type == RecordType::Delete { 0 } else { val_bytes.len() as u32 };
        
        payload.extend_from_slice(&key_len.to_le_bytes());
        payload.extend_from_slice(&val_len.to_le_bytes());
        
        payload.extend_from_slice(key_bytes);
        if self.record_type == RecordType::Put {
            payload.extend_from_slice(val_bytes);
        }

        let checksum = calculate_checksum_32(&payload);
        
        let payload_len = payload.len() as u32;
        
        let mut final_record = Vec::with_capacity(4 + payload.len() + 4);
        final_record.extend_from_slice(&payload_len.to_le_bytes());
        final_record.extend(payload);
        final_record.extend_from_slice(&checksum.to_le_bytes());
        
        final_record
    }
}

pub struct Wal {
    wal_dir: PathBuf,
    active_file: BufWriter<File>,
    unsynced_writes: u32,
    current_segment_id: u32,
    current_size_bytes: u64,
    roll_bytes: u64,
}

impl Wal {
    pub fn new(data_dir: &str, roll_bytes: u64) -> std::io::Result<(Self, u64, Vec<WalRecord>)> {
        let wal_dir = Path::new(data_dir).join("wal");
        fs::create_dir_all(&wal_dir)?;
        
        let (last_segment_id, max_seq_no, force_new_segment, recovered_records) = Self::recover(&wal_dir)?;
        
        let current_segment_id = if force_new_segment {
            last_segment_id + 1
        } else {
            last_segment_id
        };
        
        let (file, initial_size) = Self::open_segment(&wal_dir, current_segment_id)?;

        Ok((Self {
            wal_dir,
            active_file: BufWriter::new(file),
            unsynced_writes: 0,
            current_segment_id,
            current_size_bytes: initial_size,
            roll_bytes,
        }, max_seq_no, recovered_records))
    }

    fn open_segment(wal_dir: &Path, segment_id: u32) -> std::io::Result<(File, u64)> {
        let wal_path = wal_dir.join(format!("{:06}.wal", segment_id));
        let is_new = !wal_path.exists();
        let mut file = OpenOptions::new().create(true).append(true).open(&wal_path)?;
        let mut size = file.metadata()?.len();

        if is_new {
            let mut header = Vec::with_capacity(8);
            header.extend_from_slice(WAL_MAGIC);
            header.push(WAL_VERSION);
            header.extend_from_slice(&[0, 0, 0]);

            file.write_all(&header)?;
            size = 8;
        }
        Ok((file, size))
    }

    /// Scans every WAL segment, validates checksums, truncates trailing
    /// corruption, and returns:
    /// - the highest segment id seen,
    /// - the highest seqNo seen,
    /// - whether a fresh segment must be started,
    /// - every valid record, in on-disk (= seqNo) order, so the engine can
    ///   replay them back into an in-memory memtable on startup.
    fn recover(wal_dir: &Path) -> std::io::Result<(u32, u64, bool, Vec<WalRecord>)> {
        let mut entries: Vec<_> = fs::read_dir(wal_dir)?
            .filter_map(Result::ok)
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "wal"))
            .collect();
            
        entries.sort_by_key(|e| e.file_name());

        let mut max_seq_no = 0;
        let mut last_segment_id = 0;
        let mut force_new_segment = true; 
        
        let mut total_valid_records = 0;
        let mut total_segments = 0;
        let mut truncations = 0;
        let mut truncation_details = Vec::new();
        let mut recovered_records: Vec<WalRecord> = Vec::new();

        for entry in entries {
            total_segments += 1;
            let path = entry.path();
            let file_name = path.file_name().unwrap().to_string_lossy().to_string();
            
            if let Ok(id) = file_name.replace(".wal", "").parse::<u32>() {
                last_segment_id = last_segment_id.max(id);
                
                let file = OpenOptions::new().read(true).write(true).open(&path)?;
                let mut reader = BufReader::new(&file);
                
                let mut header = [0u8; 8];
                let has_header = reader.read_exact(&mut header).is_ok();

                if !has_header || &header[0..4] != WAL_MAGIC {
                    drop(reader);
                    drop(file);
                    let _ = fs::remove_file(&path); 
                    force_new_segment = true;
                    continue; 
                }

                if header[4] != WAL_VERSION {
                    force_new_segment = true;
                    continue;
                }

                force_new_segment = false;
                
                let mut valid_bytes = 8;
                let mut corrupt = false;

                while !corrupt {
                    let mut len_buf = [0u8; 4];
                    match reader.read(&mut len_buf) {
                        Ok(0) => break, 
                        Ok(4) => {}    
                        Ok(_) | Err(_) => {
                            corrupt = true;
                            break;
                        }
                    }

                    let payload_len = u32::from_le_bytes(len_buf);

                    if payload_len > 10_485_760 { corrupt = true; break; }

                    let mut payload = vec![0u8; payload_len as usize];
                    if reader.read_exact(&mut payload).is_err() { corrupt = true; break; }

                    let mut checksum_buf = [0u8; 4];
                    if reader.read_exact(&mut checksum_buf).is_err() { corrupt = true; break; }
                    let file_checksum = u32::from_le_bytes(checksum_buf);

                    if calculate_checksum_32(&payload) != file_checksum {
                        corrupt = true; break;
                    }

                    // Payload layout (see WalRecord::serialize):
                    // [0]      record type (0 = Put, 1 = Delete)
                    // [1..9]   seq_no (u64 LE)
                    // [9..13]  key_len (u32 LE)
                    // [13..17] val_len (u32 LE, always 0 for Delete)
                    // [17..]   key bytes, then value bytes (Put only)
                    if payload.len() < 17 { corrupt = true; break; }

                    let seq_no_bytes: [u8; 8] = payload[1..9].try_into().unwrap();
                    let seq_no = u64::from_le_bytes(seq_no_bytes);
                    max_seq_no = max_seq_no.max(seq_no);

                    let record_type = if payload[0] == 0 { RecordType::Put } else { RecordType::Delete };
                    let key_len = u32::from_le_bytes(payload[9..13].try_into().unwrap()) as usize;
                    let val_len = u32::from_le_bytes(payload[13..17].try_into().unwrap()) as usize;

                    let key_start = 17;
                    let key_end = key_start + key_len;
                    if payload.len() < key_end { corrupt = true; break; }
                    let key = String::from_utf8_lossy(&payload[key_start..key_end]).into_owned();

                    let value = if record_type == RecordType::Put {
                        let val_start = key_end;
                        let val_end = val_start + val_len;
                        if payload.len() < val_end { corrupt = true; break; }
                        Some(String::from_utf8_lossy(&payload[val_start..val_end]).into_owned())
                    } else {
                        None
                    };

                    recovered_records.push(WalRecord { record_type, seq_no, key, value });

                    total_valid_records += 1;
                    valid_bytes += 4 + payload_len as u64 + 4;
                }
                
                if corrupt {
                    file.set_len(valid_bytes)?;
                    truncations += 1;
                    truncation_details.push(format!("truncated_segment={} truncated_to={}", file_name, valid_bytes));
                }
            }
        }
        
        print!("recovery: segments={} records={} truncated={} last_seqno={} status=OK", 
               total_segments, total_valid_records, truncations, max_seq_no);
               
        if truncations > 0 {
            print!(" [{}]", truncation_details.join(" | "));
        }
        println!();
        
        Ok((last_segment_id, max_seq_no, force_new_segment, recovered_records))
    }

    fn roll_segment(&mut self) -> std::io::Result<()> {
        self.active_file.flush()?;
        self.active_file.get_mut().sync_all()?;
        self.current_segment_id += 1;
        let (new_file, new_size) = Self::open_segment(&self.wal_dir, self.current_segment_id)?;
        self.active_file = BufWriter::new(new_file);
        self.current_size_bytes = new_size;
        self.unsynced_writes = 0;
        Ok(())
    }

    pub fn append(&mut self, record: &WalRecord, sync_every_n: u32) -> std::io::Result<()> {
        if self.current_size_bytes >= self.roll_bytes { 
            self.roll_segment()?; 
        }
        let serialized_bytes = record.serialize();
        
        self.active_file.write_all(&serialized_bytes)?;
        self.current_size_bytes += serialized_bytes.len() as u64;
        self.unsynced_writes += 1;
        
        if self.unsynced_writes >= sync_every_n {
            self.active_file.flush()?;
            self.active_file.get_mut().sync_data()?;
            self.unsynced_writes = 0;
        }
        Ok(())
    }

    pub fn close(&mut self) -> std::io::Result<()> {
        if self.unsynced_writes > 0 {
            self.active_file.flush()?;
            self.active_file.get_mut().sync_all()?;
        }
        Ok(())
    }

    pub fn get_stats(&self) -> (u32, u64, u32) {
        let total_segments = fs::read_dir(&self.wal_dir)
            .map(|res| res.filter_map(Result::ok).count() as u32)
            .unwrap_or(0);
            
        (self.current_segment_id, self.current_size_bytes, total_segments)
    }

    pub fn verify(data_dir: &str) -> std::io::Result<()> {
        let wal_dir = Path::new(data_dir).join("wal");
        if !wal_dir.exists() {
            println!("recovery: segments=0 records=0 truncated=0 last_seqno=0 status=OK [Verify-Only]");
            return Ok(());
        }

        let mut entries: Vec<_> = fs::read_dir(&wal_dir)?
            .filter_map(Result::ok)
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "wal"))
            .collect();
            
        entries.sort_by_key(|e| e.file_name());

        let mut max_seq_no = 0;
        let mut total_valid_records = 0;
        let mut total_segments = 0;
        let mut truncations_needed = 0;
        let mut truncation_details = Vec::new();

        for entry in entries {
            total_segments += 1;
            let path = entry.path();
            let file_name = path.file_name().unwrap().to_string_lossy().to_string();
            
            let file = OpenOptions::new().read(true).open(&path)?;
            let mut reader = BufReader::new(&file);
            
            let mut valid_bytes = 0;
            let mut corrupt = false;
            
            let mut header = [0u8; 8];
            if reader.read_exact(&mut header).is_ok() {
                if &header[0..4] == WAL_MAGIC && header[4] == WAL_VERSION {
                    valid_bytes += 8;
                } else {
                    corrupt = true;
                }
            } else {
                corrupt = true;
            }

            while !corrupt {
                let mut len_buf = [0u8; 4];
                match reader.read(&mut len_buf) {
                    Ok(0) => break, 
                    Ok(4) => {}    
                    Ok(_) | Err(_) => {
                        corrupt = true;
                        break;
                    }
                }

                let payload_len = u32::from_le_bytes(len_buf);
                if payload_len > 10_485_760 { corrupt = true; break; }

                let mut payload = vec![0u8; payload_len as usize];
                if reader.read_exact(&mut payload).is_err() { corrupt = true; break; }

                let mut checksum_buf = [0u8; 4];
                if reader.read_exact(&mut checksum_buf).is_err() { corrupt = true; break; }
                let file_checksum = u32::from_le_bytes(checksum_buf);

                if calculate_checksum_32(&payload) != file_checksum {
                    corrupt = true; break;
                }

                let seq_no_bytes: [u8; 8] = payload[1..9].try_into().unwrap();
                let seq_no = u64::from_le_bytes(seq_no_bytes);
                max_seq_no = max_seq_no.max(seq_no);

                total_valid_records += 1;
                valid_bytes += 4 + payload_len as u64 + 4;
            }
            
            if corrupt {
                truncations_needed += 1;
                truncation_details.push(format!("would_truncate_segment={} at_byte={}", file_name, valid_bytes));
            }
        }
        
        print!("verify: segments={} records={} needs_truncation={} last_seqno={} status=READONLY", 
               total_segments, total_valid_records, truncations_needed, max_seq_no);
               
        if truncations_needed > 0 {
            print!(" [{}]", truncation_details.join(" | "));
        }
        println!();
        
        Ok(())
    }

    pub fn manual_truncate(data_dir: &str, segment: &str, offset: u64) -> std::io::Result<()> {
        let wal_path = Path::new(data_dir).join("wal").join(segment);
        if !wal_path.exists() {
            return Err(std::io::Error::new(std::io::ErrorKind::NotFound, "Segment file not found"));
        }

        let file = OpenOptions::new().write(true).open(&wal_path)?;
        file.set_len(offset)?;
        println!("success: truncated {} to {} bytes", segment, offset);
        Ok(())
    }

    pub fn dump(data_dir: &str, segment: &str) -> std::io::Result<()> {
        let wal_path = std::path::Path::new(data_dir).join("wal").join(segment);
        let file = std::fs::File::open(&wal_path)?;
        let mut reader = std::io::BufReader::new(file);
        
        let mut header = [0u8; 8];
        if std::io::Read::read_exact(&mut reader, &mut header).is_err() || &header[0..4] != b"LSMW" {
            println!("Nije validan WAL fajl!");
            return Ok(());
        }
        println!("=== DUMP FAJLA: {} ===", segment);
        
        while let Ok(len_buf) = { let mut b=[0u8;4]; std::io::Read::read_exact(&mut reader, &mut b).map(|_| b) } {
            let payload_len = u32::from_le_bytes(len_buf);
            let mut payload = vec![0u8; payload_len as usize];
            std::io::Read::read_exact(&mut reader, &mut payload)?;
            let mut chk = [0u8; 4];
            std::io::Read::read_exact(&mut reader, &mut chk)?;
            
            let rec_type = if payload[0] == 0 { "PUT" } else { "DEL" };
            let seq_no = u64::from_le_bytes(payload[1..9].try_into().unwrap());
            let key_len = u32::from_le_bytes(payload[9..13].try_into().unwrap()) as usize;
            let val_len = u32::from_le_bytes(payload[13..17].try_into().unwrap()) as usize;
            
            let key = String::from_utf8_lossy(&payload[17..17+key_len]);
            let val = if rec_type == "PUT" {
                String::from_utf8_lossy(&payload[17+key_len..17+key_len+val_len]).into_owned()
            } else {
                "NONE".to_string()
            };
            
            println!("SeqNo: {:<4} | Tip: {} | Ključ: {:<5} | Vrednost: {}", seq_no, rec_type, key, val);
        }
        println!("=== KRAJ DUMPA ===");
        Ok(())
    }

    /// Briše WAL segmente (osim onog trenutno aktivnog za pisanje) čiji je
    /// sopstveni max seqNo <= `watermark`. Ovo implementira "watermark"
    /// politiku zadržavanja iz spec §3.8: kada su podaci segmenta u
    /// potpunosti pokriveni trajnim SSTabelama, bezbedno ga je ukloniti.
    pub fn delete_segments_below_watermark(&self, watermark: u64) -> std::io::Result<Vec<String>> {
        let mut removed = Vec::new();

        let mut entries: Vec<_> = fs::read_dir(&self.wal_dir)?
            .filter_map(Result::ok)
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "wal"))
            .collect();
        entries.sort_by_key(|e| e.file_name());

        for entry in entries {
            let path = entry.path();
            let file_name = path.file_name().unwrap().to_string_lossy().to_string();

            let id: u32 = match file_name.replace(".wal", "").parse() {
                Ok(v) => v,
                Err(_) => continue,
            };

            // Nikad ne diramo segment u koji se trenutno piše.
            if id == self.current_segment_id {
                continue;
            }

            let segment_max_seq_no = Self::max_seq_no_in_segment(&path)?;
            if segment_max_seq_no <= watermark {
                fs::remove_file(&path)?;
                removed.push(file_name);
            }
        }

        Ok(removed)
    }

    /// Čita segment i vraća najveći seqNo koji sadrži, bez ikakvog
    /// truncate-ovanja (samo read-only skeniranje, za razliku od
    /// `recover()`). Ako segment nije čitljiv/validan, konzervativno se
    /// tretira kao prazan (max seqNo = 0) — takve segmente ionako
    /// `recover()` uklanja pri sledećem startu.
    fn max_seq_no_in_segment(path: &Path) -> std::io::Result<u64> {
        let file = OpenOptions::new().read(true).open(path)?;
        let mut reader = BufReader::new(file);
        let mut max_seq_no: u64 = 0;

        let mut header = [0u8; 8];
        if reader.read_exact(&mut header).is_err() || &header[0..4] != WAL_MAGIC {
            return Ok(0);
        }

        loop {
            let mut len_buf = [0u8; 4];
            match reader.read(&mut len_buf) {
                Ok(4) => {}
                _ => break,
            }
            let payload_len = u32::from_le_bytes(len_buf);
            if payload_len > 10_485_760 || (payload_len as usize) < 17 {
                break;
            }

            let mut payload = vec![0u8; payload_len as usize];
            if reader.read_exact(&mut payload).is_err() {
                break;
            }
            let mut checksum_buf = [0u8; 4];
            if reader.read_exact(&mut checksum_buf).is_err() {
                break;
            }
            let file_checksum = u32::from_le_bytes(checksum_buf);
            if calculate_checksum_32(&payload) != file_checksum {
                break;
            }

            let seq_no_bytes: [u8; 8] = payload[1..9].try_into().unwrap();
            let seq_no = u64::from_le_bytes(seq_no_bytes);
            max_seq_no = max_seq_no.max(seq_no);
        }

        Ok(max_seq_no)
    }
}