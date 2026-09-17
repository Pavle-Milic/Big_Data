use std::collections::BTreeMap;

#[derive(Debug, Clone)]
struct MemEntry {
    value: Option<String>,
    #[allow(dead_code)]
    seq_no: u64,
    is_tombstone: bool,
}

impl MemEntry {
    fn put(value: String, seq_no: u64) -> Self {
        Self { value: Some(value), seq_no, is_tombstone: false }
    }

    fn tombstone(seq_no: u64) -> Self {
        Self { value: None, seq_no, is_tombstone: true }
    }

    fn approx_size(&self, key: &str, overhead_bytes: usize) -> usize {
        let value_len = self.value.as_deref().map(str::len).unwrap_or(0);
        key.len() + value_len + overhead_bytes
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Lookup {
    Found(String),
    Tombstone,
    NotFound,
}

#[derive(Debug, Clone)]
pub struct MemtableOpResult {
    pub total_bytes: u64,
    pub bytes_added: usize,
    pub bytes_removed: usize,
    pub contents: String,
}

/// Read-only pogled na jedan unos memtabele, koristi ga SSTable flush path
/// (v. `sstable::flush_memtable_to_sstable`) da iterira sve unose u
/// sortiranom redosledu ključeva bez izlaganja internog `MemEntry`.
pub struct MemtableEntry<'a> {
    pub key: &'a str,
    pub value: Option<&'a str>,
    pub seq_no: u64,
    pub is_tombstone: bool,
}

#[derive(Debug, Clone)]
pub struct Memtable {
    entries: BTreeMap<String, MemEntry>,
    approx_size_bytes: u64,
    overhead_per_entry: usize,
}

impl Memtable {
    pub fn new(overhead_per_entry: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            approx_size_bytes: 0,
            overhead_per_entry,
        }
    }

    fn insert(&mut self, key: String, entry: MemEntry) -> MemtableOpResult {
        let new_size = entry.approx_size(&key, self.overhead_per_entry);
        let mut old_size = 0;

        if let Some(old) = self.entries.get(&key) {
            old_size = old.approx_size(&key, self.overhead_per_entry);
            self.approx_size_bytes = self.approx_size_bytes.saturating_sub(old_size as u64);
        }

        self.approx_size_bytes += new_size as u64;
        self.entries.insert(key, entry);

        let contents = self.format_entries();

        MemtableOpResult {
            total_bytes: self.approx_size_bytes,
            bytes_added: new_size,
            bytes_removed: old_size,
            contents,
        }
    }

    pub fn put(&mut self, key: String, value: String, seq_no: u64) -> MemtableOpResult {
        self.insert(key, MemEntry::put(value, seq_no))
    }

    pub fn delete(&mut self, key: String, seq_no: u64) -> MemtableOpResult {
        self.insert(key, MemEntry::tombstone(seq_no))
    }

    pub fn get(&self, key: &str) -> Lookup {
        match self.entries.get(key) {
            Some(entry) if entry.is_tombstone => Lookup::Tombstone,
            Some(entry) => Lookup::Found(entry.value.clone().unwrap_or_default()),
            None => Lookup::NotFound,
        }
    }

    /// Iterira sve unose u rastućem redosledu ključeva. Pošto `insert`
    /// uvek prepiše prethodni unos za dati ključ, ovo prirodno vraća samo
    /// najnoviju verziju (najveći seqNo) po ključu, a tombstone-ovi se
    /// vraćaju kao i svaki drugi unos — v. §3.6.
    pub fn iter(&self) -> impl Iterator<Item = MemtableEntry<'_>> {
        self.entries.iter().map(|(key, entry)| MemtableEntry {
            key: key.as_str(),
            value: entry.value.as_deref(),
            seq_no: entry.seq_no,
            is_tombstone: entry.is_tombstone,
        })
    }

    pub fn dump_contents(&self, label: &str) -> String {
        let mut out = format!("--- {} (Size: {} B, Entries: {}) ---\n", label, self.approx_size_bytes, self.entries.len());
        if self.entries.is_empty() {
            out.push_str("  (empty)\n");
            return out;
        }
        out.push_str(&self.format_entries());
        out
    }

    fn format_entries(&self) -> String {
        let mut contents = String::new();
        for (k, v) in &self.entries {
            let entry_size = v.approx_size(k, self.overhead_per_entry);
            if v.is_tombstone {
                contents.push_str(&format!(
                    "  • Key: '{}' | [TOMBSTONE] | Size: {} B | Seq: {}\n",
                    k, entry_size, v.seq_no
                ));
            } else {
                let val = v.value.as_deref().unwrap_or("");
                contents.push_str(&format!(
                    "  • Key: '{}' | Value: '{}' | Size: {} B | Seq: {}\n",
                    k, val, entry_size, v.seq_no
                ));
            }
        }
        contents
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn approx_size_bytes(&self) -> u64 {
        self.approx_size_bytes
    }
}