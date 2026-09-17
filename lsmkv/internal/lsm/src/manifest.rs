//! Manifest (§3.7, prošireno u §5.3): jedini izvor istine o tome koje su
//! SSTabele trenutno žive, plus (od Sekcije 5) format-verzija i epoch
//! brojač koji `Version` (v. `version.rs`) kopira pri svakom flush-u.
//! Dovoljno je mali da se pri svakom update-u prepiše u celosti, umesto da
//! se vodi delta-log (§5.3 "Student-friendly path: start with single
//! JSON" -- opciju log+checkpoint namerno preskačemo, pa
//! `manifest_checkpoint_every_edits` iz §5.9 ovde ne postoji).

use crate::fsutil;
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Bloom filter parametri zapisani po SSTabeli, da bi budući čitalac mogao
/// da rekonstruiše/proveri filter bez ponovnog izvođenja iz trenutnog
/// (možda već izmenjenog) `bloom_false_positive` config-a.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BloomParams {
    pub num_bits: u64,
    pub num_hashes: u32,
    pub false_positive_rate: f64,
}

/// Jedan red manifesta: sve što engine treba da zna o jednoj objavljenoj
/// SSTabeli bez otvaranja samog fajla.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SsTableManifestEntry {
    pub id: u64,
    pub file_name: String,
    pub min_key: String,
    pub max_key: String,
    pub min_seq_no: u64,
    pub max_seq_no: u64,
    pub created_at: u64,
    pub file_size: u64,
    pub num_entries: u64,
    pub bloom_params: BloomParams,
}

/// §5.3 file-level polja: `manifest_version` (format Manifesta na disku,
/// nezavisan od runtime brojača) i `epoch` (monotono raste pri svakom
/// flush-u koji dotiče disk; runtime `Version.epoch` -- v. `version.rs`
/// -- može biti ISPRED ove vrednosti zahvaljujući čisto-memorijskim
/// rotacijama, i "sustiže" je pri sledećem flush-u, v.
/// `LsmEngine::flush_immutable`).
///
/// `tables` je NAJNOVIJA-PRVA (§5.3 "order matters: newest first", §5.11
/// "Pick one and stick to it everywhere" -- ovde je izbor newest->oldest):
/// novi unosi se ubacuju na `tables.insert(0, ...)` u `flush_immutable`,
/// nikad na kraj.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Manifest {
    pub manifest_version: u32,
    pub epoch: u64,
    pub next_sst_id: u64,
    pub tables: Vec<SsTableManifestEntry>,
    /// Najveći seqNo koji je trajno sadržan u nekoj flush-ovanoj SSTabeli.
    /// Koristi ga watermark politika za brisanje WAL segmenata (§3.8).
    pub flushed_seq_no_watermark: u64,
}

impl Default for Manifest {
    fn default() -> Self {
        Self {
            manifest_version: 1,
            epoch: 0,
            next_sst_id: 0,
            tables: Vec::new(),
            flushed_seq_no_watermark: 0,
        }
    }
}

impl Manifest {
    /// Putanja za privremeni fajl korišćen u atomičnom write-u (§5.3
    /// "Write changes to manifest.tmp, fsync, then atomic rename"):
    /// `{file_name}.tmp` u istom direktorijumu kao finalni fajl, tako da
    /// `rename` ostane na istom fajl-sistemu (preduslov za atomičnost).
    pub fn tmp_path(final_path: &Path) -> PathBuf {
        let mut name: OsString = final_path
            .file_name()
            .map(|n| n.to_os_string())
            .unwrap_or_else(|| OsString::from("manifest.json"));
        name.push(".tmp");

        match final_path.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => parent.join(name),
            _ => PathBuf::from(name),
        }
    }

    /// Učitava manifest sa `manifest_path` (§5.9 `manifest_path` config
    /// knob). Fajl koji ne postoji se tretira kao "još nema SSTabela"
    /// (očekivano na prvom pokretanju, `epoch` kreće od 0). Fajl koji
    /// postoji ali ne može da se parsira se tretira kao korupcija i vraća
    /// se kao hard error -- tiho vraćanje na prazan manifest bi značilo da
    /// engine "zaboravi" žive SSTabele.
    ///
    /// Napomena (§5.3 "if both manifest.json and manifest.tmp exist,
    /// ignore .tmp"): ova funkcija čita ISKLJUČIVO `manifest_path`, nikad
    /// `.tmp` putanju, pa je to pravilo ovde automatski ispoštovano; poziv
    /// koji brine o BRISANJU zaostalog `.tmp` fajla (best-effort crash
    /// cleanup) je odgovornost pozivaoca (v. `LsmEngine::new`).
    ///
    /// FIX (konzistentnost poruka o korupciji, §5.10 "clear
    /// CorruptionDetected message"): ostatak projekta (sstable.rs,
    /// sstable_reader.rs) dosledno prefiksuje greške korupcije sa
    /// "CorruptionDetected: ..." i loguje ih preko `eprintln!` pre
    /// vraćanja `Err`-a. Ranije je ovde nedostajao i prefiks i log-linija.
    pub fn load(manifest_path: &str) -> std::io::Result<Self> {
        let path = Path::new(manifest_path);
        match fs::read_to_string(path) {
            Ok(contents) => serde_json::from_str(&contents).map_err(|e| {
                let msg = format!(
                    "CorruptionDetected: manifest '{}' nije validan JSON: {e}",
                    path.display()
                );
                eprintln!("{msg}");
                std::io::Error::new(std::io::ErrorKind::InvalidData, msg)
            }),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(e),
        }
    }

    /// Atomično upisuje manifest na `manifest_path`: piše u temp fajl,
    /// fsync-uje ga, pa rename-uje preko starog manifesta, pa (best-effort)
    /// fsync-uje roditeljski direktorijum (§5.3 atomicity rules; isti
    /// obrazac kao `sstable::flush_memtable_to_sstable`).
    pub fn save(&self, manifest_path: &str) -> std::io::Result<()> {
        let final_path = Path::new(manifest_path);
        if let Some(parent) = final_path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }

        let tmp_path = Self::tmp_path(final_path);

        let json_text = serde_json::to_string_pretty(self)
            .expect("Manifest should always serialize to JSON");

        {
            let mut file = fs::File::create(&tmp_path)?;
            file.write_all(json_text.as_bytes())?;
            file.sync_all()?;
        }

        fs::rename(&tmp_path, final_path)?;

        if let Some(parent) = final_path.parent() {
            if !parent.as_os_str().is_empty() {
                fsutil::sync_dir(parent);
            }
        }

        Ok(())
    }
}