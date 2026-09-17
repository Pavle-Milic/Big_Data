//! Append-only, human-readable dnevnik životnog ciklusa engine-a
//! (priprema za Sekciju 6): svaki bitan događaj — inicijalizacija engine-a,
//! rotacija memtabele u immutable, flush immutable-a u SSTabelu, i (uskoro)
//! svaki kompakcioni posao — upisuje po jedan blok u ovaj fajl.
//!
//! Ovo NIJE izvor istine za state (to su i dalje WAL i Manifest); ovo je
//! isključivo observability/debug trag za operatera (i za nas dok
//! razvijamo/testiramo kompakciju). Zbog toga je best-effort: greška pri
//! pisanju event log-a se samo prijavljuje na stderr i NIKAD ne obara
//! operaciju koja ga je pozvala (npr. `put`/rotacija/flush moraju uspeti
//! čak i ako baš ovaj fajl ne može da se upiše).

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct EventLog {
    path: String,
    file: Mutex<std::fs::File>,
}

impl EventLog {
    /// Otvara (ili kreira) append-only log fajl na `path`. Roditeljski
    /// direktorijum se kreira po potrebi (isti obrazac kao `sst_dir`).
    pub fn open(path: &str) -> std::io::Result<Self> {
        if let Some(parent) = Path::new(path).parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self { path: path.to_string(), file: Mutex::new(file) })
    }

    fn now_ms() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0)
    }

    /// Upisuje jedan događaj kao čitljiv blok:
    ///
    ///   ====== <TITLE> ======
    ///   timestamp_ms: <...>
    ///   <key>: <value>
    ///   ...
    ///   (prazan red)
    ///
    /// `lines` su već formatirani (ključ, vrednost) parovi — pozivalac
    /// bira šta je bitno da se zabeleži za dati tip događaja.
    pub fn record(&self, title: &str, lines: &[(&str, String)]) {
        let mut body = format!("====== {title} ======\ntimestamp_ms: {}\n", Self::now_ms());
        for (key, value) in lines {
            body.push_str(&format!("{key}: {value}\n"));
        }
        body.push('\n');
        self.write_body(title, &body);
    }

    /// Isti kao `record`, ali dodaje i multi-line blok (npr. serijalizovan
    /// JSON config) indentovan radi čitljivosti.
    pub fn record_with_block(&self, title: &str, lines: &[(&str, String)], block_label: &str, block: &str) {
        let mut body = format!("====== {title} ======\ntimestamp_ms: {}\n", Self::now_ms());
        for (key, value) in lines {
            body.push_str(&format!("{key}: {value}\n"));
        }
        body.push_str(&format!("{block_label}:\n"));
        for line in block.lines() {
            body.push_str("  ");
            body.push_str(line);
            body.push('\n');
        }
        body.push('\n');
        self.write_body(title, &body);
    }

    fn write_body(&self, title: &str, body: &str) {
        let mut file = match self.file.lock() {
            Ok(f) => f,
            Err(e) => {
                eprintln!("eventlog: mutex poisoned, event '{title}' izgubljen: {e}");
                return;
            }
        };
        if let Err(e) = file.write_all(body.as_bytes()) {
            eprintln!("eventlog: upis događaja '{title}' u '{}' nije uspeo: {e}", self.path);
            return;
        }
        let _ = file.flush();
    }
}