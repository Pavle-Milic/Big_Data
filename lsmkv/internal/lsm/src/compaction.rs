//! Sekcija 6: Compaction (size-tiered). Ovaj fajl drži samo knjigovodstvo
//! (`CompactionManager`, `JobStats`) -- čist, testabilan picker i merge
//! žive u podmodulima ispod. Sama orkestracija jednog posla (pick -> merge
//! -> write -> publish -> cleanup) je metoda na `LsmEngine`
//! (`run_compaction_job` u `engine.rs`), pošto joj treba pristup skoro
//! svakom polju engine-a (catalog, sstables, config, manifest_path,
//! event_log...).

pub mod merge;
pub mod picker;

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Per-job statistika (§6.9 "Per-job stats"), čuvana čisto in-memory
/// (dogovoreno: dovoljno za observability/testiranje na kursu, ne mora da
/// preživi restart).
#[derive(Debug, Clone)]
pub struct JobStats {
    pub job_id: u64,
    pub inputs: Vec<u64>,
    pub output: Option<u64>,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub keys_in: u64,
    pub keys_out: u64,
    pub keys_dropped: u64,
    pub tombstones_kept: u64,
    pub tombstones_dropped: u64,
    pub duration_ms: u128,
}

/// Runtime stanje kompakcije, deljeno preko `&LsmEngine` (v. polje
/// `compaction` u `engine.rs`). Sve unutra je `Sync`-bezbedno bez
/// spoljašnje brave.
pub struct CompactionManager {
    next_job_id_counter: AtomicU64,
    running_jobs: AtomicU32,
    paused: AtomicBool,
    /// Skip guard iz §6.4: id-jevi SSTabela trenutno "zauzeti" nekim
    /// kompakcionim poslom u toku, da dva posla nikad ne pokupe isti fajl.
    in_progress_files: Mutex<HashSet<u64>>,
    last_job: Mutex<Option<JobStats>>,
    total_jobs_run: AtomicU64,
    /// Sekcija 6 (§6.9 "Global stats: ... wa_ratio = bytes_out/bytes_in"):
    /// KUMULATIVNI zbir preko SVIH poslova od starta procesa -- ovo je
    /// GLOBALNA metrika, odvojena od bytes_in/bytes_out POJEDINAČNOG
    /// poslednjeg posla (ti su već dostupni preko `last_job_snapshot()`).
    /// Dva odvojena `AtomicU64` umesto jednog `f64` da izbegnemo
    /// read-modify-write trku na float-u; količnik se računa tek pri
    /// čitanju (v. `cumulative_wa_ratio`).
    total_bytes_in: AtomicU64,
    total_bytes_out: AtomicU64,
    /// Unix millis trenutka kad je POSLEDNJI kompakcioni posao završen (0
    /// = nijedan još nije izvršen). Odgovara §6.9 "last_compaction_ms".
    last_completed_at_ms: AtomicU64,
}

impl Default for CompactionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl CompactionManager {
        pub fn new() -> Self {
        Self {
            next_job_id_counter: AtomicU64::new(1),
            running_jobs: AtomicU32::new(0),
            paused: AtomicBool::new(false),
            in_progress_files: Mutex::new(HashSet::new()),
            last_job: Mutex::new(None),
            total_jobs_run: AtomicU64::new(0),
            total_bytes_in: AtomicU64::new(0),
            total_bytes_out: AtomicU64::new(0),
            last_completed_at_ms: AtomicU64::new(0),
        }
    }

    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::SeqCst)
    }

    pub fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
    }

    pub fn resume(&self) {
        self.paused.store(false, Ordering::SeqCst);
    }

    pub fn running_jobs(&self) -> u32 {
        self.running_jobs.load(Ordering::SeqCst)
    }

    /// Pokušava da rezerviše jedan slot pod `compaction_max_concurrent`
    /// (§6.8). CAS petlja umesto load-pa-store da izbegnemo trku između
    /// dva pozivaoca koji istovremeno vide slobodan slot.
    pub fn try_reserve_slot(&self, max_concurrent: u32) -> bool {
        let max_concurrent = max_concurrent.max(1);
        loop {
            let current = self.running_jobs.load(Ordering::SeqCst);
            if current >= max_concurrent {
                return false;
            }
            if self
                .running_jobs
                .compare_exchange(current, current + 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return true;
            }
        }
    }

    pub fn release_slot(&self) {
        self.running_jobs.fetch_sub(1, Ordering::SeqCst);
    }

    /// Rezerviše SVE `ids` atomično -- sve ili ništa -- kao "u toku"
    /// (§6.4 skip guard). Vraća `false` (bez ikakve delimične rezervacije)
    /// ako je BAR JEDAN od njih već zauzet.
    pub fn try_mark_in_progress(&self, ids: &[u64]) -> bool {
        let mut guard = self.in_progress_files.lock().unwrap();
        if ids.iter().any(|id| guard.contains(id)) {
            return false;
        }
        guard.extend(ids.iter().copied());
        true
    }

    pub fn clear_in_progress(&self, ids: &[u64]) {
        let mut guard = self.in_progress_files.lock().unwrap();
        for id in ids {
            guard.remove(id);
        }
    }

    pub fn in_progress_snapshot(&self) -> HashSet<u64> {
        self.in_progress_files.lock().unwrap().clone()
    }

    pub fn next_job_id(&self) -> u64 {
        self.next_job_id_counter.fetch_add(1, Ordering::SeqCst)
    }

    pub fn record_job(&self, stats: JobStats) {
        self.total_jobs_run.fetch_add(1, Ordering::SeqCst);
        self.total_bytes_in.fetch_add(stats.bytes_in, Ordering::SeqCst);
        self.total_bytes_out.fetch_add(stats.bytes_out, Ordering::SeqCst);
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        self.last_completed_at_ms.store(now_ms, Ordering::SeqCst);
        *self.last_job.lock().unwrap() = Some(stats);
    }

    pub fn last_job_snapshot(&self) -> Option<JobStats> {
        self.last_job.lock().unwrap().clone()
    }

    pub fn total_jobs_run(&self) -> u64 {
        self.total_jobs_run.load(Ordering::SeqCst)
    }

    /// §6.9 "wa_ratio = bytes_out/bytes_in", KUMULATIVNO preko svih
    /// poslova od starta procesa. `None` dok nijedan posao još nije
    /// upisao bajtove (izbegava deljenje nulom umesto vraćanja NaN-a).
    pub fn cumulative_wa_ratio(&self) -> Option<f64> {
        let total_in = self.total_bytes_in.load(Ordering::SeqCst);
        if total_in == 0 {
            return None;
        }
        let total_out = self.total_bytes_out.load(Ordering::SeqCst);
        Some(total_out as f64 / total_in as f64)
    }

    /// §6.9 "last_compaction_ms": unix millis trenutka poslednjeg
    /// završenog posla, ili `None` ako nijedan posao još nije izvršen.
    pub fn last_completed_at_ms(&self) -> Option<u64> {
        let v = self.last_completed_at_ms.load(Ordering::SeqCst);
        if v == 0 { None } else { Some(v) }
    }
}

// =======================================================================
// §6.8: throttling & backpressure -- I/O budžet za pisanje izlazne
// SSTabele jednog kompakcionog posla.
// =======================================================================

/// Prost sleep-zasnovan throttle (§6.8 "target IO budget: token bucket or
/// sleep between blocks") za JEDAN kompakcioni posao: prati kumulativne
/// upisane bajtove od trenutka kreiranja, i pred svaki naredni komad
/// (`throttle(bytes)`) uspava nit tačno onoliko koliko je potrebno da
/// prosečna brzina upisa ostane na ili ispod `compaction_io_mb_per_s`.
///
/// Namerna pojednostavljenja (§6.14 "keep it boring"):
/// - throttluje SAMO pisanje izlazne SSTabele (dominantan I/O trošak
///   posla), ne i čitanje ulaznih tabela tokom merge-a -- u trenutnoj
///   implementaciji (`run_compaction_job`) merge se u potpunosti
///   materijalizuje u memoriji PRE nego što upis počne, pa throttlovanje
///   upisa i dalje efektivno ograničava ukupno trajanje posla.
/// - throttle je PO POSLU, ne globalan preko svih paralelnih poslova; sa
///   `compaction_max_concurrent > 1` agregatni I/O više poslova
///   teoretski može premašiti budžet. Prihvatljivo za podrazumevanu
///   vrednost (`compaction_max_concurrent = 1`, §6.11).
/// - `limit_mb_per_sec == 0` je sentinel za "neograničeno" (isti obrazac
///   kao svuda drugde u §6.11), tj. `throttle()` tada nikad ne spava.
pub struct IoThrottle {
    limit_bytes_per_sec: u64,
    start: Instant,
    bytes_so_far: AtomicU64,
}

impl IoThrottle {
    pub fn new(limit_mb_per_sec: u32) -> Self {
        Self {
            limit_bytes_per_sec: (limit_mb_per_sec as u64).saturating_mul(1024 * 1024),
            start: Instant::now(),
            bytes_so_far: AtomicU64::new(0),
        }
    }

    /// Prijavljuje da je upravo upisano `bytes` više, i po potrebi spava
    /// da prosečna brzina od `self.start` ne pređe konfigurisani budžet.
    pub fn throttle(&self, bytes: u64) {
        if self.limit_bytes_per_sec == 0 || bytes == 0 {
            return; // neograničeno
        }
        let total = self.bytes_so_far.fetch_add(bytes, Ordering::SeqCst) + bytes;
        let expected_duration = Duration::from_secs_f64(total as f64 / self.limit_bytes_per_sec as f64);
        let elapsed = self.start.elapsed();
        if expected_duration > elapsed {
            std::thread::sleep(expected_duration - elapsed);
        }
    }
}