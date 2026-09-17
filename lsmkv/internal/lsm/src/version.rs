//! Version (Sekcija 5): imutabilan in-memory snapshot koji čitaoci
//! koriste da vide konzistentnu sliku baze dok pozadinski zadaci
//! (rotacija, flush) menjaju stanje (§5.1/§5.4).
//!
//! Pre ove sekcije, `LsmEngine::lookup_key_traced` je čitao `immutables` i
//! `sstables` iz DVE odvojene brave, jednu za drugom -- što je ostavljalo
//! prozor u kom bi flush mogao da ukloni immutable memtabelu (jer su njeni
//! podaci sada trajno u novoj SSTabeli) BAŠ između ta dva čitanja, pre nego
//! što je čitalac stigao da vidi novu tabelu (§5.11 "Inconsistent order").
//! `Version` to rešava tako što `immutables` i `sstables` uvek putuju
//! ZAJEDNO, upakovani u jedan `Arc`, objavljen jednim atomičnim swap-om
//! pokazivača (v. `LsmEngine::build_and_swap_version`/`publish_version`,
//! oba serijalizovana preko `LsmEngine::publish_lock` da ni rotacija ni
//! flush ne mogu da objave verzije van redosleda epoha).
//!
//! `active` -- VAŽNA NAPOMENA O ZASTAREVANJU: u trenutku publish-a,
//! `version.active` je isti `Arc<Memtable>` kao `LsmEngine::active`. ALI
//! čim se desi sledeći PUT/DELETE, write-path (`engine::write_op`) zove
//! `Arc::make_mut(&mut active_guard)`, koji -- pošto refcount na taj
//! trenutak nije 1 (objavljeni `Version` i dalje drži svoju referencu) --
//! interno KLONIRA memtabelu u nov `Arc` i taj novi `Arc` postaje
//! `LsmEngine::active`. Stari, objavljeni `version.active` ostaje da
//! pokazuje na ZAMRZNUT sadržaj (tipično prazan ili sa manje unosa) i
//! VIŠE NE PRATI dalje pisanje sve do sledećeg publish-a. Zbog ovoga
//! `LsmEngine::get_stats_string`/`version_info_string` NIKAD ne smeju da
//! čitaju "trenutnu veličinu aktivne memtabele" iz `version.active` --
//! moraju sveže pročitati `LsmEngine::active` (isto što i `get` radi).
//! Publish se i dalje намерно okida samo na ROTACIJU (§5.7 "Section 2
//! (memtables)"), ne na svaki PUT -- pun snapshot+swap po ključu bio bi
//! neopravdano skup i suprotan duhu spec-a.
//!
//! Refcounting (§5.5 "Refcount... SSTable handles can also have
//! refcounts") dobijamo besplatno kroz `Arc`: dokle god neki čitalac drži
//! `Arc<Version>`, sve `Arc<TableHandle>` i `Arc<Memtable>` unutra ostaju
//! žive čak i ako ih neka buduća kompakcija ukloni iz `LsmEngine`-ovog
//! "trenutnog" stanja -- nema potrebe za ručnim brojačem.

use crate::memtable::Memtable;
use crate::sstable_reader::TableHandle;
use std::collections::VecDeque;
use std::sync::Arc;

/// Immutable snapshot objavljen za čitaoce (§5.4). Jednom konstruisan,
/// nijedno polje se više ne menja -- svaka strukturna promena (rotacija,
/// flush, startup) pravi NOV `Version` i zamenjuje pokazivač u
/// `LsmEngine::current_version` (§5.5 publish protokol).
pub struct Version {
    /// Monotono rastući identifikator za debug (§5.2 "Epoch/VersionId").
    /// Kopira se iz `Manifest::epoch` pri flush-u koji dotiče disk;
    /// čisto-memorijske rotacije ga i dalje inkrementiraju preko
    /// `LsmEngine::next_epoch`, pa runtime epoch može privremeno biti
    /// ispred onoga što je poslednje upisano na disk (sustiže ga na
    /// sledećem flush-u).
    pub epoch: u64,

    /// Pokazivač na aktivnu memtabelu u trenutku publish-a (§5.4). Vidi
    /// modulski komentar iznad: ovaj sadržaj se "zamrzava" čim se desi
    /// prvi sledeći write -- za TRENUTNO stanje aktivne memtabele uvek
    /// čitati `LsmEngine::active` direktno, ne ovo polje.
    pub active: Arc<Memtable>,

    /// Immutable memtabele, NAJNOVIJA PRVA (§5.4). `LsmEngine::immutables`
    /// interno čuva oldest-u-front (radi jednostavnog `pop_front` pri
    /// flush-u najstarije), pa se ovde okreće JEDNOM, pri gradnji verzije,
    /// da čitaoci ne moraju da rade `.rev()` na svakom pojedinačnom `get`.
    pub immutables: Vec<Arc<Memtable>>,

    /// Otvorene SSTabele, NAJNOVIJA PRVA (§5.3/§5.4: isti redosled kao
    /// `Manifest::tables`, koji je od Sekcije 5 takođe newest-first -- v.
    /// komentar na `LsmEngine::sstables`). Za razliku od `immutables`, ovo
    /// se NE okreće ovde jer je izvor (`LsmEngine::sstables`) već u tom
    /// redosledu.
    pub sstables: Vec<Arc<TableHandle>>,
}

impl Version {
    /// Gradi novi `Version` iz trenutnog (živog) stanja engine-a.
    /// `immutables_oldest_front` je engine-ov interni `VecDeque`
    /// (oldest-front/newest-back); `sstables_newest_first` je engine-ov
    /// interni `Vec` koji je VEĆ newest-first, pa se prosleđuje bez
    /// izmene redosleda.
    pub fn from_parts(
        epoch: u64,
        active: Arc<Memtable>,
        immutables_oldest_front: &VecDeque<Arc<Memtable>>,
        sstables_newest_first: &[Arc<TableHandle>],
    ) -> Self {
        Self {
            epoch,
            active,
            immutables: immutables_oldest_front.iter().rev().cloned().collect(),
            sstables: sstables_newest_first.to_vec(),
        }
    }

    // --- Derived stats (§5.4 "Derived stats (counts/sizes) for stats output") ---

    pub fn immutable_count(&self) -> usize {
        self.immutables.len()
    }

    pub fn immutable_entries(&self) -> usize {
        self.immutables.iter().map(|m| m.len()).sum()
    }

    pub fn immutable_bytes(&self) -> u64 {
        self.immutables.iter().map(|m| m.approx_size_bytes()).sum()
    }

    pub fn sstable_count(&self) -> usize {
        self.sstables.len()
    }

    pub fn sstable_total_entries(&self) -> u64 {
        self.sstables.iter().map(|t| t.num_entries()).sum()
    }

    pub fn sstable_total_bytes(&self) -> u64 {
        self.sstables.iter().map(|t| t.file_size()).sum()
    }
}