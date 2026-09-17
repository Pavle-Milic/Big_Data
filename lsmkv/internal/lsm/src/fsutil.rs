//! Male filesystem pomoćne funkcije koje dele SSTable writer i manifest.
use std::path::Path;

#[cfg(unix)]
use std::fs;

/// Best-effort fsync direktorijuma nakon rename-a. Sam `rename()` je
/// atomičan na POSIX fajl-sistemima, ali bez fsync-a samog direktorijuma
/// postoji (retki) prozor gde bi, u slučaju gubitka napajanja odmah posle
/// rename-a, sam upis rename-a u direktorijum mogao da se izgubi iako je
/// fajl fizički na disku. Ovo je dopuna "atomic rename" garancije iz §3.9.
///
/// Best-effort: na platformama gde otvaranje direktorijuma kao fajla nije
/// podržano (npr. Windows), funkcija tiho ne radi ništa — rename je i
/// dalje atomičan na tim sistemima.
pub(crate) fn sync_dir(dir: &Path) {
    #[cfg(unix)]
    {
        if let Ok(d) = fs::File::open(dir) {
            let _ = d.sync_all();
        }
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
    }
}