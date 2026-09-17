//! Size-tiered picker (§6.4): deterministički bira sledeći set SSTabela za
//! kompakciju iz trenutnog manifesta.

use crate::manifest::SsTableManifestEntry;
use std::collections::HashSet;

pub struct PickerConfig {
    pub fan_in: usize,
    pub size_ratio: f64,
}

/// Bira `fan_in` SSTabela za kompakciju po pravilima iz §6.4:
///
/// 1. Sortiraj tabele rastuće po veličini; kod izjednačenja, novije prvo
///    (viši `id` == novije, pošto `next_sst_id` samo raste -- i za flush i
///    za kompakcioni izlaz).
/// 2. Skeniraj od najmanje: prvi prozor od `fan_in` UZASTOPNIH tabela (u
///    ovom sortiranom redosledu) čije su veličine sve unutar
///    `size_ratio` najmanje u tom prozoru, pobeđuje.
/// 3. Ako ništa ne odgovara, uzmi `fan_in` najmanjih preostalih (i dalje
///    deterministički, isti sort kao gore).
///
/// Tabele čiji je `id` u `in_progress` (već deo neke druge kompakcije u
/// toku, §6.4 "skip guard") se potpuno izuzimaju iz razmatranja.
///
/// Vraća `None` ako ima manje od `fan_in` podobnih tabela.
pub fn pick(
    tables: &[SsTableManifestEntry],
    in_progress: &HashSet<u64>,
    cfg: &PickerConfig,
) -> Option<Vec<u64>> {
    let fan_in = cfg.fan_in.max(2); // merge od 1 fajla nema smisla

    let mut candidates: Vec<&SsTableManifestEntry> = tables
        .iter()
        .filter(|t| !in_progress.contains(&t.id))
        .collect();

    if candidates.len() < fan_in {
        return None;
    }

    candidates.sort_by(|a, b| a.file_size.cmp(&b.file_size).then_with(|| b.id.cmp(&a.id)));

    for window in candidates.windows(fan_in) {
        let smallest = window[0].file_size.max(1);
        let largest = window.iter().map(|t| t.file_size).max().unwrap_or(0).max(1);
        if (largest as f64) <= (smallest as f64) * cfg.size_ratio {
            return Some(window.iter().map(|t| t.id).collect());
        }
    }

    Some(candidates[..fan_in].iter().map(|t| t.id).collect())
}