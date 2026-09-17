//! K-way merge preko više SSTable sken-iteratora (§6.5 "merge pipeline
//! ... streams multiple SSTables (sorted by key) and writes one new
//! SSTable with only the latest visible value per key").
//!
//! Generički je nad `I: Iterator<Item = io::Result<DecodedEntry>>` (a ne
//! hardkodiran na `SsTableScanIterator`) da bi merge logika mogla da se
//! testira izolovano, sa prostim in-memory iteratorima, bez ikakvog
//! stvarnog fajla na disku.

use crate::sstable::{DecodedEntry, SstEntryInput};
use std::io;
use std::iter::Peekable;

/// Spaja `sources` (svaki već sortiran rastuće po ključu -- garancija koju
/// obezbeđuje `SsTableScanIterator`) u JEDAN sortiran stream, gde za svaki
/// ključ preživljava tačno jedan unos: onaj sa najvećim `seq_no` među svim
/// izvorima koji taj ključ sadrže (§6.5 "Keep only the entry with the
/// highest seqNo across inputs").
///
/// Tombstone politika (§6.6, MVP): pobednički unos se UVEK propušta u
/// izlaz, čak i kad je tombstone -- "never drop tombstones yet" opcija
/// koju spec eksplicitno dozvoljava za kurs. Mesto za buduće
/// grace-period+coverage pravilo bi bilo unutar `next()`, tačno tamo gde
/// je sada `return Some(Ok(...))` -- umesto da uvek vrati, proverio bi
/// uslov za bezbedno brisanje i, ako je ispunjen, umesto `return` uradio
/// `continue` na spoljnu petlju da preskoči ovaj ključ bez emitovanja.
pub struct MergeIterator<I>
where
    I: Iterator<Item = io::Result<DecodedEntry>>,
{
    sources: Vec<Peekable<I>>,
    errored: bool,
}

impl<I> MergeIterator<I>
where
    I: Iterator<Item = io::Result<DecodedEntry>>,
{
    pub fn new(sources: Vec<I>) -> Self {
        Self {
            sources: sources.into_iter().map(|s| s.peekable()).collect(),
            errored: false,
        }
    }
}

impl<I> Iterator for MergeIterator<I>
where
    I: Iterator<Item = io::Result<DecodedEntry>>,
{
    type Item = io::Result<SstEntryInput>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.errored {
            return None;
        }

        // 1) Nađi najmanji ključ među "glavama" svih izvora koji još imaju
        //    šta da ponude. Greška iz bilo kog izvora se odmah propagira i
        //    zaustavlja ceo merge (korumpiran ulaz ne sme tiho da
        //    proizvede nepotpun izlaz).
        let mut min_key: Option<String> = None;
        for source in self.sources.iter_mut() {
            match source.peek() {
                Some(Ok(entry)) => {
                    if min_key.as_deref().map_or(true, |mk| entry.key.as_str() < mk) {
                        min_key = Some(entry.key.clone());
                    }
                }
                Some(Err(_)) => {
                    let err = source.next().unwrap().unwrap_err();
                    self.errored = true;
                    return Some(Err(err));
                }
                None => {}
            }
        }

        let min_key = min_key?; // svi izvori iscrpljeni -> merge gotov

        // 2) Konzumiraj SVAKI izvor čija je glava baš taj ključ, i zapamti
        //    pobednika (najveći seq_no) -- §6.5 "highest seqNo across
        //    inputs". Ostali (stariji) duplikati se prosto odbacuju.
        let mut winner: Option<DecodedEntry> = None;
        for source in self.sources.iter_mut() {
            let is_match = matches!(source.peek(), Some(Ok(e)) if e.key == min_key);
            if is_match {
                let entry = source.next().unwrap().unwrap();
                match &winner {
                    None => winner = Some(entry),
                    Some(w) if entry.seq_no > w.seq_no => winner = Some(entry),
                    _ => {} // stariji duplikat istog ključa -- odbačen
                }
            }
        }

        let winner = winner.expect("min_key je nađen u bar jednom izvoru u koraku 1");

        Some(Ok(SstEntryInput {
            key: winner.key,
            value: winner.value,
            seq_no: winner.seq_no,
            is_tombstone: winner.is_tombstone,
        }))
    }
}