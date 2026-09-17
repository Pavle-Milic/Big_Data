# LSMKV CLI API

Komande se mogu pokretati na dva načina:

- tokom razvoja iz projekta: `cargo run -- <komanda> ...`
- kada je `lsmkv` binary dostupan na PATH-u: `lsmkv <komanda> ...`

U primerima ispod koristim `lsmkv`. Isti poziv možeš pokrenuti preko Cargo-a tako što dodaš `cargo run --` ispred komande, npr. `cargo run -- get --key foo`.

## `init`
Pokreće LSMKV server.

Server učitava konfiguraciju, oporavlja WAL i memtable stanje, pokreće background flush i compaction workere i počinje da prihvata zahteve.

`--config <name>` je opcioni argument kojim se učitava konfiguracioni fajl umesto podrazumevane konfiguracije.

**Rezultat:** pokrenut LSMKV server spreman za obradu zahteva.

**Primer:** `lsmkv init --config dev`

---

## `put`
Upisuje ili ažurira vrednost za dati ključ.

**Rezultat:** vrednost se upisuje u WAL i aktivni memtable. Kada memtable dostigne prag za rotaciju, može biti prebačena u immutable memtable i zatim flush-ovana u SSTable. Fizička sinhronizacija WAL zapisa zavisi od podešavanja `wal_fsync_every_n`, a sama rotacija može biti odložena ako je aktivan `rotation_cooldown_ms`.

**Primer:** `lsmkv put --key user:42 --value Spiderman`

---

## `get`
Čita vrednost za dati ključ.

Pretraga ide kroz aktivni memtable, immutable memtables i SSTabele.

**Rezultat:**
- ako ključ postoji, vraća se njegova vrednost;
- ako ključ ne postoji ili je obrisan, vraća se `Key not found`.

**Primer:** `lsmkv get --key user:42`

---

## `del`
Briše dati ključ.

Operacija se izvršava samo ako ključ trenutno postoji.

**Rezultat:** za postojeći ključ upisuje se delete/tombstone zapis u WAL i memtable. Ako ključ ne postoji, vraća se `Key not found` i stanje baze se ne menja.

**Primer:** `lsmkv del --key user:42`

---

## `stats`
Prikazuje trenutno stanje LSM engine-a.

**Rezultat:** ispisuje statistiku WAL-a, aktivnog i immutable memtable-a, SSTabela, read path-a i cache-a, manifest/version stanja, kao i background/concurrency metrika.

**Primer:** `lsmkv stats`

---

## `memtable-dump`
Prikazuje sadržaj aktivnog i svih immutable memtable-a.

**Rezultat:** ispisuje ključeve, vrednosti i stanje zapisa iz memtable-a.

**Primer:** `lsmkv memtable-dump`

---

## `flush-now`
Ručno pokreće flush najstarijeg immutable memtable-a koji čeka.

**Rezultat:** immutable memtable se persistira kao nova SSTabela, ažurira se manifest i WAL watermark. Ako nema immutable memtable-a za flush, operacija prijavljuje da nema šta da se flush-uje.

**Primer:** `lsmkv flush-now`

---

## `close`
Zatvara LSMKV engine.

**Rezultat:** zatvara aktivni WAL i završava rad servera.

**Primer:** `lsmkv close`

---

## `list-sst`
Prikazuje aktivne SSTabele.

**Rezultat:** ispisuje listu SSTabela sa imenom fajla, veličinom, brojem unosa, opsegom ključeva i SeqNo opsegom, kao i ukupnu veličinu SSTabela.

**Primer:** `lsmkv list-sst`

---

## `verify-sst`
Proverava ispravnost jedne SSTabele.

`--file <file>` određuje SSTable fajl koji se proverava u aktivnom SST direktorijumu.

**Rezultat:** proveravaju se footer, filter blok, index blok i svi data blokovi, a rezultat je poruka da je SSTabela potpuno validna ili opis detektovane greške.

**Primer:** `lsmkv verify-sst --file 000001.sst`

---

## `sst-info`
Prikazuje strukturu i metapodatke jedne SSTabele.

`--file <file>` određuje SSTable fajl u aktivnom SST direktorijumu.

**Rezultat:** prikazuju se veličina fajla, broj unosa, SeqNo opseg, pozicija i veličina index/filter blokova, Bloom filter parametri, broj data blokova, statistika veličina blokova i prvi ključ svakog data bloka.

**Primer:** `lsmkv sst-info --file 000001.sst`

---

## `manifest-info`
Prikazuje trenutno stanje manifesta.

**Rezultat:** ispisuje putanju manifesta, verziju formata, epoch, sledeći SSTable ID, flush watermark i listu SSTabela evidentiranih u manifestu.

**Primer:** `lsmkv manifest-info`

---

## `version-info`
Prikazuje trenutno objavljenu verziju LSM engine-a.

**Rezultat:** ispisuje epoch/version ID, aktivni memtable, immutable memtable-e i SSTabele koje pripadaju trenutno objavljenoj verziji.

**Primer:** `lsmkv version-info`

---

## `compaction-run`
Ručno pokreće compaction.

`--files <id1,id2,...>` je opcion i određuje SSTabele koje treba spojiti. Ako nije naveden, candidate SSTabele bira compaction picker.

**Rezultat:** iz odabranih SSTabela pravi se nova SSTabela, ažurira se manifest i stare ulazne SSTabele se označavaju za odloženo brisanje.

Ako nisu pronađeni kandidati, compaction se ne izvršava.

**Primer:** `lsmkv compaction-run --files 1,2,3`

**Primer bez ručnog izbora fajlova:** `lsmkv compaction-run`

---

## `compaction-stats`
Prikazuje statistiku compaction sistema.

**Rezultat:** ispisuje stanje pause/running statusa, broj trenutno aktivnih poslova, SSTable backlog, ukupan broj izvršenih poslova, cumulative write-amplification odnos, vreme poslednjeg compaction-a, konfiguraciju compaction sistema i statistiku poslednjeg posla.

**Primer:** `lsmkv compaction-stats`

---

## `compaction-pause`
Pauzira automatski compaction.

**Rezultat:** novi compaction poslovi se neće automatski pokretati dok se compaction ne nastavi.

**Primer:** `lsmkv compaction-pause`

---

## `compaction-resume`
Nastavlja compaction nakon pauze.

**Rezultat:** compaction ponovo može da se pokreće, a worker se signalizira da proveri da li postoji posao za izvršavanje.

**Primer:** `lsmkv compaction-resume`

---

## `bg-status`
Prikazuje trenutno stanje background workera.

**Rezultat:** ispisuje stanje FlushWorker-a i CompactionWorker-a, broj immutable memtable-a, backpressure stanje, poslednji flush, trenutno aktivne compaction poslove, pending SSTable deletions i publish/backpressure metrike.

**Primer:** `lsmkv bg-status`

---

## `shutdown`
Zaustavlja LSMKV server i bira način gašenja.

- `--graceful`: čeka da se immutable memtable-i flushuju, zaustavlja compaction i zatim zatvara WAL.
- `--fast`: koristi fast shutdown putanju; pokušava flush dok ne istekne `shutdown_timeout_ms`, nakon čega preostali immutable memtable-i mogu biti ponovo rekonstruisani iz WAL-a pri sledećem pokretanju.

**Rezultat:** server završava shutdown, a WAL se zatvara.

**Primer:** `lsmkv shutdown --graceful`

**Primer fast režima:** `lsmkv shutdown --fast`

---

## `configure`
Kreira ili ažurira konfiguracioni fajl.

Komanda prima naziv konfiguracije i opcione parametre za engine, WAL, SSTable, cache, manifest, compaction, backpressure i background workere.

**Rezultat:** konfiguracija se upisuje u `config/<name>.json`. Nova konfiguracija postaje aktivna nakon ponovnog pokretanja servera sa odgovarajućom `init --config` komandom.

**Primer:** `lsmkv configure --name dev --memtable-max-bytes 1048576 --wal-fsync-every-n 10 --l0-compaction-trigger 4`

---

## `wal-verify`
Proverava WAL fajlove bez menjanja njihovog sadržaja.

**Rezultat:** prijavljuje broj WAL segmenata, broj validnih zapisa, poslednji SeqNo i da li bi neki segment zahtevao truncation. Provera je read-only.

**Primer:** `lsmkv wal-verify`

---

## `wal-truncate`
Ručno skraćuje jedan WAL segment na zadatu veličinu.

`--segment <segment>` određuje WAL segment, a `--offset <offset>` određuje novu veličinu fajla u bajtovima.

**Rezultat:** navedeni WAL segment je skraćen na zadati broj bajtova.

**Primer:** `lsmkv wal-truncate --segment 000001.wal --offset 4096`

---

## `wal-dump`
Čita i ispisuje sadržaj jednog WAL segmenta.

`--segment <segment>` određuje WAL segment.

**Rezultat:** za svaki zapis prikazuje SeqNo, tip zapisa (`PUT` ili `DEL`), ključ i vrednost.

**Primer:** `lsmkv wal-dump --segment 000001.wal`
