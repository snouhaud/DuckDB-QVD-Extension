//! Lecture du format QVD via le crate `qvdrs`, en **streaming dictionnaire**.
//!
//! Le QVD est un format colonne dictionnaire-encodé : chaque colonne a une table
//! de **symboles** (valeurs distinctes) et un bloc d'**index** vers ces symboles.
//! À l'ouverture d'un fichier, seul l'en-tête XML est lu (`read_header`, validé
//! contre la taille du fichier), puis les tables de symboles des seules colonnes
//! **projetées** (`qvdrs::symbol::read_symbols`). Chaque symbole projeté n'est
//! **converti qu'une seule fois** par fichier (le dictionnaire de `Cell`). L'index
//! est ensuite lu par chunks de 2048 lignes, dont on n'extrait que les bits des
//! champs projetés (`qvdrs::index::read_field_index`) ; les lignes sont émises par
//! simple lookup. Mémoire bornée (≈ tables de symboles projetées + un chunk
//! d'index).
//!
//! # Correspondance des types
//!
//! Le type DuckDB est déduit des **tags Qlik** de l'en-tête, avec repli sur la
//! balise `<Type>` (`NumberFormat.format_type`) :
//!
//! | Tag Qlik (ou `<Type>` en repli) | Type DuckDB |
//! |---------------------------------|-------------|
//! | `$date`                         | `DATE`      |
//! | `$timestamp` (sans `$date`)     | `TIMESTAMP` |
//! | `$time`                         | `TIME`      |
//! | `$interval`                     | `INTERVAL`  |
//! | `$text` / `$ascii`              | `VARCHAR`   |
//! | `$integer`                      | `BIGINT`    |
//! | `$numeric` (sans `$integer`)    | `DOUBLE`    |
//!
//! Les séries temporelles Qlik (numéro de série, époque 1899-12-30) sont
//! converties en `DATE`/`TIMESTAMP`/`TIME`/`INTERVAL` natif DuckDB.
//!
//! L'**écriture** (`COPY TO`) reste sur `openqvd` (voir `src/copy.rs`).

use std::error::Error;
use std::sync::Mutex;

use qvdrs::header::{QvdFieldHeader, QvdTableHeader};
use qvdrs::{QvdSymbol, QvdValue};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};

/// Source lisible d'un QVD : fichier local (tests) ou fichier du système de
/// fichiers virtuel de DuckDB (local, `s3://`, `https://`…).
pub(crate) trait Source: Read + Seek + Send {}
impl<T: Read + Seek + Send> Source for T {}

/// Ouvre un chemin en lecture.
pub(crate) type Open<'a> = &'a dyn Fn(&str) -> std::io::Result<Box<dyn Source>>;

/// Nombre max de lignes décodées par chunk.
const VECTOR_SIZE: usize = 2048;

/// Taille visée d'une lecture du bloc d'index (plusieurs chunks à la fois) : en
/// distant, chaque lecture est une requête HTTP ; 8 Mio en amortissent la latence.
const INDEX_READ_BYTES: usize = 8 << 20;

/// Taille maximale lue pour trouver la fin de l'en-tête XML.
const MAX_HEADER_BYTES: u64 = 64 << 20;

/// Représentation physique d'un `INTERVAL` DuckDB (`duckdb_interval`).
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct IntervalVal {
    pub months: i32,
    pub days: i32,
    pub micros: i64,
}

/// Schéma d'un QVD : noms et catégories de toutes les colonnes.
/// Lu au `bind` sans décoder les données.
pub(crate) struct Schema {
    pub names: Vec<String>,
    pub kinds: Vec<Kind>,
}

/// Catégorie de colonne, déduite des tags Qlik.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Kind {
    Int,
    Float,
    Text,
    Date,
    Timestamp,
    Time,
    Interval,
}

/// Valeur convertie, prête à écrire dans un vecteur DuckDB. Mutualise la logique
/// d'écriture (`write_cells`) entre `read_qvd` et `COPY ... FROM`.
#[derive(Clone)]
pub(crate) enum Cell {
    I64(i64),
    F64(f64),
    Str(String),
    I32(i32),
    Interval(IntervalVal),
    Null,
}

/// Décalage entre l'époque des numéros de série Qlik (1899-12-30) et celle de
/// DuckDB (1970-01-01), en jours.
pub(crate) const QLIK_EPOCH_OFFSET_DAYS: f64 = 25569.0;
pub(crate) const MICROS_PER_DAY: f64 = 86_400_000_000.0;

/// Ouvre un QVD et lit **uniquement l'en-tête XML** (jusqu'à l'octet nul), sans
/// toucher aux tables de symboles ni à l'index. Renvoie le fichier (positionné de
/// façon quelconque — toujours reseek en absolu ensuite), l'en-tête parsé, et
/// `binary_start` = offset absolu du début de la section binaire.
///
/// L'en-tête n'est pas fiable (fichier quelconque) : ses tailles et offsets sont
/// validés contre la taille réelle du fichier avant toute allocation qui en dépend.
fn read_header(path: &str, open: Open) -> Result<(Box<dyn Source>, QvdTableHeader, u64), Box<dyn Error>> {
    let mut file = open(path)?;
    let file_len = file.seek(SeekFrom::End(0))?;
    file.seek(SeekFrom::Start(0))?;
    let mut xml = Vec::new();
    {
        let mut br = BufReader::new(&mut file).take(MAX_HEADER_BYTES);
        br.read_until(0, &mut xml)?;
    }
    if xml.last() != Some(&0) {
        return Err(format!("'{path}' : en-tête QVD introuvable (pas de fin d'en-tête XML)").into());
    }
    let binary_start = xml.len() as u64;
    xml.pop();
    let header = qvdrs::header::parse_xml_header(&String::from_utf8(xml)?)?;
    validate_header(&header, binary_start, file_len).map_err(|e| format!("'{path}' : {e}"))?;
    Ok((file, header, binary_start))
}

/// Vérifie que les sections décrites par l'en-tête tiennent dans le fichier :
/// symboles `[binary_start, +offset)`, puis index `no_of_records × record_byte_size`,
/// chaque table de symboles dans sa section, chaque champ dans un record. Rend
/// sûres les allocations et l'arithmétique d'offsets qui suivent.
fn validate_header(h: &QvdTableHeader, binary_start: u64, file_len: u64) -> Result<(), String> {
    let index_len = (h.no_of_records as u64).checked_mul(h.record_byte_size as u64);
    let end = index_len.and_then(|i| binary_start.checked_add(h.offset as u64)?.checked_add(i));
    if end.is_none_or(|end| end > file_len) {
        return Err("sections symboles/index plus grandes que le fichier".to_string());
    }
    let record_bits = h.record_byte_size * 8;
    for f in &h.fields {
        if f.offset.checked_add(f.length).is_none_or(|e| e > h.offset) {
            return Err(format!("table de symboles de '{}' hors de la section des symboles", f.field_name));
        }
        // Un symbole occupe au moins un octet (son octet de type).
        if f.no_of_symbols > f.length {
            return Err(format!("champ '{}' : nombre de symboles incohérent", f.field_name));
        }
        if f.bit_width > 64 || f.bit_offset.checked_add(f.bit_width).is_none_or(|e| e > record_bits) {
            return Err(format!("champ '{}' : position de bits hors du record", f.field_name));
        }
    }
    Ok(())
}

/// Lit uniquement le schéma (en-tête) d'un QVD, sans décoder l'index ni les symboles.
pub(crate) fn read_schema(path: &str, open: Open) -> Result<Schema, Box<dyn Error>> {
    let (_file, header, _bs) = read_header(path, open)?;
    let mut names = Vec::new();
    let mut kinds = Vec::new();
    for f in &header.fields {
        let kind = kind_of_field(f);
        names.push(f.field_name.clone());
        kinds.push(kind);
    }
    Ok(Schema { names, kinds })
}

/// État mutable d'un scan (un fichier ouvert à la fois pour le glob).
struct ScanState {
    file_idx: usize,
    // Fichier courant ouvert (None tant qu'un fichier n'est pas entamé).
    file: Option<Box<dyn Source>>,
    /// Colonne projetée -> en-tête de son champ dans le fichier courant
    /// (None si le champ est absent → la colonne sort en `NULL`).
    fields: Vec<Option<QvdFieldHeader>>,
    /// Colonne projetée -> dictionnaire de `Cell` (symboles convertis 1× par fichier).
    dicts: Vec<Vec<Cell>>,
    file_rows: usize,  // lignes du fichier courant
    record_bytes: usize,
    index_off: u64,    // offset absolu du bloc d'index dans le fichier
    row: usize,        // curseur de ligne dans le fichier courant
    /// Octets d'index lus d'avance (records entiers) et première ligne couverte.
    idx_buf: Vec<u8>,
    idx_buf_row: usize,
}

/// Un paquet de lignes tiré du scan.
pub(crate) enum Pull {
    /// Données : colonnes projetées déjà converties (un chunk, possédées).
    Cells { columns: Vec<Vec<Cell>> },
    /// Scan terminé.
    Done,
}

/// Scan d'un (ou plusieurs, glob) fichier(s) QVD, à **décodage projeté**. Sert
/// d'`InitData`. À l'ouverture d'un fichier : en-tête seul + symboles des colonnes
/// **projetées** convertis une fois (le dictionnaire). Par chunk : on lit les octets
/// d'index et on extrait (`read_field_index`) **uniquement** les champs projetés,
/// puis on émet par lookup d'index. Mémoire bornée (dictionnaires projetés + 1 chunk
/// d'index) ; coût ∝ lignes × colonnes **projetées**, pas × toutes les colonnes.
pub(crate) struct QvdScan {
    paths: Vec<String>,
    needed_names: Vec<String>,
    needed_kinds: Vec<Kind>,
    state: Mutex<ScanState>,
}

impl QvdScan {
    /// Prépare le scan (aucune lecture ici : chaque fichier est ouvert à son tour
    /// par `pull`). DuckDB demande toujours au moins une colonne à une table
    /// function, `COUNT(*)` compris.
    pub(crate) fn new(
        paths: &[String],
        names: &[String],
        kinds: &[Kind],
        indices: &[usize],
    ) -> Result<Self, Box<dyn Error>> {
        let needed_names = indices.iter().map(|&i| names[i].clone()).collect();
        let needed_kinds = indices.iter().map(|&i| kinds[i]).collect();
        Ok(QvdScan {
            paths: paths.to_vec(),
            needed_names,
            needed_kinds,
            state: Mutex::new(ScanState {
                file_idx: 0,
                file: None,
                fields: Vec::new(),
                dicts: Vec::new(),
                file_rows: 0,
                record_bytes: 0,
                index_off: 0,
                row: 0,
                idx_buf: Vec::new(),
                idx_buf_row: 0,
            }),
        })
    }

    /// Types de sortie (ordre des colonnes émises).
    pub(crate) fn output_kinds(&self) -> &[Kind] {
        &self.needed_kinds
    }

    /// Ouvre le fichier courant : en-tête seul, dictionnaires des colonnes projetées
    /// (symboles convertis une fois), et en-têtes de champ projetés. Stocke l'état.
    fn open_current(&self, st: &mut ScanState, open: Open) -> Result<(), Box<dyn Error>> {
        let (mut file, header, binary_start) = read_header(&self.paths[st.file_idx], open)?;

        // Résolution par nom (None si champ absent → colonne en NULL).
        let positions: Vec<Option<usize>> = self
            .needed_names
            .iter()
            .map(|nm| header.fields.iter().position(|f| &f.field_name == nm))
            .collect();

        // Section symboles = [binary_start .. binary_start + header.offset]. Deux
        // stratégies : si la projection couvre l'essentiel de la section, une seule
        // lecture séquentielle bat N seeks (mêmes octets, moins de syscalls) ; sinon
        // on ne lit QUE la plage d'octets de chaque colonne projetée. Sur un QVD
        // large, les tables de symboles non projetées peuvent représenter
        // l'essentiel du fichier, et les lire coûte de l'I/O et de la mémoire pour
        // rien. La projection était déjà appliquée au *décodage* ; elle l'est
        // désormais aussi à la *lecture*.
        //
        // EN: symbol section = [binary_start .. binary_start + header.offset]. Two
        // strategies: when the projection covers most of the section, a single
        // sequential read beats N seeks (same bytes, fewer syscalls); otherwise we
        // read ONLY each projected field's byte range. On a wide QVD the
        // non-projected symbol tables can account for most of the file, so reading
        // them wastes I/O and memory. Projection already applied at *decode* time;
        // now also at *read* time.
        let needed_bytes: usize = positions
            .iter()
            .filter_map(|p| p.map(|fi| header.fields[fi].length))
            .sum();
        let bulk = needed_bytes.saturating_mul(4) >= header.offset.saturating_mul(3);
        let sym_section = if bulk {
            let mut buf = vec![0u8; header.offset];
            file.seek(SeekFrom::Start(binary_start))?;
            file.read_exact(&mut buf)?;
            Some(buf)
        } else {
            None
        };

        let mut dicts: Vec<Vec<Cell>> = Vec::with_capacity(positions.len());
        for (j, p) in positions.iter().enumerate() {
            let cells = match p {
                Some(fi) => {
                    let f = &header.fields[*fi];
                    let syms = if let Some(sec) = &sym_section {
                        qvdrs::symbol::read_symbols(sec, f)
                    } else {
                        // Plage bornée à la section des symboles par `validate_header`.
                        let mut sym_buf = vec![0u8; f.length];
                        file.seek(SeekFrom::Start(binary_start + f.offset as u64))?;
                        file.read_exact(&mut sym_buf)?;
                        // `read_symbols` tranche `buf[offset .. offset + length]` ;
                        // ici le buffer commence au champ, donc offset local = 0.
                        // EN: `read_symbols` slices `buf[offset .. offset + length]`;
                        // this buffer starts at the field, so local offset is 0.
                        let mut local = f.clone();
                        local.offset = 0;
                        qvdrs::symbol::read_symbols(&sym_buf, &local)
                    };
                    syms.map_err(|e| format!("table de symboles de '{}' : {e}", f.field_name))?
                        .iter()
                        .map(|x| convert(self.needed_kinds[j], &QvdValue::Symbol(x.clone())))
                        .collect()
                }
                None => Vec::new(),
            };
            dicts.push(cells);
        }

        st.fields = positions.iter().map(|p| p.map(|fi| header.fields[fi].clone())).collect();
        st.dicts = dicts;
        st.file_rows = header.no_of_records;
        st.record_bytes = header.record_byte_size;
        st.index_off = binary_start + header.offset as u64;
        st.row = 0;
        st.idx_buf.clear();
        st.idx_buf_row = 0;
        st.file = Some(file);
        Ok(())
    }

    /// Tire le prochain paquet de lignes (un chunk d'un fichier), en traversant les
    /// fichiers du glob. L'état avance sous verrou ; les données rendues sont possédées.
    pub(crate) fn pull(&self, open: Open) -> Result<Pull, Box<dyn Error>> {
        let mut st = self.state.lock().unwrap();

        loop {
            if st.file.is_none() {
                if st.file_idx >= self.paths.len() {
                    return Ok(Pull::Done);
                }
                self.open_current(&mut st, open)?;
            }

            if st.row >= st.file_rows {
                // Fichier épuisé → suivant (drop le fichier + son dictionnaire).
                st.file = None;
                st.file_idx += 1;
                continue;
            }

            let rbs = st.record_bytes;
            let n = (st.file_rows - st.row).min(VECTOR_SIZE);

            // Octets d'index du chunk (records entiers, champs entrelacés bit-packés),
            // lus d'avance par blocs d'environ INDEX_READ_BYTES quand le tampon ne
            // couvre plus le chunk.
            let buffered_rows = if rbs == 0 { usize::MAX } else { st.idx_buf.len() / rbs };
            if st.row + n > st.idx_buf_row.saturating_add(buffered_rows) {
                let rows = (st.file_rows - st.row).min((INDEX_READ_BYTES / rbs.max(1)).max(n));
                let off = st.index_off + (st.row as u64) * rbs as u64;
                let mut block = std::mem::take(&mut st.idx_buf);
                block.resize(rows * rbs, 0);
                let f = st.file.as_mut().unwrap();
                f.seek(SeekFrom::Start(off))?;
                f.read_exact(&mut block)?;
                st.idx_buf = block;
                st.idx_buf_row = st.row;
            }
            let start = (st.row - st.idx_buf_row) * rbs;
            let buf = &st.idx_buf[start..start + n * rbs];

            // N'extraire (read_field_index) QUE les champs projetés → Cell par lookup.
            let columns: Vec<Vec<Cell>> = (0..self.needed_kinds.len())
                .map(|j| match &st.fields[j] {
                    Some(field) => {
                        let dict = &st.dicts[j];
                        (0..n)
                            .map(|r| {
                                let record = &buf[r * rbs..r * rbs + rbs];
                                let idx = qvdrs::index::read_field_index(record, field);
                                if idx < 0 || idx as usize >= dict.len() {
                                    Cell::Null
                                } else {
                                    dict[idx as usize].clone()
                                }
                            })
                            .collect()
                    }
                    None => vec![Cell::Null; n],
                })
                .collect();

            st.row += n;
            return Ok(Pull::Cells { columns });
        }
    }
}

/// Convertit une cellule QVD en valeur typée DuckDB selon le `Kind` de la colonne.
pub(crate) fn convert(kind: Kind, v: &QvdValue) -> Cell {
    match kind {
        Kind::Int => val_to_i64(v).map_or(Cell::Null, Cell::I64),
        Kind::Float => val_to_f64(v).map_or(Cell::Null, Cell::F64),
        Kind::Text => val_to_string(v).map_or(Cell::Null, Cell::Str),
        Kind::Date => serial_to_days(v).map_or(Cell::Null, Cell::I32),
        Kind::Timestamp => serial_to_micros(v).map_or(Cell::Null, Cell::I64),
        Kind::Time => serial_to_time_micros(v).map_or(Cell::Null, Cell::I64),
        Kind::Interval => serial_to_interval(v).map_or(Cell::Null, Cell::Interval),
    }
}

/// Déduit la catégorie d'un champ à partir de ses **tags Qlik**, avec repli sur
/// `NumberFormat.format_type`.
fn kind_of_field(f: &QvdFieldHeader) -> Kind {
    let has = |t: &str| f.tags.iter().any(|x| x == t);
    if has("$date") {
        return Kind::Date;
    }
    if has("$timestamp") {
        return Kind::Timestamp;
    }
    if has("$time") {
        return Kind::Time;
    }
    if has("$interval") {
        return Kind::Interval;
    }
    if has("$text") || has("$ascii") {
        return Kind::Text;
    }
    if has("$integer") {
        return Kind::Int;
    }
    if has("$numeric") {
        return Kind::Float;
    }
    match f.number_format.format_type.as_str() {
        "INTEGER" => Kind::Int,
        "REAL" | "FIX" | "MONEY" => Kind::Float,
        "DATE" => Kind::Date,
        "TIME" => Kind::Time,
        "TIMESTAMP" => Kind::Timestamp,
        _ => Kind::Text,
    }
}

fn val_to_i64(v: &QvdValue) -> Option<i64> {
    match v {
        QvdValue::Null => None,
        QvdValue::Symbol(s) => match s {
            QvdSymbol::Int(n) | QvdSymbol::DualInt(n, _) => Some(*n as i64),
            QvdSymbol::Double(f) | QvdSymbol::DualDouble(f, _) => Some(*f as i64),
            QvdSymbol::Text(t) => t.trim().parse().ok(),
        },
    }
}

/// Valeur numérique (flottants et séries temporelles Qlik).
fn val_to_f64(v: &QvdValue) -> Option<f64> {
    match v {
        QvdValue::Null => None,
        QvdValue::Symbol(s) => match s {
            QvdSymbol::Int(n) | QvdSymbol::DualInt(n, _) => Some(*n as f64),
            QvdSymbol::Double(f) | QvdSymbol::DualDouble(f, _) => Some(*f),
            QvdSymbol::Text(t) => t.trim().parse().ok(),
        },
    }
}

fn val_to_string(v: &QvdValue) -> Option<String> {
    match v {
        QvdValue::Null => None,
        QvdValue::Symbol(s) => Some(match s {
            QvdSymbol::Int(n) => n.to_string(),
            QvdSymbol::Double(f) => f.to_string(),
            QvdSymbol::Text(t) => t.clone(),
            // Pour les duals, on privilégie le texte rendu par Qlik.
            QvdSymbol::DualInt(_, t) => t.clone(),
            QvdSymbol::DualDouble(_, t) => t.clone(),
        }),
    }
}

/// Numéro de série Qlik → jours depuis l'époque DuckDB (pour `DATE`). Tronqué :
/// la partie décimale est l'heure, elle ne doit pas faire changer de jour.
fn serial_to_days(v: &QvdValue) -> Option<i32> {
    val_to_f64(v).map(|s| (s - QLIK_EPOCH_OFFSET_DAYS).floor() as i32)
}

/// Numéro de série Qlik → microsecondes depuis l'époque DuckDB (pour `TIMESTAMP`).
fn serial_to_micros(v: &QvdValue) -> Option<i64> {
    val_to_f64(v).map(|s| ((s - QLIK_EPOCH_OFFSET_DAYS) * MICROS_PER_DAY).round() as i64)
}

/// Heure Qlik (fraction de jour) → microsecondes depuis minuit (pour `TIME`).
fn serial_to_time_micros(v: &QvdValue) -> Option<i64> {
    val_to_f64(v).map(|s| {
        let frac = s - s.floor();
        (frac * MICROS_PER_DAY).round() as i64
    })
}

/// Durée Qlik (en jours, fractionnaire) → `INTERVAL` (mois/jours/µs).
fn serial_to_interval(v: &QvdValue) -> Option<IntervalVal> {
    val_to_f64(v).map(|s| {
        let days = s.trunc();
        let micros = ((s - days) * MICROS_PER_DAY).round() as i64;
        IntervalVal { months: 0, days: days as i32, micros }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use openqvd::{Column, Value, WriteTable};

    /// Ouverture locale (std::fs) pour les tests, sans DuckDB.
    fn local(path: &str) -> std::io::Result<Box<dyn Source>> {
        Ok(Box::new(std::fs::File::open(path)?))
    }

    /// Colonne openqvd avec ses tags Qlik (génération des fichiers de test).
    fn tagged(name: &str, values: Vec<Option<Value>>, tags: &[&str]) -> Column {
        let mut c = Column::new(name, values);
        c.tags = tags.iter().map(|t| t.to_string()).collect();
        c
    }

    fn write_temp(name: &str, cols: Vec<Column>) -> String {
        let bytes = WriteTable::new("demo", cols).unwrap().to_bytes().unwrap();
        let path = std::env::temp_dir().join(name);
        std::fs::write(&path, bytes).unwrap();
        path.to_str().unwrap().to_string()
    }

    /// Colonne typée de test (équivalent de l'ancien ColumnData, côté test).
    #[derive(Debug, PartialEq)]
    enum TestCol {
        I64(Vec<Option<i64>>),
        F64(Vec<Option<f64>>),
        Utf8(Vec<Option<String>>),
        Date(Vec<Option<i32>>),
        Time(Vec<Option<i64>>),
        Interval(Vec<Option<IntervalVal>>),
    }

    impl TestCol {
        fn empty(k: Kind) -> Self {
            match k {
                Kind::Int => TestCol::I64(Vec::new()),
                Kind::Float => TestCol::F64(Vec::new()),
                Kind::Text => TestCol::Utf8(Vec::new()),
                Kind::Date => TestCol::Date(Vec::new()),
                Kind::Timestamp | Kind::Time => TestCol::Time(Vec::new()),
                Kind::Interval => TestCol::Interval(Vec::new()),
            }
        }
        fn push(&mut self, c: Cell) {
            match (self, c) {
                (TestCol::I64(v), Cell::I64(x)) => v.push(Some(x)),
                (TestCol::I64(v), Cell::Null) => v.push(None),
                (TestCol::F64(v), Cell::F64(x)) => v.push(Some(x)),
                (TestCol::F64(v), Cell::Null) => v.push(None),
                (TestCol::Utf8(v), Cell::Str(s)) => v.push(Some(s)),
                (TestCol::Utf8(v), Cell::Null) => v.push(None),
                (TestCol::Date(v), Cell::I32(x)) => v.push(Some(x)),
                (TestCol::Date(v), Cell::Null) => v.push(None),
                (TestCol::Time(v), Cell::I64(x)) => v.push(Some(x)),
                (TestCol::Time(v), Cell::Null) => v.push(None),
                (TestCol::Interval(v), Cell::Interval(x)) => v.push(Some(x)),
                (TestCol::Interval(v), Cell::Null) => v.push(None),
                _ => unreachable!("Cell incohérent avec le Kind de la colonne"),
            }
        }
    }

    /// Draine un scan en colonnes typées, pour pouvoir asserter sur les valeurs.
    fn collect(paths: &[String], names: &[String], kinds: &[Kind], indices: &[usize]) -> (Vec<TestCol>, usize) {
        let scan = QvdScan::new(paths, names, kinds, indices).unwrap();
        let out_kinds: Vec<Kind> = indices.iter().map(|&i| kinds[i]).collect();
        let mut cols: Vec<TestCol> = out_kinds.iter().map(|&k| TestCol::empty(k)).collect();
        let mut total = 0usize;
        loop {
            match scan.pull(&local).unwrap() {
                Pull::Done => break,
                Pull::Cells { columns } => {
                    total += columns.first().map_or(0, |c| c.len());
                    for (j, col) in cols.iter_mut().enumerate() {
                        for cell in columns[j].iter() {
                            col.push(cell.clone());
                        }
                    }
                }
            }
        }
        (cols, total)
    }

    /// Reader projeté : tous les `Kind` (dont temporels Date/Time/Interval), avec
    /// projection inversée et un champ absent → NULL.
    #[test]
    fn reads_all_kinds_with_reversed_projection() {
        let cols = vec![
            tagged("day", vec![Some(Value::Int(45000)), Some(Value::Int(45001))], &["$date"]),
            tagged("t", vec![Some(Value::Float(0.5)), Some(Value::Float(0.25))], &["$time"]),
            tagged("dur", vec![Some(Value::Float(1.5)), Some(Value::Float(2.0))], &["$interval"]),
            tagged("name", vec![Some(Value::Str("x".into())), None], &["$text"]),
            tagged("qty", vec![Some(Value::Int(10)), Some(Value::Int(10))], &["$integer"]),
        ];
        let path = write_temp("qvdrs_proj_test.qvd", cols);
        let s = read_schema(&path, &local).unwrap();

        // Projection inversée (qty, name, dur, t, day) pour exercer le mapping.
        let (c, n) = collect(&[path], &s.names, &s.kinds, &[4, 3, 2, 1, 0]);
        assert_eq!(n, 2);
        assert_eq!(c[0], TestCol::I64(vec![Some(10), Some(10)]));               // qty
        assert_eq!(c[1], TestCol::Utf8(vec![Some("x".to_string()), None]));      // name (NULL)
        assert_eq!(c[2], TestCol::Interval(vec![                                 // dur
            Some(IntervalVal { months: 0, days: 1, micros: 43_200_000_000 }),
            Some(IntervalVal { months: 0, days: 2, micros: 0 }),
        ]));
        assert_eq!(c[3], TestCol::Time(vec![Some(43_200_000_000), Some(21_600_000_000)])); // t
        assert_eq!(c[4], TestCol::Date(vec![Some(19431), Some(19432)]));          // day
    }

    /// Lit schéma + toutes les colonnes (projection = tous les champs).
    fn read_all(path: &str) -> (Vec<String>, Vec<Kind>, Vec<TestCol>, usize) {
        let s = read_schema(path, &local).unwrap();
        let indices: Vec<usize> = (0..s.names.len()).collect();
        let (cols, n) = collect(&[path.to_string()], &s.names, &s.kinds, &indices);
        (s.names, s.kinds, cols, n)
    }

    #[test]
    fn read_qvd_infers_types_and_values() {
        // 45000/45001/45002 (série Qlik) → 19431/19432/19433 (jours DuckDB).
        let cols = vec![
            tagged("id", vec![Some(Value::Int(1)), Some(Value::Int(2)), Some(Value::Int(3))], &["$integer"]),
            tagged(
                "price",
                vec![Some(Value::Float(1.5)), Some(Value::Float(2.0)), Some(Value::Float(3.25))],
                &["$numeric"],
            ),
            tagged(
                "name",
                vec![Some(Value::Str("a".into())), Some(Value::Str("b".into())), Some(Value::Str("a".into()))],
                &["$text"],
            ),
            tagged("qty", vec![Some(Value::Int(10)), None, Some(Value::Int(30))], &["$integer"]),
            tagged(
                "day",
                vec![Some(Value::Int(45000)), Some(Value::Int(45001)), Some(Value::Int(45002))],
                &["$date", "$timestamp"],
            ),
        ];
        let path = write_temp("qvdrs_read_test.qvd", cols);
        let (names, kinds, columns, num_rows) = read_all(&path);

        assert_eq!(num_rows, 3);
        assert_eq!(names, ["id", "price", "name", "qty", "day"]);
        assert_eq!(kinds[0], Kind::Int);
        assert_eq!(kinds[1], Kind::Float);
        assert_eq!(kinds[2], Kind::Text);
        assert_eq!(kinds[3], Kind::Int);
        assert_eq!(kinds[4], Kind::Date);

        match &columns[0] {
            TestCol::I64(v) => assert_eq!(v, &[Some(1), Some(2), Some(3)]),
            _ => panic!("id devrait être I64"),
        }
        match &columns[1] {
            TestCol::F64(v) => assert_eq!(v, &[Some(1.5), Some(2.0), Some(3.25)]),
            _ => panic!("price devrait être F64"),
        }
        match &columns[2] {
            TestCol::Utf8(v) => {
                assert_eq!(v, &[Some("a".to_string()), Some("b".to_string()), Some("a".to_string())])
            }
            _ => panic!("name devrait être Utf8"),
        }
        match &columns[3] {
            TestCol::I64(v) => assert_eq!(v, &[Some(10), None, Some(30)]),
            _ => panic!("qty devrait être I64"),
        }
        match &columns[4] {
            TestCol::Date(v) => assert_eq!(v, &[Some(19431), Some(19432), Some(19433)]),
            _ => panic!("day devrait être Date"),
        }
    }

    #[test]
    fn projection_reads_only_requested_columns() {
        let cols = vec![
            tagged("a", vec![Some(Value::Int(1)), Some(Value::Int(2))], &["$integer"]),
            tagged("b", vec![Some(Value::Float(9.5)), Some(Value::Float(8.5))], &["$numeric"]),
            tagged("c", vec![Some(Value::Str("x".into())), Some(Value::Str("y".into()))], &["$text"]),
        ];
        let path = write_temp("qvdrs_projection_test.qvd", cols);

        let s = read_schema(&path, &local).unwrap();
        assert_eq!(s.names, ["a", "b", "c"]);

        // Projeter c puis a (ordre inversé, b exclue).
        let (columns, num_rows) = collect(&[path], &s.names, &s.kinds, &[2, 0]);
        assert_eq!(num_rows, 2);
        assert_eq!(columns.len(), 2);
        match &columns[0] {
            TestCol::Utf8(v) => assert_eq!(v, &[Some("x".to_string()), Some("y".to_string())]),
            _ => panic!("position 0 devrait être c (VARCHAR)"),
        }
        match &columns[1] {
            TestCol::I64(v) => assert_eq!(v, &[Some(1), Some(2)]),
            _ => panic!("position 1 devrait être a (BIGINT)"),
        }
    }

    #[test]
    fn multi_file_concatenates_rows() {
        let f1 = vec![
            tagged("id", vec![Some(Value::Int(1)), Some(Value::Int(2))], &["$integer"]),
            tagged("name", vec![Some(Value::Str("a".into())), Some(Value::Str("b".into()))], &["$text"]),
        ];
        // Deuxième fichier : colonnes dans l'ordre inverse (name puis id).
        let f2 = vec![
            tagged("name", vec![Some(Value::Str("c".into()))], &["$text"]),
            tagged("id", vec![Some(Value::Int(3))], &["$integer"]),
        ];
        let p1 = write_temp("qvdrs_multi_1.qvd", f1);
        let p2 = write_temp("qvdrs_multi_2.qvd", f2);

        let s = read_schema(&p1, &local).unwrap();
        let indices: Vec<usize> = (0..s.names.len()).collect();
        let (columns, n) = collect(&[p1, p2], &s.names, &s.kinds, &indices);

        assert_eq!(n, 3);
        match &columns[0] {
            TestCol::I64(v) => assert_eq!(v, &[Some(1), Some(2), Some(3)]),
            _ => panic!("id devrait être I64"),
        }
        match &columns[1] {
            TestCol::Utf8(v) => {
                assert_eq!(v, &[Some("a".to_string()), Some("b".to_string()), Some("c".to_string())])
            }
            _ => panic!("name devrait être Utf8"),
        }
    }

    #[test]
    fn read_qvd_maps_time_and_interval() {
        let cols = vec![
            tagged("t", vec![Some(Value::Float(0.5)), Some(Value::Float(0.25))], &["$time"]),
            tagged("dur", vec![Some(Value::Float(1.5)), Some(Value::Float(2.0))], &["$interval"]),
        ];
        let path = write_temp("qvdrs_time_interval.qvd", cols);
        let (_, kinds, columns, _) = read_all(&path);

        assert_eq!(kinds[0], Kind::Time);
        assert_eq!(kinds[1], Kind::Interval);

        match &columns[0] {
            TestCol::Time(v) => assert_eq!(v, &[Some(12 * 3_600_000_000), Some(6 * 3_600_000_000)]),
            _ => panic!("t devrait être Time"),
        }
        match &columns[1] {
            TestCol::Interval(v) => {
                let a = v[0].unwrap();
                assert_eq!((a.months, a.days, a.micros), (0, 1, 12 * 3_600_000_000));
                let b = v[1].unwrap();
                assert_eq!((b.months, b.days, b.micros), (0, 2, 0));
            }
            _ => panic!("dur devrait être Interval"),
        }
    }

    /// Génère `/tmp/ti_sample.qvd` (tags $time/$interval) pour un test live :
    /// `cargo test --lib -- --ignored gen_time_interval_file`.
    #[test]
    #[ignore]
    fn gen_time_interval_file() {
        let cols = vec![
            tagged("etiquette", vec![Some(Value::Str("midi".into())), Some(Value::Str("matin".into()))], &["$text"]),
            tagged("heure", vec![Some(Value::Float(0.5)), Some(Value::Float(0.25))], &["$time"]),
            tagged("duree", vec![Some(Value::Float(1.5)), Some(Value::Float(2.0))], &["$interval"]),
        ];
        let bytes = WriteTable::new("ti", cols).unwrap().to_bytes().unwrap();
        std::fs::write("/tmp/ti_sample.qvd", bytes).unwrap();
    }

    /// En-tête non fiable : chaque valeur incohérente avec la taille du fichier est
    /// refusée dès la lecture de l'en-tête, avant toute allocation qui en dépend.
    #[test]
    fn malformed_headers_are_rejected() {
        let cases: &[(&[u8], &[u8], &str, &str)] = &[
            // (champ ciblé ou "" pour la table, balise, valeur, message attendu)
            (b"<FieldName>b</FieldName>", b"<Offset>", "SECTION_END", "hors de la section"),
            (b"<FieldName>b</FieldName>", b"<NoOfSymbols>", "35184372088832", "nombre de symboles"),
            (b"<FieldName>b</FieldName>", b"<BitWidth>", "1099511627776", "position de bits"),
            (b"", b"<RecordByteSize>", "1099511627776", "plus grandes que le fichier"),
            (b"", b"<NoOfRecords>", "18446744073709551615", "plus grandes que le fichier"),
        ];
        for (i, &(field, tag, value, expected)) in cases.iter().enumerate() {
            let path = bounds_test_file(&format!("qvdrs_bad_header_{i}.qvd"));
            let (_f, header, _bs) = read_header(&path, &local).unwrap();
            let value = if value == "SECTION_END" { header.offset.to_string() } else { value.to_string() };
            set_tag(&path, field, tag, &value);
            let err = read_schema(&path, &local).err().unwrap_or_else(|| panic!("cas {i} accepté")).to_string();
            assert!(err.contains(expected), "cas {i} : {err}");
        }
    }

    /// Fichier sans fin d'en-tête XML : erreur, sans lire tout le fichier.
    #[test]
    fn missing_header_terminator_is_an_error() {
        let path = std::env::temp_dir().join("qvdrs_no_header.qvd");
        std::fs::write(&path, b"<QvdTableHeader>pas de NUL").unwrap();
        let err = read_schema(path.to_str().unwrap(), &local).err().unwrap().to_string();
        assert!(err.contains("en-tête QVD introuvable"), "{err}");
    }

    /// Table de symboles illisible (texte tronqué) : erreur, pas une colonne NULL.
    #[test]
    fn corrupt_symbol_table_is_an_error() {
        let path = bounds_test_file("qvdrs_bad_symbols.qvd");
        let (_f, header, bs) = read_header(&path, &local).unwrap();
        let a = header.fields.iter().find(|f| f.field_name == "a").unwrap();
        // Remplace le NUL final du premier symbole texte par un octet non-UTF-8.
        let mut bytes = std::fs::read(&path).unwrap();
        let first = bs as usize + a.offset;
        let nul = first + find(&bytes[first..], b"\0");
        bytes[nul - 1] = 0xFF;
        std::fs::write(&path, bytes).unwrap();
        let s = read_schema(&path, &local).unwrap();
        let res = QvdScan::new(std::slice::from_ref(&path), &s.names, &s.kinds, &[0]).and_then(|scan| {
            while !matches!(scan.pull(&local)?, Pull::Done) {}
            Ok(())
        });
        assert!(res.unwrap_err().to_string().contains("table de symboles de 'a'"));
    }

    /// `$date` avec une heure (fraction) : jour tronqué, pas arrondi. Champ sans tag
    /// au format TIME : colonne `TIME`, pas `TIMESTAMP`.
    #[test]
    fn date_is_truncated_and_untagged_time_is_time() {
        let mut t = Column::new("t", vec![Some(Value::Float(0.75))]);
        t.number_format.r#type = "TIME".to_string();
        let cols = vec![tagged("d", vec![Some(Value::Float(45292.75))], &["$date"]), t];
        let path = write_temp("qvdrs_date_time.qvd", cols);
        let s = read_schema(&path, &local).unwrap();
        assert!(matches!(s.kinds[..], [Kind::Date, Kind::Time]));
        let (c, _) = collect(&[path], &s.names, &s.kinds, &[0, 1]);
        assert_eq!(c[0], TestCol::Date(vec![Some(19723)])); // 2024-01-01, pas le 02
        assert_eq!(c[1], TestCol::Time(vec![Some(64_800_000_000)])); // 18:00
    }

    /// QVD de test à deux champs (`a` texte, `b` entier), 1000 lignes.
    fn bounds_test_file(name: &str) -> String {
        let big: Vec<Option<Value>> = (0..1000).map(|i| Some(Value::Str(format!("val{i:04}")))).collect();
        let small: Vec<Option<Value>> = (0..1000).map(|i| Some(Value::Int(i % 2))).collect();
        write_temp(name, vec![tagged("a", big, &["$text"]), tagged("b", small, &["$integer"])])
    }

    /// Remplace la valeur de la première balise `tag` qui suit `after` (début de
    /// fichier si vide) dans l'en-tête XML.
    fn set_tag(path: &str, after: &[u8], tag: &[u8], value: &str) {
        let mut bytes = std::fs::read(path).unwrap();
        let from = if after.is_empty() { 0 } else { find(&bytes, after) };
        let start = from + find(&bytes[from..], tag) + tag.len();
        let end = start + find(&bytes[start..], b"<");
        bytes.splice(start..end, value.bytes());
        std::fs::write(path, bytes).unwrap();
    }

    fn find(hay: &[u8], needle: &[u8]) -> usize {
        hay.windows(needle.len()).position(|w| w == needle).unwrap()
    }

    /// Diagnostic sur les vrais fichiers de `QVD-Examples/` (non lancé par défaut).
    #[test]
    #[ignore]
    fn dump_real_qvd_files() {
        for entry in std::fs::read_dir("QVD-Examples").unwrap() {
            let path = entry.unwrap().path();
            if path.extension().and_then(|e| e.to_str()) != Some("qvd") {
                continue;
            }
            let p = path.to_str().unwrap();
            let (names, kinds, _cols, num_rows) = read_all(p);
            println!("\n===== {p} ({num_rows} lignes) =====");
            for (n, t) in names.iter().zip(&kinds) {
                println!("  {n}: {t:?}");
            }
        }
    }
}
