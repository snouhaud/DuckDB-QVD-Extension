//! Lecture du format QVD via le crate `qvdrs`, en **streaming dictionnaire**.
//!
//! Le QVD est un format colonne dictionnaire-encodé : chaque colonne a une table
//! de **symboles** (valeurs distinctes) et un bloc d'**index** vers ces symboles.
//! On ouvre le fichier en flux (`open_qvd_stream`) — les tables de symboles sont
//! décodées une fois, l'index est lu par chunks (`next_chunk_indices`, ~2048
//! lignes). Chaque symbole projeté n'est **converti qu'une seule fois** par fichier
//! (le dictionnaire de `Cell`) ; les lignes sont ensuite émises par simple lookup
//! d'index. Mémoire bornée (≈ tables de symboles + un chunk d'index), et on évite
//! de reconvertir chaque ligne (gain net sur colonnes à faible cardinalité).
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

use duckdb::core::LogicalTypeId;
use qvdrs::header::QvdFieldHeader;
use qvdrs::{open_qvd_stream, QvdSymbol, QvdValue};

/// Reader streaming concret (un fichier ouvert).
type Reader = qvdrs::QvdStreamReader<std::io::BufReader<std::fs::File>>;

/// Nombre max de lignes décodées par chunk.
const VECTOR_SIZE: usize = 2048;

/// Représentation physique d'un `INTERVAL` DuckDB (`duckdb_interval`).
#[repr(C)]
#[derive(Clone, Copy)]
pub(crate) struct IntervalVal {
    pub months: i32,
    pub days: i32,
    pub micros: i64,
}

/// Schéma d'un QVD : noms, catégories et types DuckDB de toutes les colonnes.
/// Lu au `bind` sans décoder les données.
pub(crate) struct Schema {
    pub names: Vec<String>,
    pub kinds: Vec<Kind>,
    pub type_ids: Vec<LogicalTypeId>,
}

/// Catégorie de colonne, déduite des tags Qlik.
#[derive(Clone, Copy)]
pub(crate) enum Kind {
    Int,
    Float,
    Text,
    Date,
    Timestamp,
    Time,
    Interval,
}

impl Kind {
    fn type_id(self) -> LogicalTypeId {
        match self {
            Kind::Int => LogicalTypeId::Bigint,
            Kind::Float => LogicalTypeId::Double,
            Kind::Text => LogicalTypeId::Varchar,
            Kind::Date => LogicalTypeId::Date,
            Kind::Timestamp => LogicalTypeId::Timestamp,
            Kind::Time => LogicalTypeId::Time,
            Kind::Interval => LogicalTypeId::Interval,
        }
    }
}

/// Valeur convertie, prête à écrire dans un vecteur DuckDB. Mutualise la logique
/// de conversion entre `lib.rs` (FlatVector) et `copy_from.rs` (FFI brut).
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

/// Lit uniquement le schéma (en-tête) d'un QVD, sans décoder l'index.
pub(crate) fn read_schema(path: &str) -> Result<Schema, Box<dyn Error>> {
    let reader = open_qvd_stream(path)?;
    let mut names = Vec::new();
    let mut kinds = Vec::new();
    let mut type_ids = Vec::new();
    for f in &reader.header.fields {
        let kind = kind_of_field(f);
        names.push(f.field_name.clone());
        type_ids.push(kind.type_id());
        kinds.push(kind);
    }
    Ok(Schema { names, kinds, type_ids })
}

/// État mutable d'un scan (un fichier ouvert à la fois pour le glob).
struct ScanState {
    file_idx: usize,
    reader: Option<Reader>,
    positions: Vec<Option<usize>>, // colonne projetée -> index du champ dans le fichier courant
    dicts: Vec<Vec<Cell>>,         // colonne projetée -> symboles convertis (1× par fichier)
    countstar_emitted: usize,
}

/// Un paquet de lignes tiré du scan.
pub(crate) enum Pull {
    /// `COUNT(*)` : `n` lignes sans aucune colonne.
    Rows(usize),
    /// Données : colonnes projetées déjà converties (un chunk, possédées).
    Cells { columns: Vec<Vec<Cell>> },
    /// Scan terminé.
    Done,
}

/// Scan streaming d'un (ou plusieurs, glob) fichier(s) QVD. Sert de `InitData`.
pub(crate) struct QvdScan {
    paths: Vec<String>,
    needed_names: Vec<String>,
    needed_kinds: Vec<Kind>,
    num_rows: usize,
    state: Mutex<ScanState>,
}

impl QvdScan {
    /// Prépare le scan (aucun décodage de l'index ici). `num_rows` est lu dans
    /// les en-têtes (pour `COUNT(*)`).
    pub(crate) fn new(
        paths: &[String],
        names: &[String],
        kinds: &[Kind],
        indices: &[usize],
    ) -> Result<Self, Box<dyn Error>> {
        let needed_names = indices.iter().map(|&i| names[i].clone()).collect();
        let needed_kinds = indices.iter().map(|&i| kinds[i]).collect();
        let mut num_rows = 0usize;
        for p in paths {
            num_rows += open_qvd_stream(p)?.header.no_of_records;
        }
        Ok(QvdScan {
            paths: paths.to_vec(),
            needed_names,
            needed_kinds,
            num_rows,
            state: Mutex::new(ScanState {
                file_idx: 0,
                reader: None,
                positions: Vec::new(),
                dicts: Vec::new(),
                countstar_emitted: 0,
            }),
        })
    }

    /// Types de sortie (ordre des colonnes émises).
    pub(crate) fn output_kinds(&self) -> &[Kind] {
        &self.needed_kinds
    }

    /// Tire le prochain paquet de lignes (un chunk d'un fichier), en traversant
    /// les fichiers du glob. À l'ouverture d'un fichier, on convertit chaque
    /// symbole une fois (le dictionnaire) ; chaque chunk n'est plus qu'un lookup
    /// d'index. L'état avance sous verrou ; les données rendues sont possédées.
    pub(crate) fn pull(&self) -> Result<Pull, Box<dyn Error>> {
        let mut st = self.state.lock().unwrap();

        // COUNT(*) : 0 colonne projetée → ne rien décoder.
        if self.needed_kinds.is_empty() {
            let n = (self.num_rows - st.countstar_emitted).min(VECTOR_SIZE);
            if n == 0 {
                return Ok(Pull::Done);
            }
            st.countstar_emitted += n;
            return Ok(Pull::Rows(n));
        }

        loop {
            if st.reader.is_none() {
                if st.file_idx >= self.paths.len() {
                    return Ok(Pull::Done);
                }
                let reader = open_qvd_stream(&self.paths[st.file_idx])?;
                // Résolution par nom dans le fichier courant (None si absent → NULL).
                let positions: Vec<Option<usize>> = self
                    .needed_names
                    .iter()
                    .map(|nm| reader.header.fields.iter().position(|f| &f.field_name == nm))
                    .collect();
                // Dictionnaire : convertir chaque symbole une seule fois par colonne.
                let dicts: Vec<Vec<Cell>> = positions
                    .iter()
                    .enumerate()
                    .map(|(j, p)| match p {
                        Some(fi) => reader.symbols[*fi]
                            .iter()
                            .map(|s| convert(self.needed_kinds[j], &QvdValue::Symbol(s.clone())))
                            .collect(),
                        None => Vec::new(),
                    })
                    .collect();
                st.positions = positions;
                st.dicts = dicts;
                st.reader = Some(reader);
            }

            // Index bruts du chunk (sans résolution des symboles) pour toutes les
            // colonnes du fichier ; on ne matérialise que les colonnes projetées.
            let next = st.reader.as_mut().unwrap().next_chunk_indices(VECTOR_SIZE)?;
            match next {
                Some((cols, n, _start)) if n > 0 => {
                    let columns: Vec<Vec<Cell>> = (0..self.needed_kinds.len())
                        .map(|j| match st.positions[j] {
                            Some(fi) => {
                                let dict = &st.dicts[j];
                                cols[fi]
                                    .iter()
                                    .map(|&idx| {
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
                    return Ok(Pull::Cells { columns });
                }
                _ => {
                    // Fichier épuisé → suivant (drop le reader + son dictionnaire).
                    st.reader = None;
                    st.file_idx += 1;
                }
            }
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
        "TIME" | "TIMESTAMP" => Kind::Timestamp,
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

/// Numéro de série Qlik → jours depuis l'époque DuckDB (pour `DATE`).
fn serial_to_days(v: &QvdValue) -> Option<i32> {
    val_to_f64(v).map(|s| (s - QLIK_EPOCH_OFFSET_DAYS).round() as i32)
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
            match scan.pull().unwrap() {
                Pull::Done => break,
                Pull::Rows(n) => total += n,
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

    /// Lit schéma + toutes les colonnes (projection = tous les champs).
    fn read_all(path: &str) -> (Vec<String>, Vec<LogicalTypeId>, Vec<TestCol>, usize) {
        let s = read_schema(path).unwrap();
        let indices: Vec<usize> = (0..s.names.len()).collect();
        let (cols, n) = collect(&[path.to_string()], &s.names, &s.kinds, &indices);
        (s.names, s.type_ids, cols, n)
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
        let (names, type_ids, columns, num_rows) = read_all(&path);

        assert_eq!(num_rows, 3);
        assert_eq!(names, ["id", "price", "name", "qty", "day"]);
        assert!(matches!(type_ids[0], LogicalTypeId::Bigint));
        assert!(matches!(type_ids[1], LogicalTypeId::Double));
        assert!(matches!(type_ids[2], LogicalTypeId::Varchar));
        assert!(matches!(type_ids[3], LogicalTypeId::Bigint));
        assert!(matches!(type_ids[4], LogicalTypeId::Date));

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

        let s = read_schema(&path).unwrap();
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

        let s = read_schema(&p1).unwrap();
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
        let (_, type_ids, columns, _) = read_all(&path);

        assert!(matches!(type_ids[0], LogicalTypeId::Time));
        assert!(matches!(type_ids[1], LogicalTypeId::Interval));

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
            let (names, type_ids, _cols, num_rows) = read_all(p);
            println!("\n===== {p} ({num_rows} lignes) =====");
            for (n, t) in names.iter().zip(&type_ids) {
                println!("  {n}: {t:?}");
            }
        }
    }
}
