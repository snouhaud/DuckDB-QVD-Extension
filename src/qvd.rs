//! Intégration du format QVD via le crate Rust OpenQVD.
//!
//! Ce module isole tout ce qui touche au format Qlik QVD pour garder `lib.rs`
//! concentré sur la plomberie DuckDB. Il lit un fichier QVD en mémoire avec
//! OpenQVD, déduit un type DuckDB par champ, et matérialise les données dans
//! des colonnes typées prêtes à être streamées vers DuckDB.
//!
//! # Stratégie d'intégration
//!
//! OpenQVD expose une sortie Arrow (feature `arrow`), mais son point d'entrée
//! n'est pas visible sur docs.rs (feature non buildée). On s'appuie donc sur
//! l'**API décodée**, entièrement documentée et stable :
//! `Qvd::from_path`, `fields()`, `checked_rows()` et l'enum [`Value`].
//!
//! # Correspondance des types
//!
//! Au niveau mémoire un QVD ne connaît que 5 types ([`Value`]). On en déduit
//! un type DuckDB par colonne en inspectant les valeurs réellement décodées :
//!
//! | Contenu de la colonne                    | Type DuckDB |
//! |------------------------------------------|-------------|
//! | uniquement entiers                       | `BIGINT`    |
//! | au moins un flottant, aucun texte        | `DOUBLE`    |
//! | au moins une chaîne                      | `VARCHAR`   |
//! | type déclaré DATE/TIME/TIMESTAMP/INTERVAL| `VARCHAR`*  |
//!
//! \* Les champs temporels sont exposés via le **texte « dual »** rendu par
//! Qlik (fidèle et lisible). Le mapping vers les types temporels natifs de
//! DuckDB (conversion du numéro de série Qlik) est une amélioration future.

use std::error::Error;

use duckdb::core::LogicalTypeId;
use openqvd::{Qvd, Value};

/// Une colonne entièrement matérialisée et typée pour DuckDB.
///
/// `None` représente une valeur SQL `NULL`.
pub(crate) enum ColumnData {
    I64(Vec<Option<i64>>),
    F64(Vec<Option<f64>>),
    Utf8(Vec<Option<String>>),
    /// Jours depuis l'époque DuckDB (1970-01-01) — type DuckDB `DATE`.
    Date(Vec<Option<i32>>),
    /// Microsecondes depuis 1970-01-01 — type DuckDB `TIMESTAMP`.
    Timestamp(Vec<Option<i64>>),
}

/// Résultat de la lecture d'un fichier QVD, prêt pour `bind`/`func`.
pub(crate) struct QvdData {
    /// Noms des champs, dans l'ordre des colonnes.
    pub names: Vec<String>,
    /// Type DuckDB de chaque colonne (même ordre que `names`).
    pub type_ids: Vec<LogicalTypeId>,
    /// Données matérialisées, une entrée par colonne.
    pub columns: Vec<ColumnData>,
    /// Nombre de lignes (identique pour toutes les colonnes).
    pub num_rows: usize,
}

/// Catégorie de colonne déduite des valeurs décodées.
enum Kind {
    Int,
    Float,
    Text,
    Date,
    Timestamp,
}

/// Décalage entre l'époque des numéros de série Qlik (1899-12-30) et celle de
/// DuckDB (1970-01-01), en jours.
const QLIK_EPOCH_OFFSET_DAYS: f64 = 25569.0;
const MICROS_PER_DAY: f64 = 86_400_000_000.0;

/// Lit un fichier QVD et le matérialise en colonnes typées.
pub(crate) fn read_qvd(path: &str) -> Result<QvdData, Box<dyn Error>> {
    let qvd = Qvd::from_path(path)?;

    let names: Vec<String> = qvd.fields().iter().map(|f| f.name.clone()).collect();
    let n_fields = names.len();

    // Détection des champs temporels par les tags Qlik. C'est le signal fiable
    // (la balise <Type> vaut souvent UNKNOWN même pour de vraies dates).
    let temporal: Vec<bool> = qvd
        .fields()
        .iter()
        .map(|f| f.tags.iter().any(|t| t == "$date" || t == "$timestamp"))
        .collect();

    // Lecture row-major → accumulation colonne par colonne (Value bruts).
    let mut raw: Vec<Vec<Option<Value>>> =
        (0..n_fields).map(|_| Vec::new()).collect();
    for row in qvd.checked_rows() {
        let row = row?; // Vec<Option<Value>>, propage QvdError
        for (j, cell) in row.into_iter().enumerate() {
            if j < n_fields {
                raw[j].push(cell);
            }
        }
    }
    let num_rows = raw.first().map(|c| c.len()).unwrap_or(0);

    // Déduction du type + conversion par colonne.
    let mut type_ids = Vec::with_capacity(n_fields);
    let mut columns = Vec::with_capacity(n_fields);
    for (j, raw_col) in raw.into_iter().enumerate() {
        let kind = infer_kind(&raw_col, temporal[j]);
        let (type_id, column) = match kind {
            Kind::Int => (
                LogicalTypeId::Bigint,
                ColumnData::I64(raw_col.into_iter().map(|c| c.and_then(value_to_i64)).collect()),
            ),
            Kind::Float => (
                LogicalTypeId::Double,
                ColumnData::F64(raw_col.into_iter().map(|c| c.and_then(|v| value_as_f64(&v))).collect()),
            ),
            Kind::Text => (
                LogicalTypeId::Varchar,
                ColumnData::Utf8(raw_col.into_iter().map(|c| c.map(value_to_string)).collect()),
            ),
            Kind::Date => (
                LogicalTypeId::Date,
                ColumnData::Date(raw_col.into_iter().map(|c| c.and_then(|v| serial_to_days(&v))).collect()),
            ),
            Kind::Timestamp => (
                LogicalTypeId::Timestamp,
                ColumnData::Timestamp(raw_col.into_iter().map(|c| c.and_then(|v| serial_to_micros(&v))).collect()),
            ),
        };
        type_ids.push(type_id);
        columns.push(column);
    }

    Ok(QvdData { names, type_ids, columns, num_rows })
}

/// Déduit la catégorie d'une colonne à partir des valeurs réellement décodées.
///
/// `temporal` (issu des tags `$date`/`$timestamp`) prime : on choisit alors
/// `DATE` si toutes les valeurs tombent sur un jour entier, sinon `TIMESTAMP`.
fn infer_kind(col: &[Option<Value>], temporal: bool) -> Kind {
    if temporal {
        let has_time = col
            .iter()
            .flatten()
            .filter_map(value_as_f64)
            .any(|serial| serial.fract() != 0.0);
        return if has_time { Kind::Timestamp } else { Kind::Date };
    }
    let mut has_float = false;
    for cell in col.iter().flatten() {
        match cell {
            Value::Str(_) => return Kind::Text, // un texte suffit à basculer en VARCHAR
            Value::Float(_) | Value::DualFloat(_) => has_float = true,
            Value::Int(_) | Value::DualInt(_) => {}
        }
    }
    if has_float {
        Kind::Float
    } else {
        Kind::Int
    }
}

fn value_to_i64(v: Value) -> Option<i64> {
    match v {
        Value::Int(n) => Some(n as i64),
        Value::DualInt(d) => Some(d.number as i64),
        Value::Float(f) => Some(f as i64),
        Value::DualFloat(d) => Some(d.number as i64),
        Value::Str(s) => s.trim().parse().ok(),
    }
}

/// Valeur numérique d'une cellule (utilisée pour les flottants et les séries
/// temporelles Qlik).
fn value_as_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Int(n) => Some(*n as f64),
        Value::Float(f) => Some(*f),
        Value::DualInt(d) => Some(d.number as f64),
        Value::DualFloat(d) => Some(d.number),
        Value::Str(s) => s.trim().parse().ok(),
    }
}

/// Numéro de série Qlik → jours depuis l'époque DuckDB (pour `DATE`).
fn serial_to_days(v: &Value) -> Option<i32> {
    value_as_f64(v).map(|s| (s - QLIK_EPOCH_OFFSET_DAYS).round() as i32)
}

/// Numéro de série Qlik → microsecondes depuis l'époque DuckDB (pour `TIMESTAMP`).
fn serial_to_micros(v: &Value) -> Option<i64> {
    value_as_f64(v).map(|s| ((s - QLIK_EPOCH_OFFSET_DAYS) * MICROS_PER_DAY).round() as i64)
}

fn value_to_string(v: Value) -> String {
    match v {
        Value::Int(n) => n.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Str(s) => s,
        // Pour les duals, on privilégie le texte rendu par Qlik (dates, devises…).
        Value::DualInt(d) => d.text,
        Value::DualFloat(d) => d.text,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openqvd::{Column, WriteTable};

    /// Génère un QVD couvrant entiers, flottants, texte, NULL et un champ date
    /// (tag `$date`, série Qlik), puis le relit via `read_qvd` et vérifie le
    /// typage et les valeurs de bout en bout.
    #[test]
    fn read_qvd_infers_types_and_values() {
        // 45000/45001/45002 (série Qlik) → 19431/19432/19433 (jours DuckDB).
        let mut day = Column::new(
            "day",
            vec![
                Some(Value::Int(45000)),
                Some(Value::Int(45001)),
                Some(Value::Int(45002)),
            ],
        );
        day.tags = vec!["$date".to_string(), "$timestamp".to_string()];

        let cols = vec![
            Column::new("id", vec![Some(Value::Int(1)), Some(Value::Int(2)), Some(Value::Int(3))]),
            Column::new(
                "price",
                vec![Some(Value::Float(1.5)), Some(Value::Float(2.0)), Some(Value::Float(3.25))],
            ),
            Column::new(
                "name",
                vec![Some(Value::Str("a".into())), Some(Value::Str("b".into())), Some(Value::Str("a".into()))],
            ),
            Column::new("qty", vec![Some(Value::Int(10)), None, Some(Value::Int(30))]),
            day,
        ];

        let bytes = WriteTable::new("demo", cols).unwrap().to_bytes().unwrap();
        let path = std::env::temp_dir().join("openqvd_read_qvd_test.qvd");
        std::fs::write(&path, bytes).unwrap();

        let data = read_qvd(path.to_str().unwrap()).unwrap();

        assert_eq!(data.num_rows, 3);
        assert_eq!(data.names, ["id", "price", "name", "qty", "day"]);

        // Typage déduit.
        assert!(matches!(data.type_ids[0], LogicalTypeId::Bigint));
        assert!(matches!(data.type_ids[1], LogicalTypeId::Double));
        assert!(matches!(data.type_ids[2], LogicalTypeId::Varchar));
        assert!(matches!(data.type_ids[3], LogicalTypeId::Bigint));
        assert!(matches!(data.type_ids[4], LogicalTypeId::Date)); // série Qlik → DATE

        // Valeurs.
        match &data.columns[0] {
            ColumnData::I64(v) => assert_eq!(v, &[Some(1), Some(2), Some(3)]),
            _ => panic!("id devrait être I64"),
        }
        match &data.columns[1] {
            ColumnData::F64(v) => assert_eq!(v, &[Some(1.5), Some(2.0), Some(3.25)]),
            _ => panic!("price devrait être F64"),
        }
        match &data.columns[2] {
            ColumnData::Utf8(v) => {
                assert_eq!(v, &[Some("a".to_string()), Some("b".to_string()), Some("a".to_string())])
            }
            _ => panic!("name devrait être Utf8"),
        }
        match &data.columns[3] {
            ColumnData::I64(v) => assert_eq!(v, &[Some(10), None, Some(30)]), // NULL préservé
            _ => panic!("qty devrait être I64"),
        }
        match &data.columns[4] {
            ColumnData::Date(v) => assert_eq!(v, &[Some(19431), Some(19432), Some(19433)]),
            _ => panic!("day devrait être Date"),
        }
    }

    fn type_name(c: &ColumnData) -> &'static str {
        match c {
            ColumnData::I64(_) => "BIGINT",
            ColumnData::F64(_) => "DOUBLE",
            ColumnData::Utf8(_) => "VARCHAR",
            ColumnData::Date(_) => "DATE",
            ColumnData::Timestamp(_) => "TIMESTAMP",
        }
    }

    /// Convertit un nombre de jours depuis 1970-01-01 en `YYYY-MM-DD`
    /// (algorithme civil-from-days de H. Hinnant).
    fn days_to_iso(days: i32) -> String {
        let z = days as i64 + 719_468;
        let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
        let doe = z - era * 146_097;
        let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
        let y = yoe + era * 400;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let d = doy - (153 * mp + 2) / 5 + 1;
        let m = if mp < 10 { mp + 3 } else { mp - 9 };
        let y = if m <= 2 { y + 1 } else { y };
        format!("{y:04}-{m:02}-{d:02}")
    }

    fn cell(c: &ColumnData, i: usize) -> String {
        match c {
            ColumnData::I64(v) => v[i].map_or("NULL".into(), |x| x.to_string()),
            ColumnData::F64(v) => v[i].map_or("NULL".into(), |x| x.to_string()),
            ColumnData::Utf8(v) => v[i].clone().unwrap_or_else(|| "NULL".into()),
            ColumnData::Date(v) => v[i].map_or("NULL".into(), days_to_iso),
            ColumnData::Timestamp(v) => v[i].map_or("NULL".into(), |us| {
                let days = us.div_euclid(86_400_000_000) as i32;
                let rem = us.rem_euclid(86_400_000_000) / 1_000_000;
                format!("{} {:02}:{:02}:{:02}", days_to_iso(days), rem / 3600, (rem % 3600) / 60, rem % 60)
            }),
        }
    }

    /// Diagnostic sur les vrais fichiers de `QVD-Examples/` (non lancé par
    /// défaut) : `cargo test --lib -- --ignored --nocapture dump_real`.
    #[test]
    #[ignore]
    fn dump_real_qvd_files() {
        for entry in std::fs::read_dir("QVD-Examples").unwrap() {
            let path = entry.unwrap().path();
            if path.extension().and_then(|e| e.to_str()) != Some("qvd") {
                continue;
            }
            let p = path.to_str().unwrap();
            println!("\n===== {p} =====");
            let data = read_qvd(p).unwrap();
            println!("{} lignes, {} colonnes", data.num_rows, data.columns.len());
            for (name, col) in data.names.iter().zip(&data.columns) {
                println!("  {name}: {}", type_name(col));
            }
            let preview = data.num_rows.min(3);
            for i in 0..preview {
                let row: Vec<String> = data.columns.iter().map(|c| cell(c, i)).collect();
                println!("  row[{i}] = {}", row.join(" | "));
            }
        }
    }
}
