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
//! Le type DuckDB est déduit des **tags Qlik** de l'en-tête (signal fiable,
//! présent dans tout QVD produit par Qlik), avec repli sur la balise `<Type>` :
//!
//! | Tag Qlik (ou `<Type>` en repli) | Type DuckDB |
//! |---------------------------------|-------------|
//! | `$date`                         | `DATE`      |
//! | `$timestamp` (sans `$date`)     | `TIMESTAMP` |
//! | `$text` / `$ascii`              | `VARCHAR`   |
//! | `$integer`                      | `BIGINT`    |
//! | `$numeric` (sans `$integer`)    | `DOUBLE`    |
//!
//! Choisir le type à partir des seules métadonnées permet de typer au `bind`
//! sans décoder les données, ce qui rend possible la **projection pushdown** :
//! seules les colonnes réellement demandées sont décodées (via
//! `Qvd::from_path_projected`).
//!
//! Les séries temporelles Qlik (numéro de série, époque 1899-12-30) sont
//! converties en `DATE`/`TIMESTAMP` natif DuckDB.

use std::error::Error;

use duckdb::core::LogicalTypeId;
use openqvd::{FieldHeader, Qvd, Value};

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

/// Schéma d'un QVD : noms, catégories et types DuckDB de toutes les colonnes.
/// Lu au `bind` sans décoder les données.
pub(crate) struct Schema {
    pub names: Vec<String>,
    pub kinds: Vec<Kind>,
    pub type_ids: Vec<LogicalTypeId>,
}

/// Catégorie de colonne, déduite des tags Qlik. `Copy` pour être conservée
/// dans la `BindData` et réutilisée à l'`init`.
#[derive(Clone, Copy)]
pub(crate) enum Kind {
    Int,
    Float,
    Text,
    Date,
    Timestamp,
}

impl Kind {
    fn type_id(self) -> LogicalTypeId {
        match self {
            Kind::Int => LogicalTypeId::Bigint,
            Kind::Float => LogicalTypeId::Double,
            Kind::Text => LogicalTypeId::Varchar,
            Kind::Date => LogicalTypeId::Date,
            Kind::Timestamp => LogicalTypeId::Timestamp,
        }
    }
}

/// Décalage entre l'époque des numéros de série Qlik (1899-12-30) et celle de
/// DuckDB (1970-01-01), en jours.
const QLIK_EPOCH_OFFSET_DAYS: f64 = 25569.0;
const MICROS_PER_DAY: f64 = 86_400_000_000.0;

/// Lit uniquement le schéma (en-tête) d'un QVD, sans décoder les données.
///
/// `from_path_projected(path, &[])` parse l'en-tête XML (donc tous les champs)
/// mais ne décode aucune colonne. Le type est déduit des tags Qlik.
pub(crate) fn read_schema(path: &str) -> Result<Schema, Box<dyn Error>> {
    let qvd = Qvd::from_path_projected(path, &[])?;
    let mut names = Vec::new();
    let mut kinds = Vec::new();
    let mut type_ids = Vec::new();
    for f in qvd.fields() {
        let kind = kind_of_field(f);
        names.push(f.name.clone());
        type_ids.push(kind.type_id());
        kinds.push(kind);
    }
    Ok(Schema { names, kinds, type_ids })
}

/// Décode et matérialise uniquement les colonnes désignées par `indices`
/// (positions dans le schéma de référence), dans cet ordre — projection
/// pushdown — pour **tous** les `paths` du glob, lignes concaténées.
///
/// `names`/`kinds` sont le schéma de référence (premier fichier) issu de
/// [`read_schema`]. Renvoie les colonnes (ordre de `indices`) et le total de
/// lignes sur l'ensemble des fichiers.
pub(crate) fn read_projected(
    paths: &[String],
    names: &[String],
    kinds: &[Kind],
    indices: &[usize],
) -> Result<(Vec<ColumnData>, usize), Box<dyn Error>> {
    // Colonnes demandées (nom + type), dérivées du schéma de référence.
    let needed_names: Vec<&str> = indices.iter().map(|&i| names[i].as_str()).collect();
    let needed_kinds: Vec<Kind> = indices.iter().map(|&i| kinds[i]).collect();

    // Accumulateur (une colonne vide par champ projeté) que l'on étend fichier
    // après fichier — c'est l'union (concaténation des lignes) du glob.
    let mut columns: Vec<ColumnData> = needed_kinds.iter().map(|&k| empty_column(k)).collect();
    let mut total = 0usize;
    for path in paths {
        let (cols, n) = read_one(path, &needed_names, &needed_kinds)?;
        for (dst, src) in columns.iter_mut().zip(cols) {
            append(dst, src);
        }
        total += n;
    }
    Ok((columns, total))
}

/// Lit les colonnes projetées d'UN fichier. Les champs sont résolus **par nom**
/// (robuste aux écarts d'ordre entre fichiers d'un glob) ; un champ absent du
/// fichier ressort entièrement `NULL`.
fn read_one(
    path: &str,
    needed_names: &[&str],
    needed_kinds: &[Kind],
) -> Result<(Vec<ColumnData>, usize), Box<dyn Error>> {
    let qvd = Qvd::from_path_projected(path, needed_names)?;
    let num_rows = qvd.num_rows() as usize;

    // Position de chaque champ demandé dans CE fichier (None si absent).
    let fields = qvd.fields();
    let positions: Vec<Option<usize>> = needed_names
        .iter()
        .map(|name| fields.iter().position(|f| f.name == *name))
        .collect();

    // `rows()` (non vérifié) : les colonnes non projetées ont une table de
    // symboles vide et ressortent en `None` — `checked_rows()` les rejetterait.
    let mut raw: Vec<Vec<Option<Value>>> = (0..needed_names.len()).map(|_| Vec::new()).collect();
    for row in qvd.rows() {
        for (k, pos) in positions.iter().enumerate() {
            raw[k].push(pos.and_then(|p| row.get(p).cloned().flatten()));
        }
    }

    let columns = raw
        .into_iter()
        .zip(needed_kinds)
        .map(|(col, &k)| materialize(col, k))
        .collect();
    Ok((columns, num_rows))
}

/// Colonne typée vide (pour amorcer l'accumulateur du glob).
fn empty_column(kind: Kind) -> ColumnData {
    match kind {
        Kind::Int => ColumnData::I64(Vec::new()),
        Kind::Float => ColumnData::F64(Vec::new()),
        Kind::Text => ColumnData::Utf8(Vec::new()),
        Kind::Date => ColumnData::Date(Vec::new()),
        Kind::Timestamp => ColumnData::Timestamp(Vec::new()),
    }
}

/// Concatène `src` à la fin de `dst` (mêmes `Kind` par construction).
fn append(dst: &mut ColumnData, src: ColumnData) {
    match (dst, src) {
        (ColumnData::I64(a), ColumnData::I64(b)) => a.extend(b),
        (ColumnData::F64(a), ColumnData::F64(b)) => a.extend(b),
        (ColumnData::Utf8(a), ColumnData::Utf8(b)) => a.extend(b),
        (ColumnData::Date(a), ColumnData::Date(b)) => a.extend(b),
        (ColumnData::Timestamp(a), ColumnData::Timestamp(b)) => a.extend(b),
        _ => unreachable!("types de colonnes cohérents entre fichiers du glob"),
    }
}

/// Convertit une colonne de `Value` bruts en colonne typée DuckDB.
fn materialize(col: Vec<Option<Value>>, kind: Kind) -> ColumnData {
    match kind {
        Kind::Int => ColumnData::I64(col.into_iter().map(|c| c.and_then(value_to_i64)).collect()),
        Kind::Float => ColumnData::F64(col.into_iter().map(|c| c.and_then(|v| value_as_f64(&v))).collect()),
        Kind::Text => ColumnData::Utf8(col.into_iter().map(|c| c.map(value_to_string)).collect()),
        Kind::Date => ColumnData::Date(col.into_iter().map(|c| c.and_then(|v| serial_to_days(&v))).collect()),
        Kind::Timestamp => {
            ColumnData::Timestamp(col.into_iter().map(|c| c.and_then(|v| serial_to_micros(&v))).collect())
        }
    }
}

/// Déduit la catégorie d'un champ à partir de ses **tags Qlik**, avec repli
/// sur la balise `<Type>` déclarée (les QVD produits par Qlik sont toujours
/// taggés ; le repli couvre les fichiers minimalistes).
fn kind_of_field(f: &FieldHeader) -> Kind {
    let has = |t: &str| f.tags.iter().any(|x| x == t);
    if has("$date") {
        return Kind::Date;
    }
    if has("$timestamp") {
        return Kind::Timestamp;
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
    match f.number_format_type() {
        "INTEGER" => Kind::Int,
        "REAL" | "FIX" | "MONEY" => Kind::Float,
        "DATE" => Kind::Date,
        "TIME" | "TIMESTAMP" => Kind::Timestamp,
        _ => Kind::Text, // UNKNOWN, ASCII… → texte (sans risque)
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

    /// Colonne avec ses tags Qlik (le typage est désormais piloté par les tags).
    fn tagged(name: &str, values: Vec<Option<Value>>, tags: &[&str]) -> Column {
        let mut c = Column::new(name, values);
        c.tags = tags.iter().map(|t| t.to_string()).collect();
        c
    }

    /// Données complètes d'un QVD (toutes colonnes) — pour les tests.
    struct QvdData {
        names: Vec<String>,
        type_ids: Vec<LogicalTypeId>,
        columns: Vec<ColumnData>,
        num_rows: usize,
    }

    /// Lit schéma + toutes les colonnes (projection = tous les champs).
    fn read_all(path: &str) -> QvdData {
        let s = read_schema(path).unwrap();
        let indices: Vec<usize> = (0..s.names.len()).collect();
        let paths = [path.to_string()];
        let (columns, num_rows) = read_projected(&paths, &s.names, &s.kinds, &indices).unwrap();
        QvdData { names: s.names, type_ids: s.type_ids, columns, num_rows }
    }

    fn write_temp(name: &str, cols: Vec<Column>) -> String {
        let bytes = WriteTable::new("demo", cols).unwrap().to_bytes().unwrap();
        let path = std::env::temp_dir().join(name);
        std::fs::write(&path, bytes).unwrap();
        path.to_str().unwrap().to_string()
    }

    /// Génère un QVD couvrant entiers, flottants, texte, NULL et un champ date
    /// (tag `$date`, série Qlik), puis le relit et vérifie typage et valeurs.
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

        let path = write_temp("openqvd_read_qvd_test.qvd", cols);
        let data = read_all(&path);

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

    /// `read_projected` ne renvoie que les colonnes demandées, dans l'ordre
    /// demandé (projection pushdown).
    #[test]
    fn projection_reads_only_requested_columns() {
        let cols = vec![
            tagged("a", vec![Some(Value::Int(1)), Some(Value::Int(2))], &["$integer"]),
            tagged("b", vec![Some(Value::Float(9.5)), Some(Value::Float(8.5))], &["$numeric"]),
            tagged("c", vec![Some(Value::Str("x".into())), Some(Value::Str("y".into()))], &["$text"]),
        ];
        let path = write_temp("openqvd_projection_test.qvd", cols);

        let s = read_schema(&path).unwrap();
        assert_eq!(s.names, ["a", "b", "c"]);

        // Projeter c puis a (ordre inversé, b exclue).
        let paths = [path];
        let (columns, num_rows) = read_projected(&paths, &s.names, &s.kinds, &[2, 0]).unwrap();
        assert_eq!(num_rows, 2);
        assert_eq!(columns.len(), 2);
        match &columns[0] {
            ColumnData::Utf8(v) => assert_eq!(v, &[Some("x".to_string()), Some("y".to_string())]),
            _ => panic!("position 0 devrait être la colonne c (VARCHAR)"),
        }
        match &columns[1] {
            ColumnData::I64(v) => assert_eq!(v, &[Some(1), Some(2)]),
            _ => panic!("position 1 devrait être la colonne a (BIGINT)"),
        }
    }

    /// Plusieurs fichiers (glob) : les lignes sont concaténées, et un champ
    /// résolu par nom même si l'ordre des colonnes diffère entre fichiers.
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
        let p1 = write_temp("openqvd_multi_1.qvd", f1);
        let p2 = write_temp("openqvd_multi_2.qvd", f2);

        // Schéma de référence = premier fichier (id, name).
        let s = read_schema(&p1).unwrap();
        let indices: Vec<usize> = (0..s.names.len()).collect();
        let paths = [p1, p2];
        let (columns, n) = read_projected(&paths, &s.names, &s.kinds, &indices).unwrap();

        assert_eq!(n, 3);
        match &columns[0] {
            ColumnData::I64(v) => assert_eq!(v, &[Some(1), Some(2), Some(3)]),
            _ => panic!("id devrait être I64"),
        }
        match &columns[1] {
            ColumnData::Utf8(v) => assert_eq!(
                v,
                &[Some("a".to_string()), Some("b".to_string()), Some("c".to_string())]
            ),
            _ => panic!("name devrait être Utf8"),
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
            let data = read_all(p);
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
