//! Écriture QVD : `COPY (SELECT …) TO 'fichier.qvd' (FORMAT qvd)`.
//!
//! Copy function de la C API v2 (`duckdb-neo`) : `bind` (noms + types des
//! colonnes) → `init` (chemin + tampons) → `batch`/`flush` (accumulation des
//! lots, dans l'ordre) → `finalize` (encodage OpenQVD, écrit via le système de
//! fichiers virtuel de DuckDB : local ou distant). Toutes les lignes sont
//! accumulées en mémoire avant l'écriture.
//!
//! Les noms de colonnes viennent de la requête ; l'option `FIELD_NAMES` permet
//! de les remplacer. Les types sont taggés pour que [`crate::qvd`] les relise
//! correctement.

use std::sync::Mutex;

use duckdb_neo::{
    column_data_collection::ColumnDataCollection,
    connection::Context,
    copy_function::{CopyToBindInfo, CopyToFunctionCallbacks},
    data_chunk::DataChunkRef,
    file::{FileBuilder, FileSystem},
    logical_type::{LogicalType, LogicalTypeID},
    types::{
        DateValue, DecimalSignature, HugeDecimal, IntervalValue, LongDecimal, ShortDecimal, TimeValue,
        TimestampValue, WordDecimal,
    },
    value::Value as DuckValue,
    vector::VectorElement,
    Error, Result,
};
use openqvd::{Column, Value, WriteTable};

use crate::err;
use crate::qvd::{MICROS_PER_DAY, QLIK_EPOCH_OFFSET_DAYS};

/// Largeur physique de stockage d'un `DECIMAL` (entier non-scalé).
#[derive(Clone, Copy)]
enum DecimalPhys {
    I16,
    I32,
    I64,
    I128,
}

/// Stratégie de lecture d'une colonne entrante (selon son type DuckDB).
#[derive(Clone, Copy)]
enum WriteKind {
    Bool,
    I8,
    I16,
    I32,
    I64,
    F32,
    F64,
    Str,
    Date,
    Timestamp,
    Time,
    Interval,
    /// `DECIMAL(w, s)` : entier non-scalé `phys` à diviser par 10^`scale`.
    Decimal { scale: u32, phys: DecimalPhys },
}

impl WriteKind {
    /// Déduit la stratégie d'un type logique (largeur/échelle pour `DECIMAL`).
    fn of_logical_type(lt: &LogicalType) -> Result<Option<Self>> {
        Ok(Some(match lt.id()? {
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN => WriteKind::Bool,
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_TINYINT => WriteKind::I8,
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_SMALLINT => WriteKind::I16,
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER => WriteKind::I32,
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT => WriteKind::I64,
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_FLOAT => WriteKind::F32,
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE => WriteKind::F64,
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR => WriteKind::Str,
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_DATE => WriteKind::Date,
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP => WriteKind::Timestamp,
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_TIME => WriteKind::Time,
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_INTERVAL => WriteKind::Interval,
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_DECIMAL => {
                let sig = DecimalSignature::from_logical_type(lt)?;
                let phys = match sig.width {
                    1..=4 => DecimalPhys::I16,
                    5..=9 => DecimalPhys::I32,
                    10..=18 => DecimalPhys::I64,
                    _ => DecimalPhys::I128,
                };
                WriteKind::Decimal { scale: sig.scale as u32, phys }
            }
            _ => return Ok(None),
        }))
    }

    /// Tags Qlik écrits dans l'en-tête (pour que la relecture re-type juste).
    fn tags(self) -> Vec<String> {
        let t: &[&str] = match self {
            WriteKind::Bool | WriteKind::I8 | WriteKind::I16 | WriteKind::I32 | WriteKind::I64 => {
                &["$numeric", "$integer"]
            }
            WriteKind::F32 | WriteKind::F64 | WriteKind::Decimal { .. } => &["$numeric"],
            WriteKind::Str => &["$text"],
            WriteKind::Date => &["$date"],
            WriteKind::Timestamp => &["$timestamp"],
            WriteKind::Time => &["$time"],
            WriteKind::Interval => &["$interval"],
        };
        t.iter().map(|s| s.to_string()).collect()
    }

    /// Lit la colonne `j` du chunk et ajoute ses valeurs converties à `out`.
    fn read(self, chunk: &DataChunkRef<'_>, j: usize, out: &mut Vec<Option<Value>>) -> Result<()> {
        match self {
            WriteKind::Bool => push::<bool>(chunk, j, out, |v| Value::Int(*v as i32)),
            WriteKind::I8 => push::<i8>(chunk, j, out, |v| Value::Int(*v as i32)),
            WriteKind::I16 => push::<i16>(chunk, j, out, |v| Value::Int(*v as i32)),
            WriteKind::I32 => push::<i32>(chunk, j, out, |v| Value::Int(*v)),
            WriteKind::I64 => push::<i64>(chunk, j, out, |v| match i32::try_from(*v) {
                Ok(i) => Value::Int(i),
                Err(_) => Value::Float(*v as f64), // au-delà d'i32 : QVD stocke en double
            }),
            WriteKind::F32 => push::<f32>(chunk, j, out, |v| Value::Float(*v as f64)),
            WriteKind::F64 => push::<f64>(chunk, j, out, |v| Value::Float(*v)),
            // jours DuckDB → série Qlik (en double au-delà d'i32, ex. 'infinity')
            WriteKind::Date => push::<DateValue>(chunk, j, out, |v| match v.0.checked_add(QLIK_EPOCH_OFFSET_DAYS as i32) {
                Some(serial) => Value::Int(serial),
                None => Value::Float(v.0 as f64 + QLIK_EPOCH_OFFSET_DAYS),
            }),
            WriteKind::Timestamp => push::<TimestampValue>(chunk, j, out, |v| {
                Value::Float(v.0 as f64 / MICROS_PER_DAY + QLIK_EPOCH_OFFSET_DAYS)
            }),
            // µs depuis minuit → fraction de jour Qlik.
            WriteKind::Time => push::<TimeValue>(chunk, j, out, |v| Value::Float(v.0 as f64 / MICROS_PER_DAY)),
            // INTERVAL → durée en jours (mois approximés à 30 jours, le format
            // QVD n'ayant pas de notion de mois).
            WriteKind::Interval => push::<IntervalValue>(chunk, j, out, |v| {
                Value::Float(v.months as f64 * 30.0 + v.days as f64 + v.micros as f64 / MICROS_PER_DAY)
            }),
            WriteKind::Decimal { scale, phys } => {
                let div = 10f64.powi(scale as i32);
                match phys {
                    DecimalPhys::I16 => push::<ShortDecimal>(chunk, j, out, |v| Value::Float(v.0 as f64 / div)),
                    DecimalPhys::I32 => push::<WordDecimal>(chunk, j, out, |v| Value::Float(v.0 as f64 / div)),
                    DecimalPhys::I64 => push::<LongDecimal>(chunk, j, out, |v| Value::Float(v.0 as f64 / div)),
                    DecimalPhys::I128 => push::<HugeDecimal>(chunk, j, out, |v| Value::Float(v.0 as f64 / div)),
                }
            }
            WriteKind::Str => {
                let reader = chunk.get_vector_at::<String>(j)?.reader()?;
                out.extend(reader.iter().map(|s| s.map(|s| Value::Str(s.to_string()))));
                Ok(())
            }
        }
    }
}

/// Lit un vecteur de type `T` et pousse chaque valeur convertie (`None` = NULL).
fn push<T>(chunk: &DataChunkRef<'_>, j: usize, out: &mut Vec<Option<Value>>, f: impl Fn(&T) -> Value) -> Result<()>
where
    T: VectorElement + 'static,
    for<'a> T: VectorElement<Ref<'a> = &'a T>,
{
    let reader = chunk.get_vector_at::<T>(j)?.reader()?;
    out.extend(reader.iter().map(|v| v.map(&f)));
    Ok(())
}

/// Données de bind : nom + type d'écriture de chaque colonne.
pub(crate) struct CopyBindData {
    names: Vec<String>,
    kinds: Vec<WriteKind>,
}

/// État d'écriture : chemin de sortie + valeurs accumulées par colonne.
pub(crate) struct CopyState {
    path: String,
    columns: Mutex<Vec<Vec<Option<Value>>>>,
}

/// Format `qvd` des copy functions (COPY TO ici, COPY FROM dans
/// [`crate::copy_from`]).
pub(crate) struct QvdFormat;

/// Collecte récursivement toutes les chaînes feuilles d'une valeur d'option
/// (gère `FIELD_NAMES (a, b, c)` comme `FIELD_NAMES ['a','b','c']`).
fn collect_strings(v: &DuckValue, out: &mut Vec<String>) -> Result<()> {
    if v.child_count()? > 0 {
        for child in v.children()? {
            collect_strings(&child, out)?;
        }
    } else if let Some(s) = v.get::<String>()? {
        out.push(s);
    }
    Ok(())
}

impl CopyToFunctionCallbacks for QvdFormat {
    type BindData = CopyBindData;
    type InitData = CopyState;
    type BatchData = Vec<Vec<Option<Value>>>;

    fn bind(&self, _context: &Context, info: &CopyToBindInfo) -> Result<Self::BindData> {
        let n = info.column_count()?;
        let mut names = Vec::with_capacity(n);
        let mut kinds = Vec::with_capacity(n);
        for i in 0..n {
            let (name, lt) = info.get_column(i)?;
            let kind = WriteKind::of_logical_type(&lt)?.ok_or_else(|| {
                Error::invalid_input(format!(
                    "COPY (FORMAT qvd) : type de colonne non supporté ({}) en écriture ; \
                     CAST vers BIGINT/DOUBLE/DECIMAL/VARCHAR/DATE/TIMESTAMP/TIME/INTERVAL/BOOLEAN",
                    lt.to_text().unwrap_or_default()
                ))
            })?;
            names.push(name.to_string());
            kinds.push(kind);
        }

        for (option, value) in info.options()? {
            if option.eq_ignore_ascii_case("field_names") {
                let mut custom = Vec::new();
                collect_strings(&value, &mut custom)?;
                if custom.len() != n {
                    return Err(Error::invalid_input(format!(
                        "COPY (FORMAT qvd) : FIELD_NAMES fournit {} noms pour {n} colonnes",
                        custom.len()
                    )));
                }
                names = custom;
            }
        }

        Ok(CopyBindData { names, kinds })
    }

    fn init(&self, _context: &Context, bind: &Self::BindData, file_path: &str) -> Result<Self::InitData> {
        Ok(CopyState { path: file_path.to_string(), columns: Mutex::new(vec![Vec::new(); bind.kinds.len()]) })
    }

    fn batch(
        &self,
        context: &Context,
        bind: &Self::BindData,
        _state: &Self::InitData,
        input: &ColumnDataCollection<'_>,
    ) -> Result<Self::BatchData> {
        let mut columns = vec![Vec::new(); bind.kinds.len()];
        let mut scan = input.to_scan(context)?;
        while let Some(chunk) = scan.next_chunk()? {
            for (j, kind) in bind.kinds.iter().enumerate() {
                kind.read(&chunk, j, &mut columns[j])?;
            }
        }
        Ok(columns)
    }

    /// Les lots sont vidés dans l'ordre : on les ajoute aux tampons.
    fn flush(
        &self,
        _context: &Context,
        _bind: &Self::BindData,
        state: &Self::InitData,
        _input: &ColumnDataCollection<'_>,
        batch: &mut Self::BatchData,
    ) -> Result<()> {
        let mut columns = state.columns.lock().map_err(err)?;
        for (dst, src) in columns.iter_mut().zip(batch.iter_mut()) {
            dst.append(src);
        }
        Ok(())
    }

    fn finalize(&self, context: &Context, bind: &Self::BindData, state: &Self::InitData) -> Result<()> {
        let columns = std::mem::take(&mut *state.columns.lock().map_err(err)?);
        let cols = bind
            .names
            .iter()
            .zip(&bind.kinds)
            .zip(columns)
            .map(|((name, kind), vals)| {
                let mut col = Column::new(name.clone(), vals);
                col.tags = kind.tags();
                col
            })
            .collect();
        let bytes = WriteTable::new("qvd", cols).map_err(err)?.to_bytes().map_err(err)?;

        let fs = FileSystem::new(context)?;
        let file = FileBuilder::new(&fs, &state.path)?.write().create_new().open()?;
        let mut rest = &bytes[..];
        while !rest.is_empty() {
            match file.write(rest)? {
                0 => return Err(Error::invalid_input(format!("écriture interrompue : {}", state.path))),
                n => rest = &rest[n..],
            }
        }
        file.close()
    }
}
