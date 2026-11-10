//! Écriture QVD : `COPY (SELECT …) TO 'fichier.qvd' (FORMAT qvd)`.
//!
//! Copy function de la C API v2 (`duckdb-neo`) : `bind` (noms + types des
//! colonnes) → `init` (chemin + encodeurs) → `batch`/`flush` (lots convertis,
//! puis versés dans l'ordre dans un dictionnaire par colonne) → `finalize`
//! (en-tête, symboles et lignes empaquetées, écrits par blocs via le système
//! de fichiers virtuel de DuckDB : local ou distant). En mémoire : les valeurs
//! distinctes et 4 octets par cellule, pas les lignes elles-mêmes.
//!
//! Les noms de colonnes viennent de la requête ; l'option `FIELD_NAMES` permet
//! de les remplacer. Les types sont taggés pour que [`crate::qvd`] les relise
//! correctement.

use std::collections::HashMap;
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
use openqvd::Value;

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

    /// `<NumberFormat>` (type, masque) des séries temporelles : sans lui, Qlik
    /// affiche le numéro de série (ex. 39322) au lieu d'une date.
    fn number_format(self) -> Option<(&'static str, &'static str)> {
        match self {
            WriteKind::Date => Some(("DATE", "YYYY-MM-DD")),
            WriteKind::Timestamp => Some(("TIMESTAMP", "YYYY-MM-DD hh:mm:ss")),
            WriteKind::Time => Some(("TIME", "hh:mm:ss")),
            _ => None,
        }
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

/// État d'écriture : chemin de sortie + encodeur de chaque colonne.
pub(crate) struct CopyState {
    path: String,
    columns: Mutex<Vec<Encoder>>,
}

/// Index d'une ligne NULL dans [`Encoder::rows`].
const NULL: u32 = u32::MAX;

/// Colonne en cours d'encodage : symboles distincts (encodés au format QVD,
/// numérotés par ordre de première occurrence, comme OpenQVD) et index du
/// symbole de chaque ligne. Seules les valeurs distinctes sont gardées, plus
/// 4 octets par cellule.
// ponytail: index des lignes en mémoire (4 o/cellule, ~0,2 Go pour 50 M de
// cellules) ; le passer par un fichier temporaire si les tables grossissent encore.
#[derive(Default)]
struct Encoder {
    index_of: HashMap<Box<[u8]>, u32>,
    rows: Vec<u32>,
    has_null: bool,
}

impl Encoder {
    /// Ajoute une ligne ; `key` est un tampon réutilisé d'un appel à l'autre.
    fn push(&mut self, value: Option<Value>, key: &mut Vec<u8>) -> Result<()> {
        let Some(value) = value else {
            self.has_null = true;
            self.rows.push(NULL);
            return Ok(());
        };
        key.clear();
        encode_symbol(&value, key)?;
        let i = match self.index_of.get(key.as_slice()) {
            Some(&i) => i,
            None => {
                let i = u32::try_from(self.index_of.len()).ok().filter(|&i| i != NULL).ok_or_else(|| {
                    Error::invalid_input("COPY (FORMAT qvd) : trop de valeurs distinctes".to_string())
                })?;
                self.index_of.insert(key.as_slice().into(), i);
                i
            }
        };
        self.rows.push(i);
        Ok(())
    }

    /// Biais Qlik : -2 si la colonne a des NULL (valeurs stockées 0 et 1 = NULL).
    fn bias(&self) -> i32 {
        if self.has_null {
            -2
        } else {
            0
        }
    }

    /// Nombre de bits par ligne (0 : colonne constante, sans NULL).
    fn bit_width(&self) -> u32 {
        let n = self.index_of.len() as u64;
        let max_stored = match (n, self.has_null) {
            (0 | 1, false) => return 0,
            (0, true) => 1,
            _ => n - 1 + (-self.bias()) as u64,
        };
        u64::BITS - max_stored.leading_zeros()
    }

    /// Valeur stockée pour la ligne `row` (index - biais ; NULL → 0).
    fn stored(&self, row: usize) -> u64 {
        match self.rows[row] {
            NULL => 0,
            i => i as u64 + (-self.bias()) as u64,
        }
    }

    /// Table des symboles, dans l'ordre des index.
    fn symbols(&self) -> Vec<&[u8]> {
        let mut out = vec![&[][..]; self.index_of.len()];
        for (k, &i) in &self.index_of {
            out[i as usize] = k;
        }
        out
    }
}

/// Encode un symbole (octet de type + valeur), comme OpenQVD.
fn encode_symbol(v: &Value, out: &mut Vec<u8>) -> Result<()> {
    let text = |out: &mut Vec<u8>, s: &str| {
        if s.as_bytes().contains(&0) {
            return Err(Error::invalid_input(
                "COPY (FORMAT qvd) : une chaîne contient l'octet NUL, réservé en fin de symbole".to_string(),
            ));
        }
        out.extend_from_slice(s.as_bytes());
        out.push(0);
        Ok(())
    };
    match v {
        Value::Int(i) => {
            out.push(0x01);
            out.extend_from_slice(&i.to_le_bytes());
        }
        Value::Float(f) => {
            out.push(0x02);
            out.extend_from_slice(&f.to_le_bytes());
        }
        Value::Str(s) => {
            out.push(0x04);
            text(out, s)?;
        }
        Value::DualInt(d) => {
            out.push(0x05);
            out.extend_from_slice(&d.number.to_le_bytes());
            text(out, &d.text)?;
        }
        Value::DualFloat(d) => {
            out.push(0x06);
            out.extend_from_slice(&d.number.to_le_bytes());
            text(out, &d.text)?;
        }
    }
    Ok(())
}

/// Écrit le QVD (en-tête XML, symboles, lignes empaquetées) par blocs via `write`.
/// L'en-tête reprend celui d'OpenQVD, plus `<QvBuildNo>` : sans lui, Qlik Sense
/// charge **0 ligne, sans erreur** (vérifié sur Qlik Sense client-managed).
fn write_qvd(
    table: &str,
    names: &[String],
    kinds: &[WriteKind],
    cols: &[Encoder],
    mut write: impl FnMut(&[u8]) -> Result<()>,
) -> Result<()> {
    let overflow = || Error::invalid_input("COPY (FORMAT qvd) : fichier trop gros pour le format QVD".to_string());
    let n_rows = cols.first().map_or(0, |c| c.rows.len());
    let widths: Vec<u32> = cols.iter().map(Encoder::bit_width).collect();
    let symbols: Vec<Vec<&[u8]>> = cols.iter().map(Encoder::symbols).collect();
    let lengths: Vec<u32> = symbols
        .iter()
        .map(|s| u32::try_from(s.iter().map(|b| b.len()).sum::<usize>()).map_err(|_| overflow()))
        .collect::<Result<_>>()?;
    let record_bits: u32 = widths.iter().sum();
    let record_bytes = record_bits.div_ceil(8);
    let body: u32 = lengths.iter().try_fold(0u32, |a, &l| a.checked_add(l)).ok_or_else(overflow)?;
    let rows_len = u32::try_from(n_rows).ok().and_then(|n| n.checked_mul(record_bytes)).ok_or_else(overflow)?;

    let mut h = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\r\n<QvdTableHeader>\r\n");
    h += "  <QvBuildNo>50699</QvBuildNo>\r\n";
    h += &format!("  <TableName>{}</TableName>\r\n  <Fields>\r\n", xml_escape(table));
    let (mut bit_offset, mut offset) = (0, 0);
    for (j, (name, kind)) in names.iter().zip(kinds).enumerate() {
        let (t, fmt) = kind.number_format().unwrap_or(("UNKNOWN", ""));
        h += &format!(
            "    <QvdFieldHeader>\r\n      <FieldName>{}</FieldName>\r\n      <BitOffset>{bit_offset}</BitOffset>\r\n      \
             <BitWidth>{}</BitWidth>\r\n      <Bias>{}</Bias>\r\n      <NumberFormat>\r\n        <Type>{t}</Type>\r\n        \
             <nDec>0</nDec>\r\n        <UseThou>0</UseThou>\r\n        <Fmt>{fmt}</Fmt>\r\n        <Dec></Dec>\r\n        \
             <Thou></Thou>\r\n      </NumberFormat>\r\n      <NoOfSymbols>{}</NoOfSymbols>\r\n      <Offset>{offset}</Offset>\r\n      \
             <Length>{}</Length>\r\n      <Tags>\r\n",
            xml_escape(name),
            widths[j],
            cols[j].bias(),
            symbols[j].len(),
            lengths[j],
        );
        for tag in kind.tags() {
            h += &format!("        <String>{tag}</String>\r\n");
        }
        h += "      </Tags>\r\n    </QvdFieldHeader>\r\n";
        bit_offset += widths[j];
        offset += lengths[j];
    }
    h += &format!(
        "  </Fields>\r\n  <Compression></Compression>\r\n  <RecordByteSize>{record_bytes}</RecordByteSize>\r\n  \
         <NoOfRecords>{n_rows}</NoOfRecords>\r\n  <Offset>{body}</Offset>\r\n  <Length>{rows_len}</Length>\r\n</QvdTableHeader>\r\n\0"
    );

    const BLOCK: usize = 1 << 20;
    let mut buf = Vec::with_capacity(BLOCK + record_bytes as usize);
    buf.extend_from_slice(h.as_bytes());
    for s in symbols.iter().flatten() {
        buf.extend_from_slice(s);
        if buf.len() >= BLOCK {
            write(&buf)?;
            buf.clear();
        }
    }
    let rbs = record_bytes as usize;
    for row in 0..n_rows {
        let start = buf.len();
        buf.resize(start + rbs, 0);
        let mut pos = 0;
        for (c, &w) in cols.iter().zip(&widths) {
            write_bits(&mut buf[start..], pos, w, c.stored(row));
            pos += w;
        }
        if buf.len() >= BLOCK {
            write(&buf)?;
            buf.clear();
        }
    }
    write(&buf)
}

/// Range les `width` bits de poids faible de `value` à partir du bit `pos`
/// (LSB d'abord, octets little-endian).
fn write_bits(buf: &mut [u8], mut pos: u32, mut width: u32, mut value: u64) {
    while width > 0 {
        let off = pos % 8;
        let take = (8 - off).min(width);
        buf[(pos / 8) as usize] |= ((value as u8) & ((1u16 << take) - 1) as u8) << off;
        value >>= take;
        pos += take;
        width -= take;
    }
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
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
        let columns = bind.kinds.iter().map(|_| Encoder::default()).collect();
        Ok(CopyState { path: file_path.to_string(), columns: Mutex::new(columns) })
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

    /// Les lots sont vidés dans l'ordre : leurs valeurs passent dans les
    /// dictionnaires (l'ordre des symboles, donc le fichier, est déterministe).
    fn flush(
        &self,
        _context: &Context,
        _bind: &Self::BindData,
        state: &Self::InitData,
        _input: &ColumnDataCollection<'_>,
        batch: &mut Self::BatchData,
    ) -> Result<()> {
        let mut columns = state.columns.lock().map_err(err)?;
        let mut key = Vec::new();
        for (dst, src) in columns.iter_mut().zip(batch.iter_mut()) {
            for v in src.drain(..) {
                dst.push(v, &mut key)?;
            }
        }
        Ok(())
    }

    fn finalize(&self, context: &Context, bind: &Self::BindData, state: &Self::InitData) -> Result<()> {
        let columns = std::mem::take(&mut *state.columns.lock().map_err(err)?);
        let table = std::path::Path::new(&state.path).file_stem().and_then(|s| s.to_str()).unwrap_or("qvd");
        let fs = FileSystem::new(context)?;
        let file = FileBuilder::new(&fs, &state.path)?.write().create_new().open()?;
        write_qvd(table, &bind.names, &bind.kinds, &columns, |mut rest| {
            while !rest.is_empty() {
                match file.write(rest)? {
                    0 => return Err(Error::invalid_input(format!("écriture interrompue : {}", state.path))),
                    n => rest = &rest[n..],
                }
            }
            Ok(())
        })?;
        file.close()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openqvd::{Column, Dual, WriteTable};

    /// Encode `cols` avec [`write_qvd`].
    fn ours(cols: &[(WriteKind, Vec<Option<Value>>)]) -> Vec<u8> {
        let names: Vec<String> = (0..cols.len()).map(|j| format!("c{j}")).collect();
        let kinds: Vec<WriteKind> = cols.iter().map(|c| c.0).collect();
        let mut key = Vec::new();
        let encs: Vec<Encoder> = cols
            .iter()
            .map(|(_, vals)| {
                let mut e = Encoder::default();
                vals.iter().for_each(|v| e.push(v.clone(), &mut key).unwrap());
                e
            })
            .collect();
        let mut out = Vec::new();
        write_qvd("t", &names, &kinds, &encs, |b| Ok(out.extend_from_slice(b))).unwrap();
        out
    }

    /// Même table par OpenQVD (l'ancien chemin d'écriture), `<QvBuildNo>` inséré.
    fn openqvd(cols: &[(WriteKind, Vec<Option<Value>>)]) -> Vec<u8> {
        let cols = cols
            .iter()
            .enumerate()
            .map(|(j, (kind, vals))| {
                let mut col = Column::new(format!("c{j}"), vals.clone());
                col.tags = kind.tags();
                if let Some((t, f)) = kind.number_format() {
                    col.number_format.r#type = t.to_string();
                    col.number_format.fmt = f.to_string();
                }
                col
            })
            .collect();
        let bytes = WriteTable::new("t", cols).unwrap().to_bytes().unwrap();
        const OPEN: &[u8] = b"<QvdTableHeader>\r\n";
        let at = bytes.windows(OPEN.len()).position(|w| w == OPEN).unwrap() + OPEN.len();
        [&bytes[..at], b"  <QvBuildNo>50699</QvBuildNo>\r\n", &bytes[at..]].concat()
    }

    #[test]
    fn identique_a_openqvd() {
        let s = |x: &str| Some(Value::Str(x.to_string()));
        let wide: Vec<Option<Value>> = (0..70_000).map(|i| Some(Value::Int(i % 40_000))).collect();
        let cases: Vec<Vec<(WriteKind, Vec<Option<Value>>)>> = vec![
            vec![(WriteKind::Date, vec![Some(Value::Int(45000)), None])],
            vec![
                (WriteKind::Str, vec![s("a<&>'\""), s("b"), s("a<&>'\""), None, s("é")]),
                (WriteKind::I32, vec![Some(Value::Int(1)); 5]),
                (WriteKind::F64, vec![Some(Value::Float(1.5)), None, None, Some(Value::Float(-0.0)), Some(Value::Float(2.0))]),
                (WriteKind::Timestamp, vec![None; 5]),
                (
                    WriteKind::Str,
                    vec![
                        Some(Value::DualInt(Dual { number: 1, text: "1".into() })),
                        Some(Value::DualFloat(Dual { number: 1.5, text: "1,5".into() })),
                        s("x"),
                        s("x"),
                        s("y"),
                    ],
                ),
            ],
            // > 64 bits par ligne et une colonne de 16 bits
            (0..5).map(|_| (WriteKind::I32, wide.clone())).collect(),
            vec![(WriteKind::Str, vec![])],
        ];
        for cols in &cases {
            assert!(ours(cols) == openqvd(cols), "écart avec OpenQVD pour {} colonne(s)", cols.len());
        }
        let q = openqvd::Qvd::from_bytes(ours(&cases[0])).unwrap();
        assert_eq!(q.header().build_no.as_deref(), Some("50699"));
        assert_eq!(q.fields()[0].number_format_type(), "DATE");
    }

    #[test]
    fn nul_refuse() {
        let mut e = Encoder::default();
        assert!(e.push(Some(Value::Str("a\0b".into())), &mut Vec::new()).is_err());
    }
}
