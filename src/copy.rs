//! Écriture QVD : `COPY (SELECT …) TO 'fichier.qvd' (FORMAT qvd)`.
//!
//! DuckDB-rs ne wrappe pas les *copy functions*, mais l'API C les expose
//! (`duckdb_create_copy_function` & co, accessibles via `duckdb::ffi`). Ce
//! module les pilote en FFI brut : `bind` (schéma) → `global_init` (chemin +
//! encodeurs) → `sink` (chunks versés dans un dictionnaire par colonne) →
//! `finalize` (en-tête, symboles et lignes empaquetées, écrits par blocs). En
//! mémoire : les valeurs distinctes et 4 octets par cellule.
//!
//! # Limitation connue
//!
//! L'API C de la copy function n'expose **pas les noms de colonnes** : les
//! champs sont nommés `field0`, `field1`, … (option `FIELD_NAMES` pour les
//! fixer). Les types, eux, sont préservés (entiers, flottants, texte, `DATE`,
//! `TIMESTAMP`, `TIME`, `INTERVAL` ; `DECIMAL` → double) et taggés pour que
//! [`crate::qvd`] les relise correctement.

use std::collections::HashMap;
use std::ffi::{c_void, CStr, CString};
use std::sync::Mutex;

use duckdb::ffi;
use openqvd::Value;

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
    /// Déduit la stratégie d'un type logique (nécessaire pour `DECIMAL`, dont
    /// la largeur/échelle dépendent du type, pas seulement de l'identifiant).
    unsafe fn of_logical_type(lt: ffi::duckdb_logical_type) -> Option<Self> {
        let tid = ffi::duckdb_get_type_id(lt);
        if tid == ffi::DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL {
            let scale = ffi::duckdb_decimal_scale(lt) as u32;
            let phys = match ffi::duckdb_decimal_internal_type(lt) {
                ffi::DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT => DecimalPhys::I16,
                ffi::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER => DecimalPhys::I32,
                ffi::DUCKDB_TYPE_DUCKDB_TYPE_BIGINT => DecimalPhys::I64,
                ffi::DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT => DecimalPhys::I128,
                _ => return None,
            };
            return Some(WriteKind::Decimal { scale, phys });
        }
        Self::from_type_id(tid)
    }

    /// Déduit la stratégie depuis l'identifiant de type DuckDB.
    fn from_type_id(tid: ffi::duckdb_type) -> Option<Self> {
        Some(match tid {
            ffi::DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN => WriteKind::Bool,
            ffi::DUCKDB_TYPE_DUCKDB_TYPE_TINYINT => WriteKind::I8,
            ffi::DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT => WriteKind::I16,
            ffi::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER => WriteKind::I32,
            ffi::DUCKDB_TYPE_DUCKDB_TYPE_BIGINT => WriteKind::I64,
            ffi::DUCKDB_TYPE_DUCKDB_TYPE_FLOAT => WriteKind::F32,
            ffi::DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE => WriteKind::F64,
            ffi::DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR => WriteKind::Str,
            ffi::DUCKDB_TYPE_DUCKDB_TYPE_DATE => WriteKind::Date,
            ffi::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP => WriteKind::Timestamp,
            ffi::DUCKDB_TYPE_DUCKDB_TYPE_TIME => WriteKind::Time,
            ffi::DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL => WriteKind::Interval,
            _ => return None,
        })
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

    /// Lit la valeur de la ligne `r` dans le tampon `data` du vecteur.
    unsafe fn read(self, data: *mut c_void, r: usize) -> Value {
        match self {
            WriteKind::Bool => Value::Int((*(data as *const u8).add(r) != 0) as i32),
            WriteKind::I8 => Value::Int(*(data as *const i8).add(r) as i32),
            WriteKind::I16 => Value::Int(*(data as *const i16).add(r) as i32),
            WriteKind::I32 => Value::Int(*(data as *const i32).add(r)),
            WriteKind::I64 => {
                let v = *(data as *const i64).add(r);
                if (i32::MIN as i64..=i32::MAX as i64).contains(&v) {
                    Value::Int(v as i32)
                } else {
                    Value::Float(v as f64) // au-delà d'i32 : QVD stocke en double
                }
            }
            WriteKind::F32 => Value::Float(*(data as *const f32).add(r) as f64),
            WriteKind::F64 => Value::Float(*(data as *const f64).add(r)),
            WriteKind::Date => {
                // jours DuckDB → série Qlik (en double au-delà d'i32, ex. 'infinity')
                let days = *(data as *const i32).add(r);
                match days.checked_add(QLIK_EPOCH_OFFSET_DAYS as i32) {
                    Some(serial) => Value::Int(serial),
                    None => Value::Float(days as f64 + QLIK_EPOCH_OFFSET_DAYS),
                }
            }
            WriteKind::Timestamp => {
                let us = *(data as *const i64).add(r);
                Value::Float(us as f64 / MICROS_PER_DAY + QLIK_EPOCH_OFFSET_DAYS)
            }
            WriteKind::Time => {
                // µs depuis minuit → fraction de jour Qlik.
                let us = *(data as *const i64).add(r);
                Value::Float(us as f64 / MICROS_PER_DAY)
            }
            WriteKind::Interval => {
                // INTERVAL → durée en jours (mois approximés à 30 jours, le
                // format QVD n'ayant pas de notion de mois).
                let iv = *(data as *const ffi::duckdb_interval).add(r);
                let days = iv.months as f64 * 30.0 + iv.days as f64;
                Value::Float(days + iv.micros as f64 / MICROS_PER_DAY)
            }
            WriteKind::Decimal { scale, phys } => {
                let unscaled = match phys {
                    DecimalPhys::I16 => *(data as *const i16).add(r) as i128,
                    DecimalPhys::I32 => *(data as *const i32).add(r) as i128,
                    DecimalPhys::I64 => *(data as *const i64).add(r) as i128,
                    DecimalPhys::I128 => *(data as *const i128).add(r),
                };
                Value::Float(unscaled as f64 / 10f64.powi(scale as i32))
            }
            WriteKind::Str => {
                let s_ptr = (data as *mut ffi::duckdb_string_t).add(r);
                let len = (*s_ptr).value.pointer.length as usize;
                let bytes = std::slice::from_raw_parts(
                    ffi::duckdb_string_t_data(s_ptr) as *const u8,
                    len,
                );
                Value::Str(String::from_utf8_lossy(bytes).into_owned())
            }
        }
    }
}

/// Données de bind : nom + type d'écriture de chaque colonne.
struct CopyBindData {
    names: Vec<String>,
    kinds: Vec<WriteKind>,
}

/// État global : chemin de sortie + encodeur de chaque colonne (protégés par
/// Mutex car le `sink` peut être appelé de plusieurs threads).
struct CopyGlobalState {
    path: String,
    names: Vec<String>,
    kinds: Vec<WriteKind>,
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
    fn push(&mut self, value: Option<Value>, key: &mut Vec<u8>) -> Result<(), String> {
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
                    "COPY (FORMAT qvd) : trop de valeurs distinctes".to_string()
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
fn encode_symbol(v: &Value, out: &mut Vec<u8>) -> Result<(), String> {
    let text = |out: &mut Vec<u8>, s: &str| {
        if s.as_bytes().contains(&0) {
            return Err("COPY (FORMAT qvd) : une chaîne contient l'octet NUL, réservé en fin de symbole".to_string());
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
    mut write: impl FnMut(&[u8]) -> Result<(), String>,
) -> Result<(), String> {
    let overflow = || "COPY (FORMAT qvd) : fichier trop gros pour le format QVD".to_string();
    let n_rows = cols.first().map_or(0, |c| c.rows.len());
    let widths: Vec<u32> = cols.iter().map(Encoder::bit_width).collect();
    let symbols: Vec<Vec<&[u8]>> = cols.iter().map(Encoder::symbols).collect();
    let lengths: Vec<u32> = symbols
        .iter()
        .map(|s| u32::try_from(s.iter().map(|b| b.len()).sum::<usize>()).map_err(|_| overflow()))
        .collect::<Result<_, String>>()?;
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

unsafe extern "C" fn destroy_bind(data: *mut c_void) {
    drop(Box::from_raw(data as *mut CopyBindData));
}

unsafe extern "C" fn destroy_global(data: *mut c_void) {
    drop(Box::from_raw(data as *mut CopyGlobalState));
}

/// Collecte récursivement toutes les chaînes feuilles d'une `duckdb_value`
/// (gère `FIELD_NAMES (a, b, c)` comme `FIELD_NAMES ['a','b','c']`).
unsafe fn collect_strings(v: ffi::duckdb_value, out: &mut Vec<String>) {
    let tid = ffi::duckdb_get_type_id(ffi::duckdb_get_value_type(v));
    match tid {
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR => {
            let p = ffi::duckdb_get_varchar(v);
            if !p.is_null() {
                out.push(CStr::from_ptr(p).to_string_lossy().into_owned());
                ffi::duckdb_free(p as *mut c_void);
            }
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_LIST | ffi::DUCKDB_TYPE_DUCKDB_TYPE_ARRAY => {
            for i in 0..ffi::duckdb_get_list_size(v) {
                let mut child = ffi::duckdb_get_list_child(v, i);
                collect_strings(child, out);
                ffi::duckdb_destroy_value(&mut child);
            }
        }
        _ => {}
    }
}

/// Lit l'option de COPY `FIELD_NAMES` si présente. Les options de COPY sont
/// exposées comme un STRUCT { nom_option: valeur, … }.
unsafe fn read_custom_names(info: ffi::duckdb_copy_function_bind_info) -> Option<Vec<String>> {
    let mut options = ffi::duckdb_copy_function_bind_get_options(info);
    if options.is_null() {
        return None;
    }
    let lt = ffi::duckdb_get_value_type(options); // tied to la valeur, ne pas détruire
    let mut result = None;
    if ffi::duckdb_get_type_id(lt) == ffi::DUCKDB_TYPE_DUCKDB_TYPE_STRUCT {
        for i in 0..ffi::duckdb_struct_type_child_count(lt) {
            let np = ffi::duckdb_struct_type_child_name(lt, i);
            let name = if np.is_null() {
                String::new()
            } else {
                let s = CStr::from_ptr(np).to_string_lossy().into_owned();
                ffi::duckdb_free(np as *mut c_void);
                s
            };
            if name.eq_ignore_ascii_case("field_names") {
                let mut child = ffi::duckdb_get_struct_child(options, i);
                let mut names = Vec::new();
                collect_strings(child, &mut names);
                ffi::duckdb_destroy_value(&mut child);
                result = Some(names);
                break;
            }
        }
    }
    ffi::duckdb_destroy_value(&mut options);
    result
}

/// `bind` : récupère le nombre, les types et (option) les noms des colonnes.
unsafe extern "C" fn copy_bind(info: ffi::duckdb_copy_function_bind_info) {
    guard(|| copy_bind_body(info), |msg| {
        let c = CString::new(msg).unwrap_or_default();
        ffi::duckdb_copy_function_bind_set_error(info, c.as_ptr());
    });
}

unsafe fn copy_bind_body(info: ffi::duckdb_copy_function_bind_info) {
    let set_error = |msg: String| {
        let c = CString::new(msg).unwrap_or_default();
        ffi::duckdb_copy_function_bind_set_error(info, c.as_ptr());
    };

    let n = ffi::duckdb_copy_function_bind_get_column_count(info) as usize;
    let mut kinds = Vec::with_capacity(n);
    for i in 0..n {
        let mut lt = ffi::duckdb_copy_function_bind_get_column_type(info, i as u64);
        let kind = WriteKind::of_logical_type(lt);
        let tid = ffi::duckdb_get_type_id(lt);
        ffi::duckdb_destroy_logical_type(&mut lt);
        match kind {
            Some(k) => kinds.push(k),
            None => {
                set_error(format!(
                    "COPY (FORMAT qvd) : type de colonne non supporté (id {tid}) en écriture ; \
                     CAST vers BIGINT/DOUBLE/DECIMAL/VARCHAR/DATE/TIMESTAMP/TIME/INTERVAL/BOOLEAN"
                ));
                return;
            }
        }
    }

    // Noms : option FIELD_NAMES si fournie (sinon field0, field1, …).
    let names = match read_custom_names(info) {
        Some(custom) if custom.len() == n => custom,
        Some(custom) => {
            set_error(format!(
                "COPY (FORMAT qvd) : FIELD_NAMES fournit {} noms pour {n} colonnes",
                custom.len()
            ));
            return;
        }
        None => (0..n).map(|i| format!("field{i}")).collect(),
    };

    let data = Box::into_raw(Box::new(CopyBindData { names, kinds }));
    ffi::duckdb_copy_function_bind_set_bind_data(info, data.cast(), Some(destroy_bind));
}

/// `global_init` : récupère le chemin de sortie et alloue les tampons.
unsafe extern "C" fn copy_global_init(info: ffi::duckdb_copy_function_global_init_info) {
    guard(|| copy_global_init_body(info), |msg| {
        let c = CString::new(msg).unwrap_or_default();
        ffi::duckdb_copy_function_global_init_set_error(info, c.as_ptr());
    });
}

unsafe fn copy_global_init_body(info: ffi::duckdb_copy_function_global_init_info) {
    let bind = &*(ffi::duckdb_copy_function_global_init_get_bind_data(info) as *const CopyBindData);
    let path = CStr::from_ptr(ffi::duckdb_copy_function_global_init_get_file_path(info))
        .to_string_lossy()
        .into_owned();
    let n = bind.kinds.len();
    let state = Box::new(CopyGlobalState {
        path,
        names: bind.names.clone(),
        kinds: bind.kinds.clone(),
        columns: Mutex::new((0..n).map(|_| Encoder::default()).collect()),
    });
    ffi::duckdb_copy_function_global_init_set_global_state(
        info,
        Box::into_raw(state).cast(),
        Some(destroy_global),
    );
}

/// `sink` : verse un chunk de lignes dans les dictionnaires des colonnes.
unsafe extern "C" fn copy_sink(
    info: ffi::duckdb_copy_function_sink_info,
    chunk: ffi::duckdb_data_chunk,
) {
    let set_error = |msg: String| {
        let c = CString::new(msg).unwrap_or_default();
        ffi::duckdb_copy_function_sink_set_error(info, c.as_ptr());
    };
    guard(|| if let Err(msg) = copy_sink_body(info, chunk) { set_error(msg) }, |msg| {
        let c = CString::new(msg).unwrap_or_default();
        ffi::duckdb_copy_function_sink_set_error(info, c.as_ptr());
    });
}

unsafe fn copy_sink_body(info: ffi::duckdb_copy_function_sink_info, chunk: ffi::duckdb_data_chunk) -> Result<(), String> {
    let state = &*(ffi::duckdb_copy_function_sink_get_global_state(info) as *const CopyGlobalState);
    let n_rows = ffi::duckdb_data_chunk_get_size(chunk) as usize;
    let mut columns = state.columns.lock().unwrap();
    let mut key = Vec::new();
    for (c, kind) in state.kinds.iter().enumerate() {
        let vector = ffi::duckdb_data_chunk_get_vector(chunk, c as u64);
        let data = ffi::duckdb_vector_get_data(vector);
        let validity = ffi::duckdb_vector_get_validity(vector); // null = tout valide
        let col = &mut columns[c];
        for r in 0..n_rows {
            let valid = validity.is_null() || ffi::duckdb_validity_row_is_valid(validity, r as u64);
            col.push(if valid { Some(kind.read(data, r)) } else { None }, &mut key)?;
        }
    }
    Ok(())
}

/// `finalize` : construit le QVD et l'écrit sur disque.
unsafe extern "C" fn copy_finalize(info: ffi::duckdb_copy_function_finalize_info) {
    guard(|| copy_finalize_body(info), |msg| {
        let c = CString::new(msg).unwrap_or_default();
        ffi::duckdb_copy_function_finalize_set_error(info, c.as_ptr());
    });
}

unsafe fn copy_finalize_body(info: ffi::duckdb_copy_function_finalize_info) {
    let state =
        &*(ffi::duckdb_copy_function_finalize_get_global_state(info) as *const CopyGlobalState);

    let result: Result<(), String> = (|| {
        // Sort les encodeurs (laisse un Vec vide) et relâche le lock avant l'écriture.
        let columns = std::mem::take(&mut *state.columns.lock().unwrap());
        let name = std::path::Path::new(&state.path).file_stem().and_then(|s| s.to_str()).unwrap_or("qvd");
        let mut file = std::fs::File::create(&state.path).map_err(|e| format!("{} : {e}", state.path))?;
        write_qvd(name, &state.names, &state.kinds, &columns, |b| {
            std::io::Write::write_all(&mut file, b).map_err(|e| format!("{} : {e}", state.path))
        })?;
        Ok(())
    })();

    if let Err(msg) = result {
        let c = CString::new(msg).unwrap_or_default();
        ffi::duckdb_copy_function_finalize_set_error(info, c.as_ptr());
    }
}

/// Exécute un callback `extern "C"` en convertissant une panique en erreur DuckDB :
/// une panique qui traverse la frontière C avorterait le processus hôte.
pub(crate) fn guard(body: impl FnOnce(), set_error: impl FnOnce(String)) {
    if let Err(panic) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(body)) {
        let msg = panic
            .downcast_ref::<&str>()
            .map(|s| s.to_string())
            .or_else(|| panic.downcast_ref::<String>().cloned())
            .unwrap_or_default();
        set_error(format!("qvd : erreur interne ({msg})"));
    }
}

/// Enregistre la copy function `qvd` sur la connexion donnée.
pub(crate) unsafe fn register(con: ffi::duckdb_connection) -> Result<(), String> {
    let cf = ffi::duckdb_create_copy_function();
    let name = CString::new("qvd").unwrap();
    ffi::duckdb_copy_function_set_name(cf, name.as_ptr());
    ffi::duckdb_copy_function_set_bind(cf, Some(copy_bind));
    ffi::duckdb_copy_function_set_global_init(cf, Some(copy_global_init));
    ffi::duckdb_copy_function_set_sink(cf, Some(copy_sink));
    ffi::duckdb_copy_function_set_finalize(cf, Some(copy_finalize));

    // COPY ... FROM : déléguée à une table function (lecture).
    ffi::duckdb_copy_function_set_copy_from_function(cf, crate::copy_from::build());

    let rc = ffi::duckdb_register_copy_function(con, cf);
    let mut cf_mut = cf;
    ffi::duckdb_destroy_copy_function(&mut cf_mut);

    if rc != ffi::duckdb_state_DuckDBSuccess {
        return Err("échec de l'enregistrement de la copy function qvd".to_string());
    }
    Ok(())
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
