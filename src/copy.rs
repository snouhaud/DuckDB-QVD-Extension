//! Écriture QVD : `COPY (SELECT …) TO 'fichier.qvd' (FORMAT qvd)`.
//!
//! DuckDB-rs ne wrappe pas les *copy functions*, mais l'API C les expose
//! (`duckdb_create_copy_function` & co, accessibles via `duckdb::ffi`). Ce
//! module les pilote en FFI brut : `bind` (schéma) → `global_init` (chemin +
//! tampons) → `sink` (accumulation des chunks) → `finalize` (écriture via le
//! writer OpenQVD).
//!
//! # Limitation connue
//!
//! L'API C de la copy function n'expose **pas les noms de colonnes** : les
//! champs sont nommés `field0`, `field1`, … Les types, eux, sont préservés
//! (entiers, flottants, texte, `DATE`, `TIMESTAMP`) et taggés pour que
//! [`crate::qvd`] les relise correctement.

use std::ffi::{c_void, CStr, CString};
use std::sync::Mutex;

use duckdb::ffi;
use openqvd::{Column, Value, WriteTable};

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
                let days = *(data as *const i32).add(r);
                Value::Int(days + QLIK_EPOCH_OFFSET_DAYS as i32) // jours DuckDB → série Qlik
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
                let days = (iv.months * 30 + iv.days) as f64;
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

/// État global : chemin de sortie + valeurs accumulées (protégées par Mutex
/// car le `sink` peut être appelé de plusieurs threads).
struct CopyGlobalState {
    path: String,
    names: Vec<String>,
    kinds: Vec<WriteKind>,
    columns: Mutex<Vec<Vec<Option<Value>>>>,
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
                     CAST vers BIGINT/DOUBLE/VARCHAR/DATE/TIMESTAMP"
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
    let bind = &*(ffi::duckdb_copy_function_global_init_get_bind_data(info) as *const CopyBindData);
    let path = CStr::from_ptr(ffi::duckdb_copy_function_global_init_get_file_path(info))
        .to_string_lossy()
        .into_owned();
    let n = bind.kinds.len();
    let state = Box::new(CopyGlobalState {
        path,
        names: bind.names.clone(),
        kinds: bind.kinds.clone(),
        columns: Mutex::new((0..n).map(|_| Vec::new()).collect()),
    });
    ffi::duckdb_copy_function_global_init_set_global_state(
        info,
        Box::into_raw(state).cast(),
        Some(destroy_global),
    );
}

/// `sink` : accumule un chunk de lignes dans les tampons.
unsafe extern "C" fn copy_sink(
    info: ffi::duckdb_copy_function_sink_info,
    chunk: ffi::duckdb_data_chunk,
) {
    let state = &*(ffi::duckdb_copy_function_sink_get_global_state(info) as *const CopyGlobalState);
    let n_rows = ffi::duckdb_data_chunk_get_size(chunk) as usize;
    let mut columns = state.columns.lock().unwrap();
    for (c, kind) in state.kinds.iter().enumerate() {
        let vector = ffi::duckdb_data_chunk_get_vector(chunk, c as u64);
        let data = ffi::duckdb_vector_get_data(vector);
        let validity = ffi::duckdb_vector_get_validity(vector); // null = tout valide
        let col = &mut columns[c];
        for r in 0..n_rows {
            let valid = validity.is_null() || ffi::duckdb_validity_row_is_valid(validity, r as u64);
            col.push(if valid { Some(kind.read(data, r)) } else { None });
        }
    }
}

/// `finalize` : construit le QVD et l'écrit sur disque.
unsafe extern "C" fn copy_finalize(info: ffi::duckdb_copy_function_finalize_info) {
    let state =
        &*(ffi::duckdb_copy_function_finalize_get_global_state(info) as *const CopyGlobalState);

    let result: Result<(), String> = (|| {
        let columns = state.columns.lock().unwrap();
        let cols: Vec<Column> = columns
            .iter()
            .enumerate()
            .map(|(i, vals)| {
                let mut col = Column::new(state.names[i].clone(), vals.clone());
                col.tags = state.kinds[i].tags();
                col
            })
            .collect();
        let table = WriteTable::new("qvd", cols).map_err(|e| e.to_string())?;
        let bytes = table.to_bytes().map_err(|e| e.to_string())?;
        std::fs::write(&state.path, bytes).map_err(|e| e.to_string())?;
        Ok(())
    })();

    if let Err(msg) = result {
        let c = CString::new(msg).unwrap_or_default();
        ffi::duckdb_copy_function_finalize_set_error(info, c.as_ptr());
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

    let rc = ffi::duckdb_register_copy_function(con, cf);
    let mut cf_mut = cf;
    ffi::duckdb_destroy_copy_function(&mut cf_mut);

    if rc != ffi::duckdb_state_DuckDBSuccess {
        return Err("échec de l'enregistrement de la copy function qvd".to_string());
    }
    Ok(())
}
