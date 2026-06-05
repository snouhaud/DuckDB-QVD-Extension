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
}

impl WriteKind {
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
            _ => return None,
        })
    }

    /// Tags Qlik écrits dans l'en-tête (pour que la relecture re-type juste).
    fn tags(self) -> Vec<String> {
        let t: &[&str] = match self {
            WriteKind::Bool | WriteKind::I8 | WriteKind::I16 | WriteKind::I32 | WriteKind::I64 => {
                &["$numeric", "$integer"]
            }
            WriteKind::F32 | WriteKind::F64 => &["$numeric"],
            WriteKind::Str => &["$text"],
            WriteKind::Date => &["$date"],
            WriteKind::Timestamp => &["$timestamp"],
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

/// Données de bind : type d'écriture de chaque colonne.
struct CopyBindData {
    kinds: Vec<WriteKind>,
}

/// État global : chemin de sortie + valeurs accumulées (protégées par Mutex
/// car le `sink` peut être appelé de plusieurs threads).
struct CopyGlobalState {
    path: String,
    kinds: Vec<WriteKind>,
    columns: Mutex<Vec<Vec<Option<Value>>>>,
}

unsafe extern "C" fn destroy_bind(data: *mut c_void) {
    drop(Box::from_raw(data as *mut CopyBindData));
}

unsafe extern "C" fn destroy_global(data: *mut c_void) {
    drop(Box::from_raw(data as *mut CopyGlobalState));
}

/// `bind` : récupère le nombre et les types des colonnes.
unsafe extern "C" fn copy_bind(info: ffi::duckdb_copy_function_bind_info) {
    let n = ffi::duckdb_copy_function_bind_get_column_count(info) as usize;
    let mut kinds = Vec::with_capacity(n);
    for i in 0..n {
        let mut lt = ffi::duckdb_copy_function_bind_get_column_type(info, i as u64);
        let tid = ffi::duckdb_get_type_id(lt);
        ffi::duckdb_destroy_logical_type(&mut lt);
        match WriteKind::from_type_id(tid) {
            Some(k) => kinds.push(k),
            None => {
                let msg = CString::new(format!(
                    "read_qvd/COPY : type de colonne non supporté (id {tid}) en écriture ; \
                     CAST vers BIGINT/DOUBLE/VARCHAR/DATE/TIMESTAMP"
                ))
                .unwrap();
                ffi::duckdb_copy_function_bind_set_error(info, msg.as_ptr());
                return;
            }
        }
    }
    let data = Box::into_raw(Box::new(CopyBindData { kinds }));
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
                let mut col = Column::new(format!("field{i}"), vals.clone());
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
