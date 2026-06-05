//! Lecture : `COPY table FROM 'fichier.qvd' (FORMAT qvd)`.
//!
//! Dans l'API C, le `COPY ... FROM` est délégué à une **table function**
//! (attachée à la copy function via `duckdb_copy_function_set_copy_from_function`).
//! duckdb-rs ne permet pas de récupérer le `duckdb_table_function` brut d'un
//! `VTab` enregistré, donc on construit ici une table function en FFI brut qui
//! réutilise la logique de lecture de [`crate::qvd`] et écrit les chunks.

use std::ffi::{c_void, CStr, CString};
use std::sync::atomic::{AtomicUsize, Ordering};

use duckdb::ffi;

use crate::qvd::{self, ColumnData, IntervalVal, Kind, Schema};

const VECTOR_SIZE: usize = 2048;

struct CfBind {
    path: String,
    names: Vec<String>,
    kinds: Vec<Kind>,
}

struct CfInit {
    columns: Vec<ColumnData>,
    num_rows: usize,
    cursor: AtomicUsize,
}

unsafe extern "C" fn destroy_bind(data: *mut c_void) {
    drop(Box::from_raw(data as *mut CfBind));
}

unsafe extern "C" fn destroy_init(data: *mut c_void) {
    drop(Box::from_raw(data as *mut CfInit));
}

/// `bind` : lit le chemin (1er paramètre), déclare les colonnes du QVD.
unsafe extern "C" fn cf_bind(info: ffi::duckdb_bind_info) {
    match bind_inner(info) {
        Ok(bind) => {
            let ptr = Box::into_raw(Box::new(bind));
            ffi::duckdb_bind_set_bind_data(info, ptr.cast(), Some(destroy_bind));
        }
        Err(msg) => {
            let c = CString::new(msg).unwrap_or_default();
            ffi::duckdb_bind_set_error(info, c.as_ptr());
        }
    }
}

unsafe fn bind_inner(info: ffi::duckdb_bind_info) -> Result<CfBind, String> {
    let mut pval = ffi::duckdb_bind_get_parameter(info, 0);
    let pp = ffi::duckdb_get_varchar(pval);
    let path = if pp.is_null() {
        String::new()
    } else {
        let s = CStr::from_ptr(pp).to_string_lossy().into_owned();
        ffi::duckdb_free(pp as *mut c_void);
        s
    };
    ffi::duckdb_destroy_value(&mut pval);

    let Schema { names, kinds, type_ids } = qvd::read_schema(&path).map_err(|e| e.to_string())?;
    for (name, tid) in names.iter().zip(type_ids.into_iter()) {
        let mut lt = ffi::duckdb_create_logical_type(tid as ffi::duckdb_type);
        let cname = CString::new(name.as_str()).map_err(|e| e.to_string())?;
        ffi::duckdb_bind_add_result_column(info, cname.as_ptr(), lt);
        ffi::duckdb_destroy_logical_type(&mut lt);
    }

    Ok(CfBind { path, names, kinds })
}

/// `init` : décode toutes les colonnes du fichier.
unsafe extern "C" fn cf_init(info: ffi::duckdb_init_info) {
    let bind = &*(ffi::duckdb_init_get_bind_data(info) as *const CfBind);
    let indices: Vec<usize> = (0..bind.names.len()).collect();
    match qvd::read_projected(
        std::slice::from_ref(&bind.path),
        &bind.names,
        &bind.kinds,
        &indices,
    ) {
        Ok((columns, num_rows)) => {
            let ptr = Box::into_raw(Box::new(CfInit { columns, num_rows, cursor: AtomicUsize::new(0) }));
            ffi::duckdb_init_set_init_data(info, ptr.cast(), Some(destroy_init));
        }
        Err(e) => {
            let c = CString::new(e.to_string()).unwrap_or_default();
            ffi::duckdb_init_set_error(info, c.as_ptr());
        }
    }
}

/// `function` : émet un paquet de lignes par appel.
unsafe extern "C" fn cf_func(info: ffi::duckdb_function_info, output: ffi::duckdb_data_chunk) {
    let init = &*(ffi::duckdb_function_get_init_data(info) as *const CfInit);
    let start = init.cursor.load(Ordering::Relaxed);
    let n = init.num_rows.saturating_sub(start).min(VECTOR_SIZE);

    for (c, col) in init.columns.iter().enumerate() {
        let vector = ffi::duckdb_data_chunk_get_vector(output, c as u64);
        match col {
            ColumnData::I64(v) => write_prim(vector, v, start, n),
            ColumnData::F64(v) => write_prim(vector, v, start, n),
            ColumnData::Date(v) => write_prim(vector, v, start, n),
            ColumnData::Timestamp(v) => write_prim(vector, v, start, n),
            ColumnData::Time(v) => write_prim(vector, v, start, n),
            ColumnData::Interval(v) => write_prim(vector, v, start, n),
            ColumnData::Utf8(v) => write_str(vector, v, start, n),
        }
    }

    init.cursor.store(start + n, Ordering::Relaxed);
    ffi::duckdb_data_chunk_set_size(output, n as u64);
}

/// Écrit une colonne de type primitif (copie binaire directe) dans le vecteur.
unsafe fn write_prim<T: Copy>(vector: ffi::duckdb_vector, v: &[Option<T>], start: usize, n: usize) {
    let data = ffi::duckdb_vector_get_data(vector) as *mut T;
    let mut validity: *mut u64 = std::ptr::null_mut();
    for i in 0..n {
        match v[start + i] {
            Some(x) => *data.add(i) = x,
            None => {
                if validity.is_null() {
                    ffi::duckdb_vector_ensure_validity_writable(vector);
                    validity = ffi::duckdb_vector_get_validity(vector);
                }
                *data.add(i) = std::mem::zeroed();
                ffi::duckdb_validity_set_row_invalid(validity, i as u64);
            }
        }
    }
}

/// Écrit une colonne `VARCHAR`.
unsafe fn write_str(vector: ffi::duckdb_vector, v: &[Option<String>], start: usize, n: usize) {
    let mut validity: *mut u64 = std::ptr::null_mut();
    for i in 0..n {
        match &v[start + i] {
            Some(s) => ffi::duckdb_vector_assign_string_element_len(
                vector,
                i as u64,
                s.as_ptr() as *const std::os::raw::c_char,
                s.len() as u64,
            ),
            None => {
                if validity.is_null() {
                    ffi::duckdb_vector_ensure_validity_writable(vector);
                    validity = ffi::duckdb_vector_get_validity(vector);
                }
                ffi::duckdb_validity_set_row_invalid(validity, i as u64);
            }
        }
    }
}

/// Construit la table function de `COPY ... FROM` (à attacher à la copy function).
pub(crate) unsafe fn build() -> ffi::duckdb_table_function {
    let tf = ffi::duckdb_create_table_function();
    let name = CString::new("qvd_copy_from").unwrap();
    ffi::duckdb_table_function_set_name(tf, name.as_ptr());

    let mut varchar = ffi::duckdb_create_logical_type(ffi::DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR);
    ffi::duckdb_table_function_add_parameter(tf, varchar);
    ffi::duckdb_destroy_logical_type(&mut varchar);

    ffi::duckdb_table_function_set_bind(tf, Some(cf_bind));
    ffi::duckdb_table_function_set_init(tf, Some(cf_init));
    ffi::duckdb_table_function_set_function(tf, Some(cf_func));
    tf
}

// `IntervalVal` doit avoir la même disposition que `duckdb_interval` pour la
// copie binaire (write_prim). Vérifié à la compilation.
const _: () = assert!(std::mem::size_of::<IntervalVal>() == 16);
