//! Lecture : `COPY table FROM 'fichier.qvd' (FORMAT qvd)`.
//!
//! Dans l'API C, le `COPY ... FROM` est délégué à une **table function**
//! (attachée à la copy function via `duckdb_copy_function_set_copy_from_function`).
//! duckdb-rs ne permet pas de récupérer le `duckdb_table_function` brut d'un
//! `VTab` enregistré, donc on construit ici une table function en FFI brut qui
//! réutilise le scan streaming de [`crate::qvd`] et écrit les chunks.

use std::ffi::{c_void, CStr, CString};

use duckdb::ffi;

use crate::qvd::{self, Cell, IntervalVal, Kind, Pull, QvdScan, Schema};

struct CfBind {
    path: String,
    names: Vec<String>,
    kinds: Vec<Kind>,
}

unsafe extern "C" fn destroy_bind(data: *mut c_void) {
    drop(Box::from_raw(data as *mut CfBind));
}

unsafe extern "C" fn destroy_init(data: *mut c_void) {
    drop(Box::from_raw(data as *mut QvdScan));
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

/// `init` : prépare le scan streaming (un seul fichier).
unsafe extern "C" fn cf_init(info: ffi::duckdb_init_info) {
    let bind = &*(ffi::duckdb_init_get_bind_data(info) as *const CfBind);
    let indices: Vec<usize> = (0..bind.names.len()).collect();
    match QvdScan::new(std::slice::from_ref(&bind.path), &bind.names, &bind.kinds, &indices) {
        Ok(scan) => {
            let ptr = Box::into_raw(Box::new(scan));
            ffi::duckdb_init_set_init_data(info, ptr.cast(), Some(destroy_init));
        }
        Err(e) => {
            let c = CString::new(e.to_string()).unwrap_or_default();
            ffi::duckdb_init_set_error(info, c.as_ptr());
        }
    }
}

/// `function` : tire un chunk du scan et le copie dans la sortie.
unsafe extern "C" fn cf_func(info: ffi::duckdb_function_info, output: ffi::duckdb_data_chunk) {
    let scan = &*(ffi::duckdb_function_get_init_data(info) as *const QvdScan);
    match scan.pull() {
        Ok(Pull::Done) => ffi::duckdb_data_chunk_set_size(output, 0),
        Ok(Pull::Rows(n)) => ffi::duckdb_data_chunk_set_size(output, n as u64),
        Ok(Pull::Cells { columns }) => {
            let kinds = scan.output_kinds();
            let n = columns.first().map_or(0, |c| c.len());
            for (j, &kind) in kinds.iter().enumerate() {
                let vector = ffi::duckdb_data_chunk_get_vector(output, j as u64);
                write_col_raw(vector, kind, &columns[j]);
            }
            ffi::duckdb_data_chunk_set_size(output, n as u64);
        }
        Err(e) => {
            let c = CString::new(e.to_string()).unwrap_or_default();
            ffi::duckdb_function_set_error(info, c.as_ptr());
        }
    }
}

/// Pose le bit NULL de la ligne `r` (alloue le masque de validité au besoin).
unsafe fn set_null_raw(vector: ffi::duckdb_vector, r: usize) {
    ffi::duckdb_vector_ensure_validity_writable(vector);
    let validity = ffi::duckdb_vector_get_validity(vector);
    ffi::duckdb_validity_set_row_invalid(validity, r as u64);
}

/// Écrit une colonne de `Cell` dans un vecteur DuckDB (FFI brut).
unsafe fn write_col_raw(vector: ffi::duckdb_vector, kind: Kind, cells: &[Cell]) {
    match kind {
        Kind::Text => {
            for (r, c) in cells.iter().enumerate() {
                match c {
                    Cell::Str(s) => ffi::duckdb_vector_assign_string_element_len(
                        vector,
                        r as u64,
                        s.as_ptr() as *const std::os::raw::c_char,
                        s.len() as u64,
                    ),
                    _ => set_null_raw(vector, r),
                }
            }
        }
        Kind::Int | Kind::Timestamp | Kind::Time => {
            let data = ffi::duckdb_vector_get_data(vector) as *mut i64;
            for (r, c) in cells.iter().enumerate() {
                match c {
                    Cell::I64(x) => *data.add(r) = *x,
                    _ => {
                        *data.add(r) = 0;
                        set_null_raw(vector, r);
                    }
                }
            }
        }
        Kind::Float => {
            let data = ffi::duckdb_vector_get_data(vector) as *mut f64;
            for (r, c) in cells.iter().enumerate() {
                match c {
                    Cell::F64(x) => *data.add(r) = *x,
                    _ => {
                        *data.add(r) = 0.0;
                        set_null_raw(vector, r);
                    }
                }
            }
        }
        Kind::Date => {
            let data = ffi::duckdb_vector_get_data(vector) as *mut i32;
            for (r, c) in cells.iter().enumerate() {
                match c {
                    Cell::I32(x) => *data.add(r) = *x,
                    _ => {
                        *data.add(r) = 0;
                        set_null_raw(vector, r);
                    }
                }
            }
        }
        Kind::Interval => {
            let data = ffi::duckdb_vector_get_data(vector) as *mut IntervalVal;
            for (r, c) in cells.iter().enumerate() {
                match c {
                    Cell::Interval(x) => *data.add(r) = *x,
                    _ => {
                        *data.add(r) = IntervalVal { months: 0, days: 0, micros: 0 };
                        set_null_raw(vector, r);
                    }
                }
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

// `IntervalVal` doit avoir la même disposition que `duckdb_interval`.
const _: () = assert!(std::mem::size_of::<IntervalVal>() == 16);
