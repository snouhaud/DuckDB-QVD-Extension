//! Extension DuckDB `qvd` — expose la table function `read_qvd('fichier.qvd')`.
//!
//! `lib.rs` porte la plomberie DuckDB (enregistrement de la fonction, cycle
//! bind/init/func). La lecture (streaming) et le typage du format QVD sont
//! délégués à [`mod@qvd`] (crate `qvdrs`) ; l'écriture à [`mod@copy`] (OpenQVD).

mod copy;
mod copy_from;
mod qvd;

use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    ffi,
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    Connection, Result,
};
use std::error::Error;

use qvd::{Cell, Kind, Pull, QvdScan};

/// Données produites par `bind` : la liste des fichiers (glob déployé) et le
/// schéma complet (sans données). La lecture effective (streaming) n'a lieu
/// qu'à l'`init`/`func`, en ne décodant que les colonnes projetées.
struct ReadQvdBindData {
    paths: Vec<String>,
    names: Vec<String>,
    kinds: Vec<Kind>,
}

struct ReadQvdVTab;

impl VTab for ReadQvdVTab {
    type InitData = QvdScan;
    type BindData = ReadQvdBindData;

    /// Active la projection pushdown : DuckDB indiquera à l'`init` les seules
    /// colonnes nécessaires.
    fn supports_pushdown() -> bool {
        true
    }

    /// Déploie le motif (glob), lit le schéma du premier fichier (en-tête seul)
    /// et déclare une colonne DuckDB par champ.
    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        let pattern = bind.get_parameter(0).to_string();
        let paths = expand_glob(&pattern)?;

        let schema = qvd::read_schema(&paths[0])?;
        for (name, type_id) in schema.names.iter().zip(schema.type_ids.into_iter()) {
            bind.add_result_column(name.as_str(), LogicalTypeHandle::from(type_id));
        }

        Ok(ReadQvdBindData { paths, names: schema.names, kinds: schema.kinds })
    }

    /// Prépare le scan streaming (aucun décodage de l'index ici).
    fn init(info: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        let bind = unsafe { &*info.get_bind_data::<ReadQvdBindData>() };
        let indices: Vec<usize> =
            info.get_column_indices().into_iter().map(|i| i as usize).collect();
        QvdScan::new(&bind.paths, &bind.names, &bind.kinds, &indices)
    }

    /// Tire un chunk du scan streaming et le copie dans la sortie DuckDB.
    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        let scan = func.get_init_data();
        match scan.pull()? {
            Pull::Done => output.set_len(0),
            Pull::Rows(n) => output.set_len(n), // COUNT(*) : lignes sans colonnes
            Pull::Cells { columns } => {
                // Colonnes déjà converties par dictionnaire : copie directe.
                let kinds = scan.output_kinds();
                let n = columns.first().map_or(0, |c| c.len());
                for (j, &kind) in kinds.iter().enumerate() {
                    write_column(output, j, kind, &columns[j]);
                }
                output.set_len(n);
            }
        }
        Ok(())
    }

    /// Paramètres positionnels : `read_qvd(VARCHAR)`.
    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }
}

/// Écrit une colonne de `Cell` (déjà convertis) dans le FlatVector `j`.
/// Pour les types primitifs : écrit le slice puis pose les bits NULL.
fn write_column(output: &mut DataChunkHandle, j: usize, kind: Kind, cells: &[Cell]) {
    let mut v = output.flat_vector(j);
    match kind {
        Kind::Text => {
            for (r, c) in cells.iter().enumerate() {
                match c {
                    Cell::Str(s) => v.insert(r, s.as_str()),
                    _ => v.set_null(r),
                }
            }
        }
        Kind::Int | Kind::Timestamp | Kind::Time => {
            {
                let s = unsafe { v.as_mut_slice::<i64>() };
                for (r, c) in cells.iter().enumerate() {
                    s[r] = if let Cell::I64(x) = c { *x } else { 0 };
                }
            }
            set_nulls(&mut v, cells);
        }
        Kind::Float => {
            {
                let s = unsafe { v.as_mut_slice::<f64>() };
                for (r, c) in cells.iter().enumerate() {
                    s[r] = if let Cell::F64(x) = c { *x } else { 0.0 };
                }
            }
            set_nulls(&mut v, cells);
        }
        Kind::Date => {
            {
                let s = unsafe { v.as_mut_slice::<i32>() };
                for (r, c) in cells.iter().enumerate() {
                    s[r] = if let Cell::I32(x) = c { *x } else { 0 };
                }
            }
            set_nulls(&mut v, cells);
        }
        Kind::Interval => {
            {
                let s = unsafe { v.as_mut_slice::<qvd::IntervalVal>() };
                for (r, c) in cells.iter().enumerate() {
                    s[r] = match c {
                        Cell::Interval(x) => *x,
                        _ => qvd::IntervalVal { months: 0, days: 0, micros: 0 },
                    };
                }
            }
            set_nulls(&mut v, cells);
        }
    }
}

fn set_nulls(v: &mut duckdb::core::FlatVector, cells: &[Cell]) {
    for (r, c) in cells.iter().enumerate() {
        if matches!(c, Cell::Null) {
            v.set_null(r);
        }
    }
}

/// Déploie un motif glob en liste de fichiers triée. Un chemin littéral sans
/// métacaractère se résout en lui-même (s'il existe). Erreur si aucun match.
fn expand_glob(pattern: &str) -> Result<Vec<String>, Box<dyn Error>> {
    let mut paths = Vec::new();
    for entry in glob::glob(pattern)? {
        paths.push(entry?.to_string_lossy().into_owned());
    }
    paths.sort();
    if paths.is_empty() {
        return Err(format!("aucun fichier ne correspond au motif '{pattern}'").into());
    }
    Ok(paths)
}

/// Enregistre la table function `read_qvd` et la copy function `qvd`.
///
/// On n'utilise pas la macro `duckdb_entrypoint_c_api` car la copy function
/// (non wrappée par duckdb-rs) requiert le `duckdb_connection` brut, que la
/// `Connection` de la macro n'expose pas. On reproduit donc sa logique :
/// init de l'API C, récupération de la database, puis enregistrements.
unsafe fn init_internal(
    info: ffi::duckdb_extension_info,
    access: *const ffi::duckdb_extension_access,
) -> Result<bool, Box<dyn Error>> {
    let version = option_env!("DUCKDB_EXTENSION_MIN_DUCKDB_VERSION").unwrap_or("dev");
    if !ffi::duckdb_rs_extension_api_init(info, access, version)? {
        return Ok(false); // incompatibilité de version d'API
    }

    let get_database = (*access)
        .get_database
        .ok_or("get_database est null dans duckdb_extension_access")?;
    let db_ptr = get_database(info);
    if db_ptr.is_null() {
        return Ok(false);
    }
    let database: ffi::duckdb_database = *db_ptr;

    // Table function via l'API haut niveau de duckdb-rs.
    let connection = Connection::open_from_raw(database.cast())?;
    connection.register_table_function::<ReadQvdVTab>("read_qvd")?;

    // Copy function via FFI brut, sur une connexion dédiée (l'enregistrement
    // persiste au niveau de la base).
    let mut con: ffi::duckdb_connection = std::ptr::null_mut();
    if ffi::duckdb_connect(database, &mut con) != ffi::duckdb_state_DuckDBSuccess {
        return Err("duckdb_connect a échoué".into());
    }
    let res = copy::register(con);
    ffi::duckdb_disconnect(&mut con);
    res?;

    Ok(true)
}

/// Point d'entrée C appelé par DuckDB au chargement de l'extension.
///
/// # Safety
/// Appelé par DuckDB avec des pointeurs valides.
#[no_mangle]
pub unsafe extern "C" fn qvd_init_c_api(
    info: ffi::duckdb_extension_info,
    access: *const ffi::duckdb_extension_access,
) -> bool {
    match init_internal(info, access) {
        Ok(v) => v,
        Err(e) => {
            if let Some(set_error) = (*access).set_error {
                if let Ok(c) = std::ffi::CString::new(e.to_string()) {
                    set_error(info, c.as_ptr());
                }
            }
            false
        }
    }
}
