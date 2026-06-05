//! Extension DuckDB `qvd` — expose la table function `read_qvd('fichier.qvd')`.
//!
//! `lib.rs` porte la plomberie DuckDB (enregistrement de la fonction, cycle
//! bind/init/func). La lecture et le typage du format QVD sont délégués à
//! [`mod@qvd`], qui s'appuie sur le crate OpenQVD.

mod copy;
mod qvd;

use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    ffi,
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    Connection, Result,
};
use std::{
    error::Error,
    sync::atomic::{AtomicUsize, Ordering},
};

use qvd::{ColumnData, Kind};

/// Taille standard d'un vecteur DuckDB : nombre max de lignes par `func`.
const VECTOR_SIZE: usize = 2048;

/// Données produites par `bind` : la liste des fichiers (glob déployé) et le
/// schéma complet (sans données). La lecture effective n'a lieu qu'à l'`init`,
/// en ne décodant que les colonnes projetées.
struct ReadQvdBindData {
    paths: Vec<String>,
    names: Vec<String>,
    kinds: Vec<Kind>,
}

/// État d'un scan : colonnes projetées (ordre de sortie), nombre de lignes et
/// curseur courant.
struct ReadQvdInitData {
    columns: Vec<ColumnData>,
    num_rows: usize,
    cursor: AtomicUsize,
}

struct ReadQvdVTab;

impl VTab for ReadQvdVTab {
    type InitData = ReadQvdInitData;
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

    /// Décode uniquement les colonnes projetées par DuckDB, sur tous les
    /// fichiers du glob (lignes concaténées).
    fn init(info: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        let bind = unsafe { &*info.get_bind_data::<ReadQvdBindData>() };
        let indices: Vec<usize> =
            info.get_column_indices().into_iter().map(|i| i as usize).collect();

        let (columns, num_rows) =
            qvd::read_projected(&bind.paths, &bind.names, &bind.kinds, &indices)?;

        Ok(ReadQvdInitData { columns, num_rows, cursor: AtomicUsize::new(0) })
    }

    /// Émet un paquet de lignes (jusqu'à `VECTOR_SIZE`) à chaque appel, jusqu'à
    /// épuisement (`set_len(0)`).
    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        let init_data = func.get_init_data();

        let start = init_data.cursor.load(Ordering::Relaxed);
        if start >= init_data.num_rows {
            output.set_len(0);
            return Ok(());
        }
        let n = (init_data.num_rows - start).min(VECTOR_SIZE);

        for (j, column) in init_data.columns.iter().enumerate() {
            let mut vector = output.flat_vector(j);
            match column {
                ColumnData::I64(data) => {
                    {
                        // Le vecteur est dimensionné pour VECTOR_SIZE lignes ;
                        // on n'écrit que les indices 0..n.
                        let slice = vector.as_mut_slice::<i64>();
                        for i in 0..n {
                            slice[i] = data[start + i].unwrap_or_default();
                        }
                    }
                    for i in 0..n {
                        if data[start + i].is_none() {
                            vector.set_null(i);
                        }
                    }
                }
                ColumnData::F64(data) => {
                    {
                        let slice = vector.as_mut_slice::<f64>();
                        for i in 0..n {
                            slice[i] = data[start + i].unwrap_or_default();
                        }
                    }
                    for i in 0..n {
                        if data[start + i].is_none() {
                            vector.set_null(i);
                        }
                    }
                }
                ColumnData::Utf8(data) => {
                    for i in 0..n {
                        match &data[start + i] {
                            Some(s) => vector.insert(i, s.as_str()),
                            None => vector.set_null(i),
                        }
                    }
                }
                // DuckDB DATE : physiquement un i32 (jours depuis 1970-01-01).
                ColumnData::Date(data) => {
                    {
                        let slice = vector.as_mut_slice::<i32>();
                        for i in 0..n {
                            slice[i] = data[start + i].unwrap_or_default();
                        }
                    }
                    for i in 0..n {
                        if data[start + i].is_none() {
                            vector.set_null(i);
                        }
                    }
                }
                // DuckDB TIMESTAMP : physiquement un i64 (µs depuis 1970-01-01).
                ColumnData::Timestamp(data) => {
                    {
                        let slice = vector.as_mut_slice::<i64>();
                        for i in 0..n {
                            slice[i] = data[start + i].unwrap_or_default();
                        }
                    }
                    for i in 0..n {
                        if data[start + i].is_none() {
                            vector.set_null(i);
                        }
                    }
                }
            }
        }

        init_data.cursor.store(start + n, Ordering::Relaxed);
        output.set_len(n);
        Ok(())
    }

    /// Paramètres positionnels : `read_qvd(VARCHAR)`.
    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
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
