//! Extension DuckDB `qvd` — expose la table function `read_qvd('fichier.qvd')`.
//!
//! `lib.rs` porte la plomberie DuckDB (enregistrement de la fonction, cycle
//! bind/init/func). La lecture et le typage du format QVD sont délégués à
//! [`mod@qvd`], qui s'appuie sur le crate OpenQVD.

mod qvd;

use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    duckdb_entrypoint_c_api,
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

#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<ReadQvdVTab>("read_qvd")
        .expect("Échec de l'enregistrement de la table function read_qvd");
    Ok(())
}
