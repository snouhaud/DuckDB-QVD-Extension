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

use qvd::{ColumnData, QvdData};

/// Taille standard d'un vecteur DuckDB : nombre max de lignes par `func`.
const VECTOR_SIZE: usize = 2048;

/// Données produites par `bind` : le QVD entièrement matérialisé en colonnes
/// typées, partagé en lecture par tous les appels de `func`.
struct ReadQvdBindData {
    num_rows: usize,
    columns: Vec<ColumnData>,
}

/// État d'exécution du scan : curseur de ligne courant.
struct ReadQvdInitData {
    cursor: AtomicUsize,
}

struct ReadQvdVTab;

impl VTab for ReadQvdVTab {
    type InitData = ReadQvdInitData;
    type BindData = ReadQvdBindData;

    /// Ouvre le fichier, déclare une colonne DuckDB par champ QVD et conserve
    /// les données matérialisées pour `func`.
    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        let path = bind.get_parameter(0).to_string();

        let QvdData { names, type_ids, columns, num_rows } = qvd::read_qvd(&path)?;

        for (name, type_id) in names.iter().zip(type_ids.into_iter()) {
            bind.add_result_column(name.as_str(), LogicalTypeHandle::from(type_id));
        }

        Ok(ReadQvdBindData { num_rows, columns })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(ReadQvdInitData { cursor: AtomicUsize::new(0) })
    }

    /// Émet un paquet de lignes (jusqu'à `VECTOR_SIZE`) à chaque appel, jusqu'à
    /// épuisement (`set_len(0)`).
    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        let bind_data = func.get_bind_data();
        let init_data = func.get_init_data();

        let start = init_data.cursor.load(Ordering::Relaxed);
        if start >= bind_data.num_rows {
            output.set_len(0);
            return Ok(());
        }
        let n = (bind_data.num_rows - start).min(VECTOR_SIZE);

        for (j, column) in bind_data.columns.iter().enumerate() {
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

#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<ReadQvdVTab>("read_qvd")
        .expect("Échec de l'enregistrement de la table function read_qvd");
    Ok(())
}
