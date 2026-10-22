//! Extension DuckDB `qvd` — expose la table function `read_qvd('fichier.qvd')`.
//!
//! `lib.rs` porte la plomberie DuckDB (C API v2 via `duckdb-neo` : point
//! d'entrée, table function). La lecture (streaming) et le typage du format QVD
//! sont délégués à [`mod@qvd`] (crate `qvdrs`) ; l'écriture à [`mod@copy`]
//! (OpenQVD).

mod copy;
mod copy_from;
mod qvd;

use std::fmt::Display;

use duckdb_loadable_macros::duckdb_entrypoint_c_api_v2;
use duckdb_neo::{
    bind_arguments::BindArgument,
    connection::Context,
    copy_function::CopyFunctionBuilder,
    data_chunk::DataChunkRef,
    extension::Extension,
    logical_type::LogicalType,
    signature::{FunctionParameter, SignatureBuilder},
    table_function::{
        BindFunctionHandle, ExecColumnInfo, InitColumnData, TableFunctionBuilder,
        TableFunctionCallbacks,
    },
    types::{DateValue, IntervalValue, TimeValue, TimestampValue},
    vector::VectorElement,
    DuckDBType, Error, Result,
};

use qvd::{Cell, Kind, Pull, QvdScan};

/// Données produites par `bind` : la liste des fichiers (glob déployé) et le
/// schéma complet (sans données). La lecture effective (streaming) n'a lieu
/// qu'à l'`init`/`exec`, en ne décodant que les colonnes projetées.
struct ReadQvdBindData {
    paths: Vec<String>,
    names: Vec<String>,
    kinds: Vec<Kind>,
}

struct ReadQvd;

impl TableFunctionCallbacks for ReadQvd {
    type BindData = ReadQvdBindData;
    type GlobalState = QvdScan;
    type LocalState = ();

    /// Déploie le motif (glob), lit le schéma du premier fichier (en-tête seul)
    /// et déclare une colonne DuckDB par champ.
    fn bind(
        &self,
        context: &Context,
        arguments: Vec<BindArgument>,
        bind: BindFunctionHandle<'_>,
    ) -> Result<Self::BindData> {
        let pattern = arguments[0]
            .value
            .as_ref()
            .and_then(|v| v.get::<String>().transpose())
            .transpose()?
            .ok_or_else(|| Error::invalid_input("read_qvd : chemin NULL"))?;
        let paths = expand_glob(&pattern).map_err(err)?;

        let schema = qvd::read_schema(&paths[0]).map_err(err)?;
        for (name, &kind) in schema.names.iter().zip(&schema.kinds) {
            bind.add_result_column(name, &logical_type(kind, context)?)?;
        }

        Ok(ReadQvdBindData { paths, names: schema.names, kinds: schema.kinds })
    }

    /// Prépare le scan streaming sur les seules colonnes projetées (aucun
    /// décodage de l'index ici). Un seul thread : le scan est séquentiel.
    fn init_global_state(
        &self,
        bind: &Self::BindData,
        _context: &Context,
        columns: InitColumnData<'_>,
    ) -> Result<(Option<Self::GlobalState>, Option<usize>)> {
        let indices = (0..columns.count()?)
            .map(|i| columns.get_column_index(i))
            .collect::<Result<Vec<_>>>()?;
        let scan = QvdScan::new(&bind.paths, &bind.names, &bind.kinds, &indices).map_err(err)?;
        Ok((Some(scan), Some(1)))
    }

    /// Tire un chunk du scan streaming et le copie dans la sortie DuckDB.
    /// Un chunk vide signale la fin.
    fn exec(
        &self,
        _bind: &Self::BindData,
        scan: Option<&Self::GlobalState>,
        _local: Option<&mut Self::LocalState>,
        _context: &Context,
        mut output: DataChunkRef<'_>,
        _columns: ExecColumnInfo<'_>,
    ) -> Result<()> {
        let scan = scan.ok_or_else(|| Error::internal("read_qvd : état de scan absent"))?;
        match scan.pull().map_err(err)? {
            Pull::Done => {}
            // DuckDB demande toujours au moins une colonne (même pour COUNT(*)) :
            // ce cas ne survient pas, mais reste géré par `QvdScan`.
            Pull::Rows(_) => {}
            Pull::Cells { columns } => {
                for (j, &kind) in scan.output_kinds().iter().enumerate() {
                    write_cells(&mut output, j, kind, &columns[j])?;
                }
            }
        }
        Ok(())
    }
}

/// Type logique DuckDB d'une catégorie de colonne QVD.
pub(crate) fn logical_type(kind: Kind, context: &Context) -> Result<LogicalType> {
    match kind {
        Kind::Int => i64::logical_type(context),
        Kind::Float => f64::logical_type(context),
        Kind::Text => String::logical_type(context),
        Kind::Date => DateValue::logical_type(context),
        Kind::Timestamp => TimestampValue::logical_type(context),
        Kind::Time => TimeValue::logical_type(context),
        Kind::Interval => IntervalValue::logical_type(context),
    }
}

/// Écrit une colonne de `Cell` (déjà convertis) dans le vecteur `j` de la
/// sortie ; `Cell::Null` (ou une valeur d'un autre type) donne NULL.
pub(crate) fn write_cells(output: &mut DataChunkRef<'_>, j: usize, kind: Kind, cells: &[Cell]) -> Result<()> {
    match kind {
        Kind::Text => {
            let mut v = output.get_vector_at_mut::<String>(j)?;
            v.set_size(cells.len())?;
            let mut w = v.writer()?;
            for (r, c) in cells.iter().enumerate() {
                w.set(r, if let Cell::Str(s) = c { Some(s.as_str()) } else { None })?;
            }
            Ok(())
        }
        Kind::Int => write_typed::<i64>(output, j, cells, |c| if let Cell::I64(x) = c { Some(*x) } else { None }),
        Kind::Float => write_typed::<f64>(output, j, cells, |c| if let Cell::F64(x) = c { Some(*x) } else { None }),
        Kind::Date => write_typed::<DateValue>(output, j, cells, |c| if let Cell::I32(x) = c { Some(DateValue(*x)) } else { None }),
        Kind::Timestamp => write_typed::<TimestampValue>(output, j, cells, |c| if let Cell::I64(x) = c { Some(TimestampValue(*x)) } else { None }),
        Kind::Time => write_typed::<TimeValue>(output, j, cells, |c| if let Cell::I64(x) = c { Some(TimeValue(*x)) } else { None }),
        Kind::Interval => write_typed::<IntervalValue>(output, j, cells, |c| match c {
            Cell::Interval(x) => Some(IntervalValue { months: x.months, days: x.days, micros: x.micros }),
            _ => None,
        }),
    }
}

fn write_typed<T>(output: &mut DataChunkRef<'_>, j: usize, cells: &[Cell], f: impl Fn(&Cell) -> Option<T>) -> Result<()>
where
    T: VectorElement + duckdb_neo::vector::WritableVectorElement,
    for<'a> T: duckdb_neo::vector::WritableVectorElement<Write<'a> = T>,
{
    let mut v = output.get_vector_at_mut::<T>(j)?;
    v.set_size(cells.len())?;
    let mut w = v.writer()?;
    for (r, c) in cells.iter().enumerate() {
        w.set(r, f(c))?;
    }
    Ok(())
}

/// Convertit une erreur quelconque (qvdrs, glob, OpenQVD…) en erreur DuckDB.
pub(crate) fn err(e: impl Display) -> Error {
    Error::invalid_input(e.to_string())
}

/// Déploie un motif glob en liste de fichiers triée. Un chemin littéral sans
/// métacaractère se résout en lui-même (s'il existe). Erreur si aucun match.
fn expand_glob(pattern: &str) -> std::result::Result<Vec<String>, Box<dyn std::error::Error>> {
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

/// Point d'entrée (`qvd_init_c_api_v2`) : enregistre la table function
/// `read_qvd` et la copy function `qvd` (COPY TO et COPY FROM).
#[duckdb_entrypoint_c_api_v2(extension_name = "qvd", api_version = "v2.0.0")]
fn entrypoint(extension: &Extension) -> Result<()> {
    TableFunctionBuilder::new(
        "read_qvd",
        SignatureBuilder::new().param(FunctionParameter::new::<String>("path")),
        ReadQvd,
    )
    .with_projection_pushdown(true)
    .register(extension)?;

    CopyFunctionBuilder::new("qvd", copy::QvdFormat).register_to_and_from(extension)?;
    Ok(())
}
