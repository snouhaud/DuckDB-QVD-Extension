//! Lecture : `COPY table FROM 'fichier.qvd' (FORMAT qvd)`.
//!
//! Volet COPY FROM de la copy function `qvd` (C API v2). DuckDB fournit les
//! colonnes de la **table cible** : les champs du QVD leur sont associés par
//! position, lus via le scan streaming de [`crate::qvd`], puis écrits dans le
//! type de la colonne cible (directement si le type correspond, sinon via un
//! cast DuckDB).

use duckdb_neo::{
    connection::{Context, FFILink},
    copy_function::{CopyFromBindInfo, CopyFromCardinality, CopyFromFunctionCallbacks},
    data_chunk::DataChunkRef,
    logical_type::LogicalType,
    value::{Value, ValueInput},
    Error, Result,
};

use crate::copy::QvdFormat;
use crate::qvd::{self, Cell, Kind, Pull, QvdScan};
use crate::{err, logical_type, write_cells};

/// Données de bind : fichier, champs lus (les `n` premiers, `n` = nombre de
/// colonnes cibles) et, pour chacun, le type cible quand il diffère du type lu.
pub(crate) struct CopyFromBindData {
    path: String,
    names: Vec<String>,
    kinds: Vec<Kind>,
    /// Type cible (texte SQL) quand un cast est nécessaire, `None` sinon.
    casts: Vec<Option<String>>,
}

impl CopyFromFunctionCallbacks for QvdFormat {
    type BindData = CopyFromBindData;
    type GlobalState = QvdScan;
    type LocalState = ();

    fn bind(
        &self,
        context: &Context,
        file_path: &str,
        info: &CopyFromBindInfo,
    ) -> Result<(Self::BindData, Option<CopyFromCardinality>)> {
        let schema = qvd::read_schema(file_path).map_err(err)?;
        let n = info.column_count()?;
        if schema.names.len() < n {
            return Err(Error::invalid_input(format!(
                "COPY FROM (FORMAT qvd) : le fichier a {} champs pour {n} colonnes cibles",
                schema.names.len()
            )));
        }

        let mut casts = Vec::with_capacity(n);
        for (i, &kind) in schema.kinds.iter().take(n).enumerate() {
            let (_, target) = info.get_column(i)?;
            let same = target.id()? == logical_type(kind, context)?.id()?;
            casts.push(if same { None } else { Some(target.to_text()?) });
        }

        let bind = CopyFromBindData {
            path: file_path.to_string(),
            names: schema.names.into_iter().take(n).collect(),
            kinds: schema.kinds.into_iter().take(n).collect(),
            casts,
        };
        Ok((bind, None))
    }

    /// Scan streaming de tous les champs retenus. Un seul thread.
    fn init_global_state(
        &self,
        bind: &Self::BindData,
        _context: &Context,
    ) -> Result<(Option<Self::GlobalState>, Option<std::num::NonZeroUsize>)> {
        let indices: Vec<usize> = (0..bind.kinds.len()).collect();
        let scan = QvdScan::new(std::slice::from_ref(&bind.path), &bind.names, &bind.kinds, &indices).map_err(err)?;
        Ok((Some(scan), std::num::NonZeroUsize::new(1)))
    }

    /// Un lot vide signale la fin de la lecture.
    fn exec(
        &self,
        bind: &Self::BindData,
        scan: Option<&Self::GlobalState>,
        _local: Option<&mut Self::LocalState>,
        context: &Context,
        mut output: DataChunkRef<'_>,
    ) -> Result<()> {
        let scan = scan.ok_or_else(|| Error::internal("COPY FROM qvd : état de scan absent"))?;
        let Pull::Cells { columns } = scan.pull().map_err(err)? else {
            return Ok(());
        };
        for (j, cells) in columns.iter().enumerate() {
            let kind = bind.kinds[j];
            match &bind.casts[j] {
                None => write_cells(&mut output, j, kind, cells)?,
                Some(target) => {
                    let target = LogicalType::from_text(context, target)?;
                    let [mut v] = output.get_vector_array_mut([j])?;
                    v.set_size(cells.len())?;
                    for (r, cell) in cells.iter().enumerate() {
                        match cell_value(context, kind, cell)? {
                            Some(value) => v.write_value_slow(r, value.cast(context, &target)?)?,
                            None => v.set_null_slow(r)?,
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

/// Valeur DuckDB d'une cellule (avant cast vers le type cible) ; `None` = NULL.
fn cell_value(context: &Context, kind: Kind, cell: &Cell) -> Result<Option<Value>> {
    let input = match (kind, cell) {
        (_, Cell::Null) => return Ok(None),
        (Kind::Int, Cell::I64(x)) => ValueInput::BigInt(*x),
        (Kind::Timestamp, Cell::I64(x)) => ValueInput::Timestamp(*x),
        (Kind::Time, Cell::I64(x)) => ValueInput::Time(*x),
        (_, Cell::F64(x)) => ValueInput::Double(*x),
        (_, Cell::Str(s)) => ValueInput::Varchar(s),
        (_, Cell::I32(x)) => ValueInput::Date(*x),
        (_, Cell::Interval(x)) => ValueInput::Interval { months: x.months, days: x.days, micros: x.micros },
        (_, Cell::I64(x)) => ValueInput::BigInt(*x),
    };
    context.create_value(input).map(Some)
}
