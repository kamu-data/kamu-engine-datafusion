use std::path::Path;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::DataFusionError;
use datafusion::logical_expr as expr;
use datafusion::logical_expr::expr::WindowFunction;
use datafusion::logical_expr::{CreateView, DdlStatement, LogicalPlan};
use datafusion::prelude::*;
use datafusion::sql::TableReference;
use opendatafabric::*;

use crate::datafusion_hacks::ListingTableOfFiles;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct Engine {}

impl Engine {
    pub async fn new() -> Self {
        Self {}
    }

    // TODO: Error handling
    // TODO: Isolate from GRPC protocol with proper result/error types
    pub async fn execute_query(
        &self,
        request: ExecuteQueryRequest,
    ) -> Result<ExecuteQueryResponse, Box<dyn std::error::Error>> {
        let Transform::Sql(transform) = request.transform;
        let transform = transform.normalize_queries();
        let vocab = DatasetVocabularyResolved::from(&request.vocab);

        let cfg = SessionConfig::new()
            .with_information_schema(true)
            .with_default_catalog_and_schema("odf", "odf")
            .with_target_partitions(1);

        let ctx = SessionContext::with_config(cfg);

        // Setup inputs
        for input in &request.inputs {
            let table = ListingTableOfFiles::try_new(
                // TODO: Is state snapshotting correct?
                &ctx.state(),
                input
                    .data_paths
                    .iter()
                    .map(|p| p.to_string_lossy().into())
                    .collect(),
            )
            .await?;

            ctx.register_table(
                TableReference::bare(input.dataset_name.as_str()),
                Arc::new(table),
            )?;
        }

        // Setup queries
        for step in transform.queries.unwrap() {
            let logical_plan = match ctx.state().create_logical_plan(&step.query).await {
                Ok(plan) => plan,
                Err(error) => {
                    tracing::debug!(?error, query = %step.query, "Error when setting up query");
                    return Ok(ExecuteQueryResponse::InvalidQuery(
                        ExecuteQueryResponseInvalidQuery {
                            message: error.to_string(),
                        },
                    ));
                }
            };

            let view_name = if let Some(alias) = &step.alias {
                alias.as_str()
            } else {
                request.dataset_name.as_str()
            };

            let create_view = LogicalPlan::Ddl(DdlStatement::CreateView(CreateView {
                name: TableReference::bare(view_name).to_owned_reference(),
                input: Arc::new(logical_plan),
                or_replace: false,
                definition: Some(step.query),
            }));

            ctx.execute_logical_plan(create_view).await?;
        }

        let df = ctx
            .table(TableReference::bare(request.dataset_name.as_str()))
            .await?;

        if let Some(error) = Self::validate_raw_result(&df, &vocab) {
            return Ok(error);
        }

        let df = Self::with_system_columns(df, &vocab, request.system_time, request.offset).await?;

        // TODO: compression
        let num_rows = Self::write_parquet_single_file(df, &request.out_data_path).await?;

        // Compute output watermark as minimum of all inputs
        // This will change when we add support for aggregation, joins and other
        // streaming operations
        let output_watermark = request
            .inputs
            .iter()
            .flat_map(|i| i.explicit_watermarks.iter())
            .map(|wm| &wm.event_time)
            .min()
            .map(|dt| dt.clone());

        if num_rows == 0 {
            Ok(ExecuteQueryResponse::Success(ExecuteQueryResponseSuccess {
                data_interval: None,
                output_watermark,
            }))
        } else {
            Ok(ExecuteQueryResponse::Success(ExecuteQueryResponseSuccess {
                data_interval: Some(OffsetInterval {
                    start: request.offset,
                    end: request.offset + num_rows - 1,
                }),
                output_watermark,
            }))
        }
    }

    fn validate_raw_result(
        df: &DataFrame,
        vocab: &DatasetVocabularyResolved<'_>,
    ) -> Option<ExecuteQueryResponse> {
        tracing::info!(schema = ?df.schema(), "Raw result schema computed");

        let system_columns = [&vocab.offset_column, &vocab.system_time_column];
        for system_column in system_columns {
            if df.schema().has_column_with_unqualified_name(system_column) {
                return Some(ExecuteQueryResponse::InvalidQuery(
                    ExecuteQueryResponseInvalidQuery {
                        message: format!(
                            "Transformed data contains a column that conflicts with the system \
                             column name, you should either rename the data column or configure \
                             the dataset vocabulary to use a different name: {}",
                            system_column
                        ),
                    },
                ));
            }
        }

        let event_time_col = df
            .schema()
            .fields()
            .iter()
            .find(|f| f.name().as_str() == vocab.event_time_column);

        if let Some(event_time_col) = event_time_col {
            match event_time_col.data_type() {
                DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) => {}
                typ => {
                    return Some(ExecuteQueryResponse::InvalidQuery(
                        ExecuteQueryResponseInvalidQuery {
                            message: format!(
                                "Event time column should be either Date or Timestamp, found: {}",
                                typ
                            ),
                        },
                    ));
                }
            }
        } else {
            return Some(ExecuteQueryResponse::InvalidQuery(
                ExecuteQueryResponseInvalidQuery {
                    message: format!(
                        "Event time column {} was not found amongst: {}",
                        vocab.event_time_column,
                        df.schema()
                            .fields()
                            .iter()
                            .map(|f| f.name().as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                },
            ));
        }

        None
    }

    async fn with_system_columns(
        df: DataFrame,
        vocab: &DatasetVocabularyResolved<'_>,
        system_time: DateTime<Utc>,
        start_offset: i64,
    ) -> Result<DataFrame, DataFusionError> {
        // Collect non-system column names for later
        let mut raw_columns_wo_event_time: Vec<_> = df
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .filter(|n| n.as_str() != vocab.event_time_column)
            .collect();

        // TODO: For some reason this adds two collumns: the expected "offset", but also
        // "ROW_NUMBER()" for now we simply filter out the latter.
        let df = df.with_column(
            &vocab.offset_column,
            Expr::WindowFunction(WindowFunction {
                fun: expr::WindowFunction::BuiltInWindowFunction(
                    expr::BuiltInWindowFunction::RowNumber,
                ),
                args: vec![],
                partition_by: vec![],
                order_by: vec![],
                window_frame: expr::WindowFrame::new(false),
            }),
        )?;

        let df = df.with_column(
            &vocab.offset_column,
            cast(
                col(&vocab.offset_column as &str) + lit(start_offset - 1),
                DataType::UInt64,
            ),
        )?;

        let df = df.with_column(
            &vocab.system_time_column,
            lit_timestamp_nano(system_time.timestamp_nanos()),
        )?;

        // Reorder columns for nice looks
        let mut full_columns = vec![
            vocab.offset_column.to_string(),
            vocab.system_time_column.to_string(),
            vocab.event_time_column.to_string(),
        ];
        full_columns.append(&mut raw_columns_wo_event_time);
        let full_columns_str: Vec<_> = full_columns.iter().map(String::as_str).collect();

        let df = df.select_columns(&full_columns_str)?;

        tracing::info!(schema = ?df.schema(), "Final result schema formed");
        Ok(df)
    }

    async fn write_parquet_single_file(df: DataFrame, path: &Path) -> Result<i64, DataFusionError> {
        use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};

        // Produces a directory of "part-X.parquet" files
        // We configure to only produce one partition
        df.write_parquet(path.as_os_str().to_str().unwrap(), None)
            .await?;

        assert_eq!(
            1,
            path.read_dir().unwrap().into_iter().count(),
            "write_parquet produced more than one file"
        );

        let tmp_path = path.with_extension("tmp");
        std::fs::rename(path.join("part-0.parquet"), &tmp_path)?;
        std::fs::remove_dir(path)?;
        std::fs::rename(tmp_path, path)?;

        let reader = SerializedFileReader::new(std::fs::File::open(path).unwrap()).unwrap();
        let num_rows: i64 = reader
            .metadata()
            .row_groups()
            .iter()
            .map(|rg| rg.num_rows())
            .sum();

        if num_rows == 0 {
            std::fs::remove_file(path)?;
        }

        Ok(num_rows)
    }
}
