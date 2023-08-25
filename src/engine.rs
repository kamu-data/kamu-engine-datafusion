use std::path::Path;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::error::DataFusionError;
use datafusion::logical_expr as expr;
use datafusion::logical_expr::expr::WindowFunction;
use datafusion::logical_expr::{CreateView, DdlStatement, LogicalPlan};
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
use datafusion::sql::TableReference;
use internal_error::*;
use opendatafabric::engine::ExecuteQueryError;
use opendatafabric::*;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct Engine {}

impl Engine {
    pub async fn new() -> Self {
        Self {}
    }

    pub async fn execute_query(
        &self,
        request: ExecuteQueryRequest,
    ) -> Result<ExecuteQueryResponseSuccess, ExecuteQueryError> {
        let Transform::Sql(transform) = request.transform.clone();
        let transform = transform.normalize_queries(Some(request.dataset_name.to_string()));
        let vocab = DatasetVocabularyResolved::from(&request.vocab);

        let cfg = SessionConfig::new()
            .with_information_schema(true)
            .with_default_catalog_and_schema("odf", "odf")
            .with_target_partitions(1);

        let ctx = SessionContext::with_config(cfg);

        // Setup inputs
        for input in &request.inputs {
            Self::register_input(&ctx, input).await?;
        }

        // Setup queries
        for step in transform.queries.as_ref().unwrap() {
            Self::register_view_for_step(&ctx, step).await?;
        }

        // Get result's execution plan
        let df = ctx
            .table(TableReference::bare(request.dataset_name.as_str()))
            .await
            .int_err()?;

        let df = Self::normalize_raw_result(df)?;

        Self::validate_raw_result(&df, &vocab)?;

        let df = Self::with_system_columns(df, &vocab, request.system_time, request.offset)
            .await
            .int_err()?;

        let num_rows = Self::write_parquet_single_file(df, &request.out_data_path).await?;

        let output_watermark = Self::compute_output_watermark(&request);

        if num_rows == 0 {
            Ok(ExecuteQueryResponseSuccess {
                data_interval: None,
                output_watermark,
            })
        } else {
            Ok(ExecuteQueryResponseSuccess {
                data_interval: Some(OffsetInterval {
                    start: request.offset,
                    end: request.offset + num_rows - 1,
                }),
                output_watermark,
            })
        }
    }

    async fn register_input(
        ctx: &SessionContext,
        input: &ExecuteQueryInput,
    ) -> Result<(), ExecuteQueryError> {
        use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};

        assert!(
            (input.data_paths.is_empty() && input.data_interval.is_none())
                || (!input.data_paths.is_empty() && input.data_interval.is_some())
        );

        // Read raw parquet schema
        // TODO: Use async reader?
        let parquet_reader =
            SerializedFileReader::new(std::fs::File::open(&input.schema_file).int_err()?)
                .int_err()?;
        let parquet_schema = parquet_reader.metadata().file_metadata().schema();
        let mut parquet_schema_str = Vec::new();
        datafusion::parquet::schema::printer::print_schema(&mut parquet_schema_str, parquet_schema);
        let parquet_schema_str = String::from_utf8(parquet_schema_str).unwrap();

        tracing::info!(
            name = %input.dataset_name,
            parquet_schema = %parquet_schema_str,
            "Raw parquet input schema",
        );

        // The input might not have any data to read - in this case we use the schema
        // file as an input but will filter out all rows from it so it acts as
        // an empty table but with correct schema
        let data_fallback = [input.schema_file.clone()];
        let data_paths = if input.data_paths.is_empty() {
            &data_fallback
        } else {
            &input.data_paths[..]
        };

        let input_files: Vec<_> = data_paths
            .iter()
            .map(|p| {
                assert!(p.is_absolute());
                p.to_str().unwrap().to_string()
            })
            .collect();

        let df = ctx
            .read_parquet(
                input_files,
                ParquetReadOptions {
                    file_extension: "",
                    table_partition_cols: Vec::new(),
                    parquet_pruning: None,
                    skip_metadata: None,
                },
            )
            .await
            .int_err()?;

        tracing::info!(
            name = %input.dataset_name,
            schema = ?df.schema(),
            "Registering input",
        );

        let vocab = DatasetVocabularyResolved::from(&input.vocab);
        let df = if let Some(offset_interval) = &input.data_interval {
            df.filter(and(
                col(&vocab.offset_column as &str).gt_eq(lit(offset_interval.start)),
                col(&vocab.offset_column as &str).lt_eq(lit(offset_interval.end)),
            ))
            .int_err()?
        } else {
            df.filter(lit(false)).int_err()?
        };

        ctx.register_table(
            TableReference::bare(input.dataset_name.as_str()),
            df.into_view(),
        )
        .int_err()?;

        Ok(())
    }

    async fn register_view_for_step(
        ctx: &SessionContext,
        step: &SqlQueryStep,
    ) -> Result<(), ExecuteQueryError> {
        let name = step.alias.as_ref().unwrap();

        tracing::info!(
            %name,
            query = %step.query,
            "Creating view for a query",
        );

        let logical_plan = match ctx.state().create_logical_plan(&step.query).await {
            Ok(plan) => plan,
            Err(error) => {
                tracing::debug!(?error, query = %step.query, "Error when setting up query");
                return Err(ExecuteQueryResponseInvalidQuery {
                    message: error.to_string(),
                }
                .into());
            }
        };

        let create_view = LogicalPlan::Ddl(DdlStatement::CreateView(CreateView {
            name: TableReference::bare(step.alias.as_ref().unwrap()).to_owned_reference(),
            input: Arc::new(logical_plan),
            or_replace: false,
            definition: Some(step.query.clone()),
        }));

        ctx.execute_logical_plan(create_view).await.int_err()?;
        Ok(())
    }

    // Computes output watermark as minimum of all inputs
    // This will change when we add support for aggregation, joins and other
    // streaming operations
    fn compute_output_watermark(request: &ExecuteQueryRequest) -> Option<DateTime<Utc>> {
        let watermark = request
            .inputs
            .iter()
            .filter_map(|i| i.explicit_watermarks.iter().map(|wm| &wm.event_time).max())
            .min()
            .map(|dt| dt.clone());

        tracing::info!(?watermark, "Computed ouptut watermark");

        watermark
    }

    // TODO: This function currently ensures that all timestamps in the ouput are
    // represeted as `Timestamp(Millis, "UTC")` for compatibility with other engines
    // (e.g. Flink does not support event time with nanosecond precision).
    fn normalize_raw_result(df: DataFrame) -> Result<DataFrame, ExecuteQueryError> {
        let utc_tz: Arc<str> = Arc::from("UTC");

        let mut select: Vec<Expr> = Vec::new();
        let mut noop = true;

        for field in df.schema().fields() {
            let expr = match field.data_type() {
                DataType::Timestamp(TimeUnit::Millisecond, Some(tz)) if tz.as_ref() == "UTC" => {
                    col(field.unqualified_column())
                }
                DataType::Timestamp(_, _) => {
                    noop = false;
                    cast(
                        col(field.unqualified_column()),
                        DataType::Timestamp(TimeUnit::Millisecond, Some(utc_tz.clone())),
                    )
                    .alias(field.name())
                }
                _ => col(field.unqualified_column()),
            };
            select.push(expr);
        }

        if noop {
            Ok(df)
        } else {
            Ok(df.select(select).int_err()?)
        }
    }

    fn validate_raw_result(
        df: &DataFrame,
        vocab: &DatasetVocabularyResolved<'_>,
    ) -> Result<(), ExecuteQueryError> {
        tracing::info!(schema = ?df.schema(), "Computed raw result schema");

        let system_columns = [&vocab.offset_column, &vocab.system_time_column];
        for system_column in system_columns {
            if df.schema().has_column_with_unqualified_name(system_column) {
                return Err(ExecuteQueryResponseInvalidQuery {
                    message: format!(
                        "Transformed data contains a column that conflicts with the system column \
                         name, you should either rename the data column or configure the dataset \
                         vocabulary to use a different name: {}",
                        system_column
                    ),
                }
                .into());
            }
        }

        let event_time_col = df
            .schema()
            .fields()
            .iter()
            .find(|f| f.name().as_str() == vocab.event_time_column);

        if let Some(event_time_col) = event_time_col {
            match event_time_col.data_type() {
                DataType::Date32 | DataType::Date64 => {}
                DataType::Timestamp(_, None) => {
                    return Err(ExecuteQueryResponseInvalidQuery {
                        message: format!(
                            "Event time column '{}' should be adjusted to UTC, but local/naive \
                             timestamp found",
                            vocab.event_time_column
                        ),
                    }
                    .into());
                }
                DataType::Timestamp(_, Some(tz)) => match tz as &str {
                    "+00:00" | "UTC" => {}
                    tz => {
                        // TODO: Is this restriction necessary?
                        // Datafusion has very sane (metadata-only) approach to storing timezones.
                        // The fear currently is about compatibility with engines like Spark/Flink
                        // that might interpret it incorrectly. This has to be tested further.
                        return Err(ExecuteQueryResponseInvalidQuery {
                            message: format!(
                                "Event time column '{}' should be adjusted to UTC, but found: {}",
                                vocab.event_time_column, tz
                            ),
                        }
                        .into());
                    }
                },
                typ => {
                    return Err(ExecuteQueryResponseInvalidQuery {
                        message: format!(
                            "Event time column '{}' should be either Date or Timestamp, but \
                             found: {}",
                            vocab.event_time_column, typ
                        ),
                    }
                    .into());
                }
            }
        } else {
            return Err(ExecuteQueryResponseInvalidQuery {
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
            }
            .into());
        }

        Ok(())
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
                DataType::Int64,
            ),
        )?;

        let df = df.with_column(
            &vocab.system_time_column,
            Expr::Literal(ScalarValue::TimestampMillisecond(
                Some(system_time.timestamp_millis()),
                Some("UTC".into()),
            )),
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

        tracing::info!(schema = ?df.schema(), "Computed final result schema");
        Ok(df)
    }

    async fn write_parquet_single_file(
        df: DataFrame,
        path: &Path,
    ) -> Result<i64, ExecuteQueryError> {
        use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};

        tracing::info!(?path, "Writing result to parquet");

        // Produces a directory of "part-X.parquet" files
        // We configure to only produce one partition
        df.write_parquet(path.as_os_str().to_str().unwrap(), None)
            .await
            .int_err()?;

        assert_eq!(
            1,
            path.read_dir().unwrap().into_iter().count(),
            "write_parquet produced more than one file"
        );

        let tmp_path = path.with_extension("tmp");
        std::fs::rename(path.join("part-0.parquet"), &tmp_path).int_err()?;
        std::fs::remove_dir(path).int_err()?;
        std::fs::rename(tmp_path, path).int_err()?;

        // Read file back and use metadata to understand how many rows were written
        let reader = SerializedFileReader::new(std::fs::File::open(path).int_err()?).int_err()?;

        let metadata = reader.metadata();
        let num_rows: i64 = reader.metadata().file_metadata().num_rows();

        // Print metadata for debugging
        let metadata = {
            let mut metadata_buf = Vec::new();
            datafusion::parquet::schema::printer::print_parquet_metadata(
                &mut metadata_buf,
                metadata,
            );
            String::from_utf8(metadata_buf).unwrap()
        };

        tracing::info!(%metadata, num_rows, "Wrote data to parquet file");

        if num_rows == 0 {
            std::fs::remove_file(path).int_err()?;
        }

        Ok(num_rows)
    }
}
