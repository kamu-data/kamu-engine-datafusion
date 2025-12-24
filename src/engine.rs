use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::config::{ParquetColumnOptions, ParquetOptions, TableParquetOptions};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::error::DataFusionError;
use datafusion::logical_expr as expr;
use datafusion::logical_expr::expr::WindowFunction;
use datafusion::logical_expr::{CreateView, DdlStatement, LogicalPlan};
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
use datafusion::sql::TableReference;
use internal_error::*;
use opendatafabric::engine::{ExecuteRawQueryError, ExecuteTransformError};
use opendatafabric::*;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct Engine {}

impl Engine {
    const OUTPUT_VIEW_NAME: &'static str = "__output__";

    pub async fn new() -> Self {
        Self {}
    }

    fn new_context(&self) -> SessionContext {
        let mut cfg = SessionConfig::new()
            .with_information_schema(true)
            .with_default_catalog_and_schema("odf", "odf")
            .with_target_partitions(1);

        // Forcing cese-sensitive identifiers in case-insensitive language seems to
        // be a lesser evil than following DataFusion's default behavior of forcing
        // identifiers to lowercase instead of case-insensitive matching.
        //
        // See: https://github.com/apache/datafusion/issues/7460
        // TODO: Consider externalizing this config (e.g. by allowing custom engine
        // options in transform DTOs)
        cfg.options_mut().sql_parser.enable_ident_normalization = false;

        let mut ctx = SessionContext::new_with_config(cfg);

        datafusion_functions_json::register_all(&mut ctx).unwrap();

        ctx
    }

    pub async fn execute_raw_query(
        &self,
        request: RawQueryRequest,
    ) -> Result<RawQueryResponseSuccess, ExecuteRawQueryError> {
        let ctx = self.new_context();

        // Setup input
        let input = ctx
            .read_parquet(
                request
                    .input_data_paths
                    .into_iter()
                    .map(|p| p.as_os_str().to_str().unwrap().to_string())
                    .collect::<Vec<_>>(),
                ParquetReadOptions {
                    file_extension: "",
                    ..Default::default()
                },
            )
            .await
            .int_err()?;

        ctx.register_table(TableReference::bare("input"), input.into_view())
            .int_err()?;

        // Setup queries
        let Transform::Sql(transform) = request.transform.clone();
        for step in transform.queries.as_ref().unwrap() {
            match Self::register_view_for_step(
                &ctx,
                step.alias.as_deref().unwrap_or(Self::OUTPUT_VIEW_NAME),
                &step.query,
            )
            .await
            {
                Ok(_) => Ok(()),
                Err(ExecuteTransformError::InvalidQuery(e)) => {
                    Err(ExecuteRawQueryError::InvalidQuery(
                        RawQueryResponseInvalidQuery { message: e.message },
                    ))
                }
                Err(ExecuteTransformError::InternalError(e)) => {
                    Err(ExecuteRawQueryError::InternalError(e))
                }
                Err(ExecuteTransformError::EngineInternalError(_)) => unreachable!(),
            }?;
        }

        let df = ctx.table(Self::OUTPUT_VIEW_NAME).await.int_err()?;
        let df = Self::normalize_raw_result(df, &DatasetVocabulary::default())?;

        let num_records = self
            .write_parquet(
                &request.output_data_path,
                df,
                TableParquetOptions {
                    global: ParquetOptions {
                        writer_version: "1.0".into(),
                        compression: Some("snappy".into()),
                        ..Default::default()
                    },
                    column_specific_options: HashMap::new(),
                    key_value_metadata: HashMap::new(),
                },
            )
            .await?;

        Ok(RawQueryResponseSuccess { num_records })
    }

    pub async fn execute_transform(
        &self,
        request: TransformRequest,
    ) -> Result<TransformResponseSuccess, ExecuteTransformError> {
        let ctx = self.new_context();

        // Setup inputs
        for input in &request.query_inputs {
            Self::register_input(&ctx, input).await?;
        }

        // Setup queries
        let Transform::Sql(transform) = request.transform.clone();
        for step in transform.queries.as_ref().unwrap() {
            Self::register_view_for_step(
                &ctx,
                step.alias.as_deref().unwrap_or(Self::OUTPUT_VIEW_NAME),
                &step.query,
            )
            .await?;
        }

        // Get result's execution plan
        let df = ctx.table(Self::OUTPUT_VIEW_NAME).await.int_err()?;
        tracing::info!(schema = ?df.schema(), "Raw result schema");

        let df = Self::normalize_raw_result(df, &request.vocab)?;
        tracing::info!(schema = ?df.schema(), "Normalized result schema");

        Self::validate_raw_result(&df, &request.vocab)?;

        let df =
            Self::with_system_columns(df, &request.vocab, request.system_time, request.next_offset)
                .await
                .int_err()?;

        let num_rows = self
            .write_parquet(
                &request.new_data_path,
                df,
                self.get_writer_properties(&request.vocab),
            )
            .await?;

        let new_watermark = Self::compute_new_watermark(&request);

        Ok(TransformResponseSuccess {
            new_offset_interval: if num_rows != 0 {
                Some(OffsetInterval {
                    start: request.next_offset,
                    end: request.next_offset + num_rows - 1,
                })
            } else {
                None
            },
            new_watermark,
        })
    }

    #[tracing::instrument(level = "info", skip_all, fields(dataset_id = %input.dataset_id, dataset_alias = %input.dataset_alias, query_alias = %input.query_alias))]
    async fn register_input(
        ctx: &SessionContext,
        input: &TransformRequestInput,
    ) -> Result<(), ExecuteTransformError> {
        use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};

        assert!(
            (input.data_paths.is_empty() && input.offset_interval.is_none())
                || (!input.data_paths.is_empty() && input.offset_interval.is_some())
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
                    // TODO: Schema evolution
                    schema: None,
                    file_extension: "",
                    // TODO: Perf specifying `offset` sort order may improve some queries
                    file_sort_order: Vec::new(),
                    table_partition_cols: Vec::new(),
                    parquet_pruning: None,
                    skip_metadata: None,
                },
            )
            .await
            .int_err()?;

        tracing::info!(
            query_alias = %input.query_alias,
            arrow_schema = ?df.schema(),
            "Registering input",
        );

        let df = if let Some(offset_interval) = &input.offset_interval {
            df.filter(and(
                col(Column::from_name(&input.vocab.offset_column))
                    .gt_eq(lit(offset_interval.start)),
                col(Column::from_name(&input.vocab.offset_column)).lt_eq(lit(offset_interval.end)),
            ))
            .int_err()?
        } else {
            df.filter(lit(false)).int_err()?
        };

        ctx.register_table(
            TableReference::bare(input.query_alias.as_str()),
            df.into_view(),
        )
        .int_err()?;

        Ok(())
    }

    async fn register_view_for_step(
        ctx: &SessionContext,
        name: &str,
        query: &str,
    ) -> Result<(), ExecuteTransformError> {
        tracing::info!(
            view_name = %name,
            query = %query,
            "Creating view for a query",
        );

        let logical_plan = match ctx.state().create_logical_plan(query).await {
            Ok(plan) => plan,
            Err(error) => {
                tracing::debug!(?error, query = %query, "Error when setting up query");
                return Err(TransformResponseInvalidQuery {
                    message: error.to_string(),
                }
                .into());
            }
        };

        let create_view = LogicalPlan::Ddl(DdlStatement::CreateView(CreateView {
            name: TableReference::bare(name),
            input: Arc::new(logical_plan),
            or_replace: false,
            definition: Some(query.to_string()),
        }));

        ctx.execute_logical_plan(create_view).await.int_err()?;
        Ok(())
    }

    // Computes new watermark as minimum of all inputs
    // This will change when we add support for aggregation, joins and other
    // streaming operations
    fn compute_new_watermark(request: &TransformRequest) -> Option<DateTime<Utc>> {
        let watermark = request
            .query_inputs
            .iter()
            .filter_map(|i| i.explicit_watermarks.iter().map(|wm| &wm.event_time).max())
            .min()
            .copied();

        tracing::info!(?watermark, "Computed ouptut watermark");

        watermark
    }

    // TODO: This function currently ensures that all timestamps in the ouput are
    // represeted as `Timestamp(Millis, "UTC")` for compatibility with other engines
    // (e.g. Flink does not support event time with nanosecond precision).
    fn normalize_raw_result(
        df: DataFrame,
        vocab: &DatasetVocabulary,
    ) -> Result<DataFrame, InternalError> {
        let utc_tz: Arc<str> = Arc::from("UTC");

        let mut select: Vec<Expr> = Vec::new();
        let mut noop = true;

        for field in df.schema().fields() {
            let expr = match field.data_type() {
                DataType::Timestamp(TimeUnit::Millisecond, Some(tz)) if tz.as_ref() == "UTC" => {
                    col(Column::from_name(field.name()))
                }
                DataType::Timestamp(_, _) => {
                    noop = false;
                    cast(
                        col(Column::from_name(field.name())),
                        DataType::Timestamp(TimeUnit::Millisecond, Some(utc_tz.clone())),
                    )
                    .alias(field.name())
                }
                // TODO: Normalize towards UInt8 after Spark is updated
                // See: https://github.com/kamu-data/kamu-cli/issues/445
                DataType::Int8
                | DataType::UInt8
                | DataType::Int16
                | DataType::UInt16
                | DataType::UInt32
                    if *field.name() == vocab.operation_type_column =>
                {
                    noop = false;
                    cast(col(Column::from_name(field.name())), DataType::Int32).alias(field.name())
                }
                _ => col(Column::from_name(field.name())),
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
        vocab: &DatasetVocabulary,
    ) -> Result<(), ExecuteTransformError> {
        let system_columns = [&vocab.offset_column, &vocab.system_time_column];
        for system_column in system_columns {
            if df.schema().has_column_with_unqualified_name(system_column) {
                return Err(TransformResponseInvalidQuery {
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

        if let Some(op_col) = df
            .schema()
            .fields_with_unqualified_name(&vocab.operation_type_column)
            .first()
        {
            match op_col.data_type() {
                // TODO: Require UInt8 after Spark is updated
                // See: https://github.com/kamu-data/kamu-cli/issues/445
                DataType::Int32 => {}
                typ => {
                    return Err(TransformResponseInvalidQuery {
                        message: format!(
                            "Operation type column '{}' should be Int32, but found: {}",
                            vocab.operation_type_column, typ
                        ),
                    }
                    .into());
                }
            }
        }

        if let Some(event_time_col) = df
            .schema()
            .fields_with_unqualified_name(&vocab.event_time_column)
            .first()
        {
            match event_time_col.data_type() {
                DataType::Date32 | DataType::Date64 => {}
                DataType::Timestamp(_, None) => {
                    return Err(TransformResponseInvalidQuery {
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
                        return Err(TransformResponseInvalidQuery {
                            message: format!(
                                "Event time column '{}' should be adjusted to UTC, but found: {}",
                                vocab.event_time_column, tz
                            ),
                        }
                        .into());
                    }
                },
                typ => {
                    return Err(TransformResponseInvalidQuery {
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
            return Err(TransformResponseInvalidQuery {
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
        vocab: &DatasetVocabulary,
        system_time: DateTime<Utc>,
        start_offset: u64,
    ) -> Result<DataFrame, DataFusionError> {
        // Collect non-system column names for later
        let mut data_columns: Vec<_> = df
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .filter(|n| {
                n.as_str() != vocab.event_time_column && n.as_str() != vocab.operation_type_column
            })
            .collect();

        // Offset
        // TODO: For some reason this adds two collumns: the expected "offset", but also
        // "ROW_NUMBER()" for now we simply filter out the latter.
        let df = df.with_column(
            &vocab.offset_column,
            Expr::WindowFunction(WindowFunction {
                fun: expr::WindowFunctionDefinition::BuiltInWindowFunction(
                    expr::BuiltInWindowFunction::RowNumber,
                ),
                args: vec![],
                partition_by: vec![],
                // TODO: Can this potentially lead to reordering?
                order_by: vec![Expr::Literal(ScalarValue::Null).sort(true, false)],
                window_frame: expr::WindowFrame::new(Some(false)),
                null_treatment: None,
            }),
        )?;

        // TODO: Cast to UInt64 after Spark is updated
        // See: https://github.com/kamu-data/kamu-cli/issues/445
        let df = df.with_column(
            &vocab.offset_column,
            cast(
                col(Column::from_name(&vocab.offset_column)) + lit(start_offset as i64 - 1),
                DataType::Int64,
            ),
        )?;

        // Operation type
        let df = if !df
            .schema()
            .has_column_with_unqualified_name(&vocab.operation_type_column)
        {
            df.with_column(
                &vocab.operation_type_column,
                // TODO: Cast to u8 after Spark is updated
                // See: https://github.com/kamu-data/kamu-cli/issues/445
                lit(OperationType::Append as i32),
            )?
        } else {
            df
        };

        // System time
        let df = df.with_column(
            &vocab.system_time_column,
            Expr::Literal(ScalarValue::TimestampMillisecond(
                Some(system_time.timestamp_millis()),
                Some("UTC".into()),
            )),
        )?;

        // Reorder columns for nice looks
        let mut full_columns = vec![
            vocab.offset_column.clone(),
            vocab.operation_type_column.clone(),
            vocab.system_time_column.clone(),
            vocab.event_time_column.clone(),
        ];
        full_columns.append(&mut data_columns);
        let full_columns_str: Vec<_> = full_columns.iter().map(String::as_str).collect();

        let df = df.select_columns(&full_columns_str)?;

        tracing::info!(schema = ?df.schema(), "Computed final result schema");
        Ok(df)
    }

    // TODO: Externalize configuration
    fn get_writer_properties(&self, vocab: &DatasetVocabulary) -> TableParquetOptions {
        // TODO: `offset` column is sorted integers so we could use delta encoding, but
        // Flink does not support it.
        // See: https://github.com/kamu-data/kamu-engine-flink/issues/3
        TableParquetOptions {
            global: ParquetOptions {
                writer_version: "1.0".into(),
                compression: Some("snappy".into()),
                ..Default::default()
            },
            column_specific_options: HashMap::from([
                (
                    // op column is low cardinality and best encoded as RLE_DICTIONARY
                    vocab.operation_type_column.clone(),
                    ParquetColumnOptions {
                        dictionary_enabled: Some(true),
                        ..Default::default()
                    },
                ),
                (
                    vocab.system_time_column.clone(),
                    ParquetColumnOptions {
                        // system_time value will be the same for all rows in a batch
                        dictionary_enabled: Some(true),
                        ..Default::default()
                    },
                ),
            ]),
            key_value_metadata: HashMap::new(),
        }
    }

    async fn write_parquet(
        &self,
        path: &Path,
        df: DataFrame,
        props: TableParquetOptions,
    ) -> Result<u64, InternalError> {
        use datafusion::arrow::array::UInt64Array;

        tracing::info!(?path, "Writing result to parquet");

        let res = df
            .write_parquet(
                path.as_os_str().to_str().unwrap(),
                DataFrameWriteOptions::new().with_single_file_output(true),
                Some(props),
            )
            .await
            .int_err()?;

        assert_eq!(res.len(), 1);
        assert_eq!(res[0].num_columns(), 1);
        assert_eq!(res[0].num_rows(), 1);
        let num_records = res[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0);

        tracing::info!(?path, num_records, "Produced parquet file");
        Ok(num_records as u64)
    }
}
