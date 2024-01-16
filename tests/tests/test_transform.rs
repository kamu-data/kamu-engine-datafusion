use std::assert_matches::assert_matches;
use std::path::Path;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use indoc::indoc;
use kamu_engine_datafusion::engine::Engine;
use opendatafabric::engine::ExecuteTransformError;
use opendatafabric::*;
use pretty_assertions::assert_eq;

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
struct Record {
    offset: u64,
    op: OperationType,
    system_time: i64,
    event_time: i64,
    city: String,
    population: u64,
}

impl Record {
    fn new(
        offset: u64,
        op: OperationType,
        system_time: &str,
        event_time: &str,
        city: &str,
        population: u64,
    ) -> Self {
        Self {
            offset,
            op,
            system_time: DateTime::parse_from_rfc3339(system_time)
                .unwrap()
                .timestamp_millis(),
            event_time: DateTime::parse_from_rfc3339(event_time)
                .unwrap()
                .timestamp_millis(),
            city: city.to_string(),
            population,
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

// TODO: Replace with derived arrow-parquet serialization
fn write_sample_data(path: impl AsRef<Path>, data: &[Record]) {
    use datafusion::arrow::array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::arrow::record_batch::RecordBatch;

    // TODO: Replace with UInt64 and UInt8 after Spark is updated
    // See: https://github.com/kamu-data/kamu-cli/issues/445
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            DatasetVocabulary::DEFAULT_OFFSET_COLUMN_NAME,
            DataType::Int64,
            false,
        ),
        Field::new(
            DatasetVocabulary::DEFAULT_OPERATION_TYPE_COLUMN_NAME,
            DataType::Int32,
            false,
        ),
        Field::new(
            DatasetVocabulary::DEFAULT_SYSTEM_TIME_COLUMN_NAME,
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
        Field::new(
            DatasetVocabulary::DEFAULT_EVENT_TIME_COLUMN_NAME,
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
        Field::new("city", DataType::Utf8, false),
        Field::new("population", DataType::UInt64, false),
    ]));

    let record_batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(array::Int64Array::from(
                data.iter().map(|r| r.offset as i64).collect::<Vec<_>>(),
            )),
            Arc::new(array::Int32Array::from(
                data.iter().map(|r| r.op as i32).collect::<Vec<_>>(),
            )),
            Arc::new(
                array::TimestampMillisecondArray::from(
                    data.iter().map(|r| r.system_time).collect::<Vec<_>>(),
                )
                .with_timezone("UTC"),
            ),
            Arc::new(
                array::TimestampMillisecondArray::from(
                    data.iter().map(|r| r.event_time).collect::<Vec<_>>(),
                )
                .with_timezone("UTC"),
            ),
            Arc::new(array::StringArray::from(
                data.iter().map(|r| r.city.clone()).collect::<Vec<_>>(),
            )),
            Arc::new(array::UInt64Array::from(
                data.iter().map(|r| r.population).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();

    use datafusion::parquet::arrow::ArrowWriter;

    let mut arrow_writer = ArrowWriter::try_new(
        std::fs::File::create(path).unwrap(),
        record_batch.schema(),
        None,
    )
    .unwrap();

    arrow_writer.write(&record_batch).unwrap();
    arrow_writer.close().unwrap();
}

///////////////////////////////////////////////////////////////////////////////

async fn read_data_pretty(path: impl AsRef<Path>) -> String {
    use datafusion::arrow::util::pretty;
    use datafusion::prelude::*;

    let ctx = SessionContext::new();
    let df = ctx
        .read_parquet(
            path.as_ref().as_os_str().to_str().unwrap(),
            ParquetReadOptions {
                file_extension: "",
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    pretty::pretty_format_batches(&batches).unwrap().to_string()
}

///////////////////////////////////////////////////////////////////////////////

async fn read_parquet_schema_pretty(path: impl AsRef<Path>) -> String {
    use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};

    let reader = SerializedFileReader::new(std::fs::File::open(path).unwrap()).unwrap();
    let metadata = reader.metadata();
    let schema = metadata.file_metadata().schema();

    let schema = {
        let mut buf = Vec::new();
        datafusion::parquet::schema::printer::print_schema(&mut buf, schema);
        String::from_utf8(buf).unwrap()
    };

    schema
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct TestQueryCommonOpts {
    input_data: Option<Vec<Record>>,
    queries: Option<Vec<SqlQueryStep>>,
    mutate_request: Option<Box<dyn FnOnce(TransformRequest) -> TransformRequest>>,
    check_result: Option<Box<dyn FnOnce(Result<TransformResponseSuccess, ExecuteTransformError>)>>,
    check_data: Option<Box<dyn FnOnce(&Path)>>,
    expected_data: Option<Option<&'static str>>,
    expected_schema: Option<&'static str>,
    expected_watermark: Option<Option<DateTime<Utc>>>,
}

///////////////////////////////////////////////////////////////////////////////

async fn test_query_common(opts: TestQueryCommonOpts) {
    // Test defaults
    let queries = opts.queries.unwrap_or(vec![SqlQueryStep {
        alias: None,
        query: "select event_time, city, population + 100 as population from foo".to_string(),
    }]);
    let expected_data = opts.expected_data.unwrap_or(Some(indoc!(
        r#"
        +--------+----+----------------------+----------------------+-----------+------------+
        | offset | op | system_time          | event_time           | city      | population |
        +--------+----+----------------------+----------------------+-----------+------------+
        | 0      | 0  | 2023-03-01T00:00:00Z | 2023-01-01T00:00:00Z | vancouver | 675100     |
        | 1      | 0  | 2023-03-01T00:00:00Z | 2023-01-01T00:00:00Z | seattle   | 733100     |
        | 2      | 0  | 2023-03-01T00:00:00Z | 2023-01-01T00:00:00Z | kyiv      | 2884100    |
        +--------+----+----------------------+----------------------+-----------+------------+
        "#,
    )));

    // Run test
    let tempdir = tempfile::tempdir().unwrap();

    let new_data_path = tempdir.path().join("output");
    let input_data_path = tempdir.path().join("input");

    let input_data = opts.input_data.unwrap_or_else(|| {
        vec![
            Record::new(
                0,
                OperationType::Append,
                "2023-02-01T00:00:00Z",
                "2023-01-01T00:00:00Z",
                "vancouver",
                675_000,
            ),
            Record::new(
                1,
                OperationType::Append,
                "2023-02-01T00:00:00Z",
                "2023-01-01T00:00:00Z",
                "seattle",
                733_000,
            ),
            Record::new(
                2,
                OperationType::Append,
                "2023-02-01T00:00:00Z",
                "2023-01-01T00:00:00Z",
                "kyiv",
                2_884_000,
            ),
        ]
    });

    write_sample_data(&input_data_path, &input_data);

    let engine = Engine::new().await;

    let request = TransformRequest {
        dataset_id: DatasetID::new_seeded_ed25519(b"bar"),
        dataset_alias: "bar".try_into().unwrap(),
        system_time: DateTime::parse_from_rfc3339("2023-03-01T00:00:00Z")
            .unwrap()
            .into(),
        next_offset: 0,
        vocab: DatasetVocabulary::default(),
        transform: Transform::Sql(TransformSql {
            engine: "datafusion".into(),
            version: None,
            query: None,
            queries: Some(queries),
            temporal_tables: None,
        }),
        query_inputs: vec![TransformRequestInput {
            dataset_id: DatasetID::new_seeded_ed25519(b"foo"),
            dataset_alias: "foo".try_into().unwrap(),
            query_alias: "foo".to_string(),
            offset_interval: if input_data.is_empty() {
                None
            } else {
                Some(OffsetInterval {
                    start: input_data.iter().map(|r| r.offset).min().unwrap(),
                    end: input_data.iter().map(|r| r.offset).max().unwrap(),
                })
            },
            vocab: DatasetVocabulary::default(),
            data_paths: vec![input_data_path.clone()],
            schema_file: input_data_path.clone(),
            explicit_watermarks: vec![],
        }],
        prev_checkpoint_path: None,
        new_checkpoint_path: tempdir.path().join("checkpoint"),
        new_data_path: new_data_path.clone(),
    };
    let request = if let Some(f) = opts.mutate_request {
        f(request)
    } else {
        request
    };

    let actual_result = engine.execute_transform(request).await;

    if let Some(check_result) = opts.check_result {
        check_result(actual_result)
    } else {
        assert_eq!(
            actual_result.unwrap(),
            TransformResponseSuccess {
                new_offset_interval: Some(OffsetInterval { start: 0, end: 2 }),
                new_watermark: opts.expected_watermark.unwrap_or(None),
            }
        );
    }

    if let Some(check_data) = opts.check_data {
        check_data(&new_data_path)
    }

    if let Some(expected_data) = expected_data {
        let actual_data = read_data_pretty(&new_data_path).await;
        assert_eq!(expected_data.trim(), actual_data);
    } else {
        assert!(!new_data_path.exists());
    }

    if let Some(expected_schema) = opts.expected_schema {
        let actual_schema = read_parquet_schema_pretty(&new_data_path).await;
        assert_eq!(expected_schema.trim(), actual_schema.trim());
    }
}

///////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_result_schema() {
    test_query_common(TestQueryCommonOpts {
        expected_schema: Some(indoc!(
            r#"
            message arrow_schema {
              OPTIONAL INT64 offset;
              REQUIRED INT32 op;
              REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
              REQUIRED INT64 event_time (TIMESTAMP(MILLIS,true));
              REQUIRED BYTE_ARRAY city (STRING);
              REQUIRED INT64 population;
            }
            "#,
        )),
        ..Default::default()
    })
    .await
}

///////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_result_optimal_parquet_encoding() {
    test_query_common(TestQueryCommonOpts {
        expected_schema: Some(indoc!(
            r#"
            message arrow_schema {
              OPTIONAL INT64 offset;
              REQUIRED INT32 op;
              REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
              REQUIRED INT64 event_time (TIMESTAMP(MILLIS,true));
              REQUIRED BYTE_ARRAY city (STRING);
              REQUIRED INT64 population;
            }
            "#
        )),
        check_data: Some(Box::new(|path| {
            use datafusion::parquet::basic::{Compression, Encoding, PageType};
            use datafusion::parquet::file::reader::FileReader;
            use datafusion::parquet::file::serialized_reader::SerializedFileReader;

            let reader = SerializedFileReader::new(std::fs::File::open(path).unwrap()).unwrap();
            let meta = reader.metadata();

            // TODO: Migrate to Parquet v2 and DATA_PAGE_V2
            let assert_data_encoding = |col, enc| {
                let data_page = reader
                    .get_row_group(0)
                    .unwrap()
                    .get_column_page_reader(col)
                    .unwrap()
                    .map(|p| p.unwrap())
                    .filter(|p| p.page_type() == PageType::DATA_PAGE)
                    .next()
                    .unwrap();

                assert_eq!(data_page.encoding(), enc);
            };

            assert_eq!(meta.num_row_groups(), 1);

            let offset_col = meta.row_group(0).column(0);
            assert_eq!(offset_col.column_path().string(), "offset");
            assert_eq!(offset_col.compression(), Compression::SNAPPY);

            // TODO: Validate offset is delta-encoded
            // See: https://github.com/kamu-data/kamu-engine-flink/issues/3
            // assert_data_encoding(0, Encoding::DELTA_BINARY_PACKED);

            let op_col = meta.row_group(0).column(1);
            assert_eq!(op_col.column_path().string(), "op");
            assert_eq!(op_col.compression(), Compression::SNAPPY);
            assert_data_encoding(1, Encoding::RLE_DICTIONARY);

            let system_time_col = meta.row_group(0).column(2);
            assert_eq!(system_time_col.column_path().string(), "system_time");
            assert_eq!(system_time_col.compression(), Compression::SNAPPY);
            assert_data_encoding(2, Encoding::RLE_DICTIONARY);
        })),
        ..Default::default()
    })
    .await
}

///////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_chain_of_queries() {
    test_query_common(TestQueryCommonOpts {
        queries: Some(vec![
            SqlQueryStep {
                alias: Some("a".to_string()),
                query: "select event_time, city, population + 100 as population from foo"
                    .to_string(),
            },
            SqlQueryStep {
                alias: None,
                query: "select event_time, city, population + 50 as population from a".to_string(),
            },
        ]),
        expected_data: Some(Some(indoc!(
            r#"
            +--------+----+----------------------+----------------------+-----------+------------+
            | offset | op | system_time          | event_time           | city      | population |
            +--------+----+----------------------+----------------------+-----------+------------+
            | 0      | 0  | 2023-03-01T00:00:00Z | 2023-01-01T00:00:00Z | vancouver | 675150     |
            | 1      | 0  | 2023-03-01T00:00:00Z | 2023-01-01T00:00:00Z | seattle   | 733150     |
            | 2      | 0  | 2023-03-01T00:00:00Z | 2023-01-01T00:00:00Z | kyiv      | 2884150    |
            +--------+----+----------------------+----------------------+-----------+------------+
            "#,
        ))),
        ..Default::default()
    })
    .await
}

///////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_propagates_input_watermark() {
    let input_watermark_1: DateTime<Utc> = DateTime::parse_from_rfc3339("2023-01-01T00:00:00Z")
        .unwrap()
        .into();

    let input_watermark_2: DateTime<Utc> = DateTime::parse_from_rfc3339("2023-02-01T00:00:00Z")
        .unwrap()
        .into();

    test_query_common(TestQueryCommonOpts {
        mutate_request: Some(Box::new(move |mut r| {
            r.query_inputs[0].explicit_watermarks = vec![
                Watermark {
                    system_time: input_watermark_1.clone(), // Doesn't matter
                    event_time: input_watermark_1.clone(),
                },
                Watermark {
                    system_time: input_watermark_2.clone(), // Doesn't matter
                    event_time: input_watermark_2.clone(),
                },
            ];
            r
        })),
        expected_watermark: Some(Some(input_watermark_2)),
        ..Default::default()
    })
    .await
}

///////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_empty_result() {
    test_query_common(TestQueryCommonOpts {
        queries: Some(vec![SqlQueryStep {
            alias: None,
            query: "select event_time, city, population from foo where city = 'mumbai'".to_string(),
        }]),
        check_result: Some(Box::new(|res| {
            assert_eq!(
                res.unwrap(),
                TransformResponseSuccess {
                    new_offset_interval: None,
                    new_watermark: None,
                }
            )
        })),
        expected_data: Some(None),
        ..Default::default()
    })
    .await
}

///////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_empty_input() {
    test_query_common(TestQueryCommonOpts {
        mutate_request: Some(Box::new(|mut r| {
            r.query_inputs[0].offset_interval = None;
            r.query_inputs[0].data_paths = Vec::new();
            r
        })),
        check_result: Some(Box::new(|res| {
            assert_eq!(
                res.unwrap(),
                TransformResponseSuccess {
                    new_offset_interval: None,
                    new_watermark: None,
                }
            )
        })),
        expected_data: Some(None),
        ..Default::default()
    })
    .await
}

///////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_partial_input() {
    test_query_common(TestQueryCommonOpts {
        mutate_request: Some(Box::new(|mut r| {
            r.query_inputs[0].offset_interval = Some(OffsetInterval { start: 1, end: 1 });
            r
        })),
        check_result: Some(Box::new(|res| {
            assert_eq!(
                res.unwrap(),
                TransformResponseSuccess {
                    new_offset_interval: Some(OffsetInterval { start: 0, end: 0 }),
                    new_watermark: None,
                }
            )
        })),
        expected_data: Some(Some(indoc!(
            r#"
            +--------+----+----------------------+----------------------+---------+------------+
            | offset | op | system_time          | event_time           | city    | population |
            +--------+----+----------------------+----------------------+---------+------------+
            | 0      | 0  | 2023-03-01T00:00:00Z | 2023-01-01T00:00:00Z | seattle | 733100     |
            +--------+----+----------------------+----------------------+---------+------------+
            "#,
        ))),
        ..Default::default()
    })
    .await
}

///////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_output_offset() {
    test_query_common(TestQueryCommonOpts {
        mutate_request: Some(Box::new(|mut r| {
            r.next_offset = 10;
            r
        })),
        check_result: Some(Box::new(|res| {
            assert_eq!(
                res.unwrap(),
                TransformResponseSuccess {
                    new_offset_interval: Some(OffsetInterval { start: 10, end: 12 }),
                    new_watermark: None,
                }
            )
        })),
        expected_data: Some(Some(indoc!(
            r#"
            +--------+----+----------------------+----------------------+-----------+------------+
            | offset | op | system_time          | event_time           | city      | population |
            +--------+----+----------------------+----------------------+-----------+------------+
            | 10     | 0  | 2023-03-01T00:00:00Z | 2023-01-01T00:00:00Z | vancouver | 675100     |
            | 11     | 0  | 2023-03-01T00:00:00Z | 2023-01-01T00:00:00Z | seattle   | 733100     |
            | 12     | 0  | 2023-03-01T00:00:00Z | 2023-01-01T00:00:00Z | kyiv      | 2884100    |
            +--------+----+----------------------+----------------------+-----------+------------+
            "#,
        ))),
        ..Default::default()
    })
    .await
}

///////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_bad_sql() {
    test_query_common(TestQueryCommonOpts {
        queries: Some(vec![SqlQueryStep {
            alias: None,
            query: "select event_time, city, populllation from foo".to_string(),
        }]),
        check_result: Some(Box::new(|res| {
            assert_matches!(res, Err(ExecuteTransformError::InvalidQuery(_)))
        })),
        expected_data: Some(None),
        ..Default::default()
    })
    .await
}

///////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_event_time_as_date() {
    test_query_common(TestQueryCommonOpts {
        queries: Some(vec![SqlQueryStep {
            alias: None,
            query: "select cast(event_time as date) as event_time, city, population from foo"
                .to_string(),
        }]),
        expected_data: Some(Some(indoc!(
            r#"
            +--------+----+----------------------+------------+-----------+------------+
            | offset | op | system_time          | event_time | city      | population |
            +--------+----+----------------------+------------+-----------+------------+
            | 0      | 0  | 2023-03-01T00:00:00Z | 2023-01-01 | vancouver | 675000     |
            | 1      | 0  | 2023-03-01T00:00:00Z | 2023-01-01 | seattle   | 733000     |
            | 2      | 0  | 2023-03-01T00:00:00Z | 2023-01-01 | kyiv      | 2884000    |
            +--------+----+----------------------+------------+-----------+------------+
            "#,
        ))),
        ..Default::default()
    })
    .await
}

///////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_event_time_as_invalid_type() {
    test_query_common(TestQueryCommonOpts {
        queries: Some(vec![SqlQueryStep {
            alias: None,
            query: "select 123 as event_time, city, population from foo".to_string(),
        }]),
        check_result: Some(Box::new(|res| {
            assert_matches!(res, Err(ExecuteTransformError::InvalidQuery(_)))
        })),
        expected_data: Some(None),
        ..Default::default()
    })
    .await
}

///////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_event_time_coerced_to_millis() {
    test_query_common(TestQueryCommonOpts {
        queries: Some(vec![SqlQueryStep {
            alias: None,
            query: indoc!(
                r#"
                select
                    cast(cast(event_time as STRING) as TIMESTAMP) as event_time,
                    city,
                    population
                from foo
                "#
            )
            .to_string(),
        }]),
        expected_data: Some(Some(indoc!(
            r#"
            +--------+----+----------------------+----------------------+-----------+------------+
            | offset | op | system_time          | event_time           | city      | population |
            +--------+----+----------------------+----------------------+-----------+------------+
            | 0      | 0  | 2023-03-01T00:00:00Z | 2023-01-01T00:00:00Z | vancouver | 675000     |
            | 1      | 0  | 2023-03-01T00:00:00Z | 2023-01-01T00:00:00Z | seattle   | 733000     |
            | 2      | 0  | 2023-03-01T00:00:00Z | 2023-01-01T00:00:00Z | kyiv      | 2884000    |
            +--------+----+----------------------+----------------------+-----------+------------+
            "#,
        ))),
        expected_schema: Some(indoc!(
            r#"
            message arrow_schema {
              OPTIONAL INT64 offset;
              REQUIRED INT32 op;
              REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
              REQUIRED INT64 event_time (TIMESTAMP(MILLIS,true));
              REQUIRED BYTE_ARRAY city (STRING);
              REQUIRED INT64 population (INTEGER(64,false));
            }
            "#,
        )),
        ..Default::default()
    })
    .await
}

///////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_propagates_retractions_corrections() {
    test_query_common(TestQueryCommonOpts {
        input_data: Some(vec![
            Record::new(
                0,
                OperationType::Append,
                "2023-02-01T00:00:00Z",
                "2023-01-01T00:00:00Z",
                "vancouver",
                675_000,
            ),
            Record::new(
                1,
                OperationType::CorrectFrom,
                "2023-02-01T00:00:00Z",
                "2023-01-01T00:00:00Z",
                "vancouver",
                675_000,
            ),
            Record::new(
                2,
                OperationType::CorrectTo,
                "2023-02-01T00:00:00Z",
                "2023-01-01T00:00:00Z",
                "vancouver",
                676_000,
            ),
            Record::new(
                3,
                OperationType::Retract,
                "2023-02-01T00:00:00Z",
                "2023-01-01T00:00:00Z",
                "vancouver",
                676_000,
            ),
        ]),
        // Notice the propagation of `op` column
        queries: Some(vec![SqlQueryStep {
            alias: None,
            query: "select op, event_time, city, population from foo".to_string(),
        }]),
        expected_data: Some(Some(indoc!(
            r#"
            +--------+----+----------------------+----------------------+-----------+------------+
            | offset | op | system_time          | event_time           | city      | population |
            +--------+----+----------------------+----------------------+-----------+------------+
            | 0      | 0  | 2023-03-01T00:00:00Z | 2023-01-01T00:00:00Z | vancouver | 675000     |
            | 1      | 2  | 2023-03-01T00:00:00Z | 2023-01-01T00:00:00Z | vancouver | 675000     |
            | 2      | 3  | 2023-03-01T00:00:00Z | 2023-01-01T00:00:00Z | vancouver | 676000     |
            | 3      | 1  | 2023-03-01T00:00:00Z | 2023-01-01T00:00:00Z | vancouver | 676000     |
            +--------+----+----------------------+----------------------+-----------+------------+
            "#,
        ))),
        check_result: Some(Box::new(|_| {})),
        ..Default::default()
    })
    .await
}
