use std::assert_matches::assert_matches;
use std::path::Path;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use kamu_engine_datafusion::engine::Engine;
use opendatafabric::engine::ExecuteQueryError;
use opendatafabric::*;

///////////////////////////////////////////////////////////////////////////////

fn write_sample_data(path: impl AsRef<Path>) {
    use datafusion::arrow::array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::arrow::record_batch::RecordBatch;

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            DatasetVocabulary::DEFAULT_OFFSET_COLUMN_NAME,
            DataType::Int64,
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
        Field::new("population", DataType::Int64, false),
    ]));

    let system_time = DateTime::parse_from_rfc3339("2023-02-01T00:00:00Z")
        .unwrap()
        .timestamp_millis();
    let event_time = DateTime::parse_from_rfc3339("2023-01-01T00:00:00Z")
        .unwrap()
        .timestamp_millis();

    let record_batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(array::Int64Array::from(vec![0, 1, 2])),
            Arc::new(
                array::TimestampMillisecondArray::from(vec![system_time, system_time, system_time])
                    .with_timezone("UTC"),
            ),
            Arc::new(
                array::TimestampMillisecondArray::from(vec![event_time, event_time, event_time])
                    .with_timezone("UTC"),
            ),
            Arc::new(array::StringArray::from(vec![
                "vancouver",
                "seattle",
                "kyiv",
            ])),
            Arc::new(array::Int64Array::from(vec![675_000, 733_000, 2_884_000])),
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
    write_input_data: Option<Box<dyn FnOnce(&Path)>>,
    queries: Option<Vec<SqlQueryStep>>,
    mutate_request: Option<Box<dyn FnOnce(ExecuteQueryRequest) -> ExecuteQueryRequest>>,
    check_result: Option<Box<dyn FnOnce(Result<ExecuteQueryResponseSuccess, ExecuteQueryError>)>>,
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
    let expected_data = opts.expected_data.unwrap_or(Some(
        r#"
+--------+----------------------+----------------------+-----------+------------+
| offset | system_time          | event_time           | city      | population |
+--------+----------------------+----------------------+-----------+------------+
| 0      | 2023-03-01T00:00:00Z | 2023-01-01T00:00:00Z | vancouver | 675100     |
| 1      | 2023-03-01T00:00:00Z | 2023-01-01T00:00:00Z | seattle   | 733100     |
| 2      | 2023-03-01T00:00:00Z | 2023-01-01T00:00:00Z | kyiv      | 2884100    |
+--------+----------------------+----------------------+-----------+------------+
        "#,
    ));

    // Run test
    let tempdir = tempfile::tempdir().unwrap();

    let output_data_path = tempdir.path().join("output");
    let input_data_path = tempdir.path().join("input");

    if let Some(write_input_data) = opts.write_input_data {
        write_input_data(&input_data_path);
    } else {
        write_sample_data(&input_data_path);
    }

    let engine = Engine::new().await;

    let request = ExecuteQueryRequest {
        dataset_id: DatasetID::from_new_keypair_ed25519().1,
        dataset_name: DatasetName::new_unchecked("bar"),
        system_time: DateTime::parse_from_rfc3339("2023-03-01T00:00:00Z")
            .unwrap()
            .into(),
        offset: 0,
        vocab: DatasetVocabulary::default(),
        transform: Transform::Sql(TransformSql {
            engine: "datafusion".into(),
            version: None,
            query: None,
            queries: Some(queries),
            temporal_tables: None,
        }),
        inputs: vec![ExecuteQueryInput {
            dataset_id: DatasetID::from_new_keypair_ed25519().1,
            dataset_name: DatasetName::new_unchecked("foo"),
            data_interval: Some(OffsetInterval { start: 0, end: 2 }),
            vocab: DatasetVocabulary::default(),
            data_paths: vec![input_data_path.clone()],
            schema_file: input_data_path.clone(),
            explicit_watermarks: vec![],
        }],
        prev_checkpoint_path: None,
        new_checkpoint_path: tempdir.path().join("checkpoint"),
        out_data_path: output_data_path.clone(),
    };
    let request = if let Some(f) = opts.mutate_request {
        f(request)
    } else {
        request
    };

    let actual_result = engine.execute_query(request).await;

    if let Some(check_result) = opts.check_result {
        check_result(actual_result)
    } else {
        assert_eq!(
            actual_result.unwrap(),
            ExecuteQueryResponseSuccess {
                data_interval: Some(OffsetInterval { start: 0, end: 2 }),
                output_watermark: opts.expected_watermark.unwrap_or(None),
            }
        );
    }

    if let Some(expected_data) = expected_data {
        let actual_data = read_data_pretty(&output_data_path).await;
        assert_eq!(actual_data, expected_data.trim());
    } else {
        assert!(!output_data_path.exists());
    }

    if let Some(expected_schema) = opts.expected_schema {
        let actual_schema = read_parquet_schema_pretty(&output_data_path).await;
        assert_eq!(actual_schema.trim(), expected_schema.trim());
    }
}

///////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_result_schema() {
    test_query_common(TestQueryCommonOpts {
        expected_schema: Some(
            r#"
message arrow_schema {
  REQUIRED INT64 offset;
  REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
  REQUIRED INT64 event_time (TIMESTAMP(MILLIS,true));
  REQUIRED BYTE_ARRAY city (STRING);
  REQUIRED INT64 population;
}
            "#,
        ),
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
        expected_data: Some(Some(
            r#"
+--------+----------------------+----------------------+-----------+------------+
| offset | system_time          | event_time           | city      | population |
+--------+----------------------+----------------------+-----------+------------+
| 0      | 2023-03-01T00:00:00Z | 2023-01-01T00:00:00Z | vancouver | 675150     |
| 1      | 2023-03-01T00:00:00Z | 2023-01-01T00:00:00Z | seattle   | 733150     |
| 2      | 2023-03-01T00:00:00Z | 2023-01-01T00:00:00Z | kyiv      | 2884150    |
+--------+----------------------+----------------------+-----------+------------+
            "#,
        )),
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
            r.inputs[0].explicit_watermarks = vec![
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
                ExecuteQueryResponseSuccess {
                    data_interval: None,
                    output_watermark: None,
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
            r.inputs[0].data_interval = None;
            r.inputs[0].data_paths = Vec::new();
            r
        })),
        check_result: Some(Box::new(|res| {
            assert_eq!(
                res.unwrap(),
                ExecuteQueryResponseSuccess {
                    data_interval: None,
                    output_watermark: None,
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
            r.inputs[0].data_interval = Some(OffsetInterval { start: 1, end: 1 });
            r
        })),
        check_result: Some(Box::new(|res| {
            assert_eq!(
                res.unwrap(),
                ExecuteQueryResponseSuccess {
                    data_interval: Some(OffsetInterval { start: 0, end: 0 }),
                    output_watermark: None,
                }
            )
        })),
        expected_data: Some(Some(
            r#"
+--------+----------------------+----------------------+---------+------------+
| offset | system_time          | event_time           | city    | population |
+--------+----------------------+----------------------+---------+------------+
| 0      | 2023-03-01T00:00:00Z | 2023-01-01T00:00:00Z | seattle | 733100     |
+--------+----------------------+----------------------+---------+------------+
            "#,
        )),
        ..Default::default()
    })
    .await
}

///////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_output_offset() {
    test_query_common(TestQueryCommonOpts {
        mutate_request: Some(Box::new(|mut r| {
            r.offset = 10;
            r
        })),
        check_result: Some(Box::new(|res| {
            assert_eq!(
                res.unwrap(),
                ExecuteQueryResponseSuccess {
                    data_interval: Some(OffsetInterval { start: 10, end: 12 }),
                    output_watermark: None,
                }
            )
        })),
        expected_data: Some(Some(
            r#"
+--------+----------------------+----------------------+-----------+------------+
| offset | system_time          | event_time           | city      | population |
+--------+----------------------+----------------------+-----------+------------+
| 10     | 2023-03-01T00:00:00Z | 2023-01-01T00:00:00Z | vancouver | 675100     |
| 11     | 2023-03-01T00:00:00Z | 2023-01-01T00:00:00Z | seattle   | 733100     |
| 12     | 2023-03-01T00:00:00Z | 2023-01-01T00:00:00Z | kyiv      | 2884100    |
+--------+----------------------+----------------------+-----------+------------+
            "#,
        )),
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
            assert_matches!(res, Err(ExecuteQueryError::InvalidQuery(_)))
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
        expected_data: Some(Some(
            r#"
+--------+----------------------+------------+-----------+------------+
| offset | system_time          | event_time | city      | population |
+--------+----------------------+------------+-----------+------------+
| 0      | 2023-03-01T00:00:00Z | 2023-01-01 | vancouver | 675000     |
| 1      | 2023-03-01T00:00:00Z | 2023-01-01 | seattle   | 733000     |
| 2      | 2023-03-01T00:00:00Z | 2023-01-01 | kyiv      | 2884000    |
+--------+----------------------+------------+-----------+------------+
            "#,
        )),
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
            assert_matches!(res, Err(ExecuteQueryError::InvalidQuery(_)))
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
            query: r#"
select
    cast(cast(event_time as STRING) as TIMESTAMP) as event_time,
    city,
    population
from foo
            "#
            .to_string(),
        }]),
        expected_data: Some(Some(
            r#"
+--------+----------------------+----------------------+-----------+------------+
| offset | system_time          | event_time           | city      | population |
+--------+----------------------+----------------------+-----------+------------+
| 0      | 2023-03-01T00:00:00Z | 2023-01-01T00:00:00Z | vancouver | 675000     |
| 1      | 2023-03-01T00:00:00Z | 2023-01-01T00:00:00Z | seattle   | 733000     |
| 2      | 2023-03-01T00:00:00Z | 2023-01-01T00:00:00Z | kyiv      | 2884000    |
+--------+----------------------+----------------------+-----------+------------+
            "#,
        )),
        expected_schema: Some(
            r#"
message arrow_schema {
  REQUIRED INT64 offset;
  REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
  REQUIRED INT64 event_time (TIMESTAMP(MILLIS,true));
  REQUIRED BYTE_ARRAY city (STRING);
  REQUIRED INT64 population;
}
            "#,
        ),
        ..Default::default()
    })
    .await
}
