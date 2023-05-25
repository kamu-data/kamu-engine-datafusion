use std::path::Path;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use kamu_engine_datafusion::engine::Engine;
use opendatafabric::*;

fn write_sample_data(path: impl AsRef<Path>) {
    use datafusion::arrow::array::{StringArray, TimestampMillisecondArray, UInt64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::arrow::record_batch::RecordBatch;

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            DatasetVocabulary::DEFAULT_OFFSET_COLUMN_NAME,
            DataType::UInt64,
            false,
        ),
        Field::new(
            DatasetVocabulary::DEFAULT_SYSTEM_TIME_COLUMN_NAME,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new(
            DatasetVocabulary::DEFAULT_EVENT_TIME_COLUMN_NAME,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("city", DataType::Utf8, false),
        Field::new("population", DataType::UInt64, false),
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
            Arc::new(UInt64Array::from(vec![0, 1, 2])),
            Arc::new(TimestampMillisecondArray::from(vec![
                system_time,
                system_time,
                system_time,
            ])),
            Arc::new(TimestampMillisecondArray::from(vec![
                event_time, event_time, event_time,
            ])),
            Arc::new(StringArray::from(vec!["vancouver", "seattle", "kyiv"])),
            Arc::new(UInt64Array::from(vec![675_000, 733_000, 2_884_000])),
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

#[derive(Default)]
struct TestQueryCommonOpts {
    queries: Option<Vec<SqlQueryStep>>,
    mutate_request: Option<Box<dyn FnOnce(ExecuteQueryRequest) -> ExecuteQueryRequest>>,
    expected_result: Option<ExecuteQueryResponse>,
    expected_data: Option<Option<&'static str>>,
    expected_watermark: Option<Option<DateTime<Utc>>>,
}

async fn test_query_common(opts: TestQueryCommonOpts) {
    // Test defaults
    let queries = opts.queries.unwrap_or(vec![SqlQueryStep {
        alias: None,
        query: "select event_time, city, population + 100 as population from foo".to_string(),
    }]);
    let expected_data = opts.expected_data.unwrap_or(Some(
        r#"
+--------+---------------------+---------------------+-----------+------------+
| offset | system_time         | event_time          | city      | population |
+--------+---------------------+---------------------+-----------+------------+
| 10     | 2023-03-01T00:00:00 | 2023-01-01T00:00:00 | vancouver | 675100     |
| 11     | 2023-03-01T00:00:00 | 2023-01-01T00:00:00 | seattle   | 733100     |
| 12     | 2023-03-01T00:00:00 | 2023-01-01T00:00:00 | kyiv      | 2884100    |
+--------+---------------------+---------------------+-----------+------------+
        "#,
    ));
    let expected_result = opts
        .expected_result
        .unwrap_or(ExecuteQueryResponse::Success(ExecuteQueryResponseSuccess {
            data_interval: Some(OffsetInterval { start: 10, end: 12 }),
            output_watermark: opts.expected_watermark.unwrap_or(None),
        }));

    // Run test
    let tempdir = tempfile::tempdir().unwrap();

    let output_data_path = tempdir.path().join("output");
    let input_data_path = tempdir.path().join("input");
    write_sample_data(&input_data_path);

    let engine = Engine::new().await;

    let request = ExecuteQueryRequest {
        dataset_id: DatasetID::from_new_keypair_ed25519().1,
        dataset_name: DatasetName::new_unchecked("bar"),
        system_time: DateTime::parse_from_rfc3339("2023-03-01T00:00:00Z")
            .unwrap()
            .into(),
        offset: 10,
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

    let actual_result = engine.execute_query(request).await.unwrap();

    assert_eq!(actual_result, expected_result);

    if let Some(expected_data) = expected_data {
        let actual_data = read_data_pretty(&output_data_path).await;
        assert_eq!(actual_data, expected_data.trim());
    } else {
        assert!(!output_data_path.exists());
    }
}

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
+--------+---------------------+---------------------+-----------+------------+
| offset | system_time         | event_time          | city      | population |
+--------+---------------------+---------------------+-----------+------------+
| 10     | 2023-03-01T00:00:00 | 2023-01-01T00:00:00 | vancouver | 675150     |
| 11     | 2023-03-01T00:00:00 | 2023-01-01T00:00:00 | seattle   | 733150     |
| 12     | 2023-03-01T00:00:00 | 2023-01-01T00:00:00 | kyiv      | 2884150    |
+--------+---------------------+---------------------+-----------+------------+
            "#,
        )),
        ..Default::default()
    })
    .await
}

#[test_log::test(tokio::test)]
async fn test_propagates_input_watermark() {
    let input_watermark: DateTime<Utc> = DateTime::parse_from_rfc3339("2023-01-01T00:00:00Z")
        .unwrap()
        .into();

    test_query_common(TestQueryCommonOpts {
        mutate_request: Some(Box::new(move |mut r| {
            r.inputs[0].explicit_watermarks = vec![Watermark {
                system_time: input_watermark.clone(), // Doesn't matter
                event_time: input_watermark.clone(),
            }];
            r
        })),
        expected_watermark: Some(Some(input_watermark)),
        ..Default::default()
    })
    .await
}

#[test_log::test(tokio::test)]
async fn test_empty_result() {
    test_query_common(TestQueryCommonOpts {
        queries: Some(vec![SqlQueryStep {
            alias: None,
            query: "select event_time, city, population from foo where city = 'mumbai'".to_string(),
        }]),
        expected_result: Some(ExecuteQueryResponse::Success(ExecuteQueryResponseSuccess {
            data_interval: None,
            output_watermark: None,
        })),
        expected_data: Some(None),
        ..Default::default()
    })
    .await
}

#[test_log::test(tokio::test)]
async fn test_bad_sql() {
    test_query_common(TestQueryCommonOpts {
        queries: Some(vec![SqlQueryStep {
            alias: None,
            query: "select event_time, city, populllation from foo".to_string(),
        }]),
        expected_result: Some(ExecuteQueryResponse::InvalidQuery(
            ExecuteQueryResponseInvalidQuery {
                message: "Schema error: No field named populllation. Valid fields are foo.offset, \
                          foo.system_time, foo.event_time, foo.city, foo.population."
                    .to_string(),
            },
        )),
        expected_data: Some(None),
        ..Default::default()
    })
    .await
}

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
+--------+---------------------+------------+-----------+------------+
| offset | system_time         | event_time | city      | population |
+--------+---------------------+------------+-----------+------------+
| 10     | 2023-03-01T00:00:00 | 2023-01-01 | vancouver | 675000     |
| 11     | 2023-03-01T00:00:00 | 2023-01-01 | seattle   | 733000     |
| 12     | 2023-03-01T00:00:00 | 2023-01-01 | kyiv      | 2884000    |
+--------+---------------------+------------+-----------+------------+
            "#,
        )),
        ..Default::default()
    })
    .await
}

#[test_log::test(tokio::test)]
async fn test_event_time_as_invalid_type() {
    test_query_common(TestQueryCommonOpts {
        queries: Some(vec![SqlQueryStep {
            alias: None,
            query: "select 123 as event_time, city, population from foo".to_string(),
        }]),
        expected_result: Some(ExecuteQueryResponse::InvalidQuery(
            ExecuteQueryResponseInvalidQuery {
                message: "Event time column should be either Date or Timestamp, found: Int64"
                    .to_string(),
            },
        )),
        expected_data: Some(None),
        ..Default::default()
    })
    .await
}
