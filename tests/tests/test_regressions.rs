use std::str::FromStr as _;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::*;

///////////////////////////////////////////////////////////////////////////////

// Regression test for: https://github.com/apache/arrow-datafusion/issues/6463
#[test_log::test(tokio::test)]
async fn test_issues_datafusion_6463() {
    let ctx = SessionContext::new();

    ctx.register_parquet(
        "ab",
        "data/datafusion-issue-6463/alberta.parquet",
        ParquetReadOptions::default(),
    )
    .await
    .unwrap();
    ctx.register_parquet(
        "bc",
        "data/datafusion-issue-6463/british-columbia.parquet",
        ParquetReadOptions::default(),
    )
    .await
    .unwrap();

    let df = ctx
        .sql(
            r#"
            SELECT * FROM (
                SELECT
                    'AB' as province,
                    id,
                    reported_date,
                    gender,
                    location
                FROM ab
                UNION ALL
                SELECT
                    'BC' as province,
                    id,
                    reported_date,
                    gender,
                    location
                FROM bc
            )
            "#,
        )
        .await
        .unwrap();

    let tempdir = tempfile::tempdir().unwrap();
    let output_path = format!("{}/foo", tempdir.path().display());
    df.write_parquet(
        &output_path,
        DataFrameWriteOptions::new().with_single_file_output(true),
        None,
    )
    .await
    .unwrap();

    let df = ctx
        .read_parquet(
            &output_path,
            ParquetReadOptions {
                file_extension: "",
                ..Default::default()
            },
        )
        .await
        .unwrap();

    assert_eq!(df.count().await.unwrap(), 20);
}

///////////////////////////////////////////////////////////////////////////////

// Regression test for: https://github.com/apache/arrow-rs/issues/4308
#[test_log::test(tokio::test)]
async fn test_issues_arrow_4308() {
    let ctx = SessionContext::new();

    let df = ctx
        .read_parquet(
            "data/arrow-issue-4308/data.parquet",
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();

    let system_time_dt = df
        .schema()
        .field_with_unqualified_name("system_time")
        .unwrap()
        .data_type();

    assert_eq!(
        *system_time_dt,
        DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from("UTC")))
    );
}

///////////////////////////////////////////////////////////////////////////////

// Regression test for: https://github.com/apache/datafusion/issues/14281
#[test_log::test(tokio::test)]
async fn test_issues_datafusion_14281() {
    use std::path::PathBuf;

    use chrono::DateTime;
    use odf::*;

    let workspace_dir = PathBuf::from_str(env!("CARGO_MANIFEST_DIR")).unwrap();
    let tempdir = tempfile::tempdir().unwrap();
    let new_data_path = tempdir.path().join("output");
    let new_checkpoint_path = tempdir.path().join("checkpoint");

    let request = TransformRequest {
        dataset_id: DatasetID::new_seeded_ed25519(b"bar"),
        dataset_alias: "deriv".try_into().unwrap(),
        system_time: DateTime::parse_from_rfc3339("2050-01-02T12:00:00Z")
            .unwrap()
            .into(),
        vocab: DatasetVocabulary {
            offset_column: "offset".to_string(),
            operation_type_column: "op".to_string(),
            system_time_column: "system_time".to_string(),
            event_time_column: "event_time".to_string(),
        },
        transform: TransformSql {
            engine: "datafusion".to_string(),
            version: None,
            query: None,
            queries: Some(vec![SqlQueryStep {
                alias: None,
                query: r#"
                SELECT
                    op,
                    event_time,
                    city,
                    cast(population * 10 as int) as population_x10
                FROM root
                "#
                .to_string(),
            }]),
            temporal_tables: None,
        }
        .into(),
        query_inputs: vec![TransformRequestInput {
            dataset_id: DatasetID::new_seeded_ed25519(b"foo"),
            dataset_alias: "root".try_into().unwrap(),
            query_alias: "root".to_string(),
            vocab: DatasetVocabulary {
                offset_column: "offset".to_string(),
                operation_type_column: "op".to_string(),
                system_time_column: "system_time".to_string(),
                event_time_column: "event_time".to_string(),
            },
            offset_interval: Some(OffsetInterval { start: 0, end: 2 }),
            data_paths: vec![workspace_dir.join("data/datafusion-issue-14281/data.parquet")],
            schema_file: workspace_dir.join("data/datafusion-issue-14281/schema.parquet"),
            explicit_watermarks: vec![Watermark {
                system_time: DateTime::parse_from_rfc3339("2050-01-01T12:00:00Z")
                    .unwrap()
                    .into(),
                event_time: DateTime::parse_from_rfc3339("2050-01-01T12:00:00Z")
                    .unwrap()
                    .into(),
            }],
        }],
        next_offset: 0,
        prev_checkpoint_path: None,
        new_checkpoint_path,
        new_data_path,
    };

    let engine = kamu_engine_datafusion::engine::Engine::new().await;

    engine.execute_transform(request).await.unwrap();
}
