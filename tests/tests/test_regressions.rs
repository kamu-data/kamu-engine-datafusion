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
