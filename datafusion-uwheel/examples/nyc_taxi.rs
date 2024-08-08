use std::sync::Arc;

use datafusion::{
    arrow::{self, array::RecordBatch},
    datasource::{
        file_format::parquet::ParquetFormat,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    },
    error::Result,
    physical_plan::collect,
    prelude::{col, lit, SessionContext},
    scalar::ScalarValue,
};
use datafusion_uwheel::{builder::Builder, AggregateType, IndexBuilder, UWheelOptimizer};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    let table_path = "../data/";

    // Parse the path
    let table_path = ListingTableUrl::parse(table_path)?;

    // Create default parquet options
    let file_format = ParquetFormat::new();
    let listing_options =
        ListingOptions::new(Arc::new(file_format)).with_file_extension(".parquet");

    // Resolve the schema
    let resolved_schema = listing_options
        .infer_schema(&ctx.state(), &table_path)
        .await?;

    let config = ListingTableConfig::new(table_path)
        .with_listing_options(listing_options)
        .with_schema(resolved_schema);

    // Create a new TableProvider
    let provider = Arc::new(ListingTable::try_new(config)?);

    // Build a provider over parquet files in data/ using the time column "tpep_dropoff_datetime"
    let optimizer: Arc<UWheelOptimizer> = Arc::new(
        Builder::new("tpep_dropoff_datetime")
            .with_name("yellow_tripdata")
            .with_min_max_wheels(vec!["fare_amount"])
            .build_with_provider(provider)
            .await
            .unwrap(),
    );

    // Build a wheel SUM on fare_amount
    let builder = IndexBuilder::with_col_and_aggregate("fare_amount", AggregateType::Sum);
    optimizer.build_index(builder).await?;

    // Build a wheel for a custom expression
    let builder = IndexBuilder::with_col_and_aggregate("fare_amount", AggregateType::Sum)
        .with_filter(col("passenger_count").eq(lit(ScalarValue::Float64(Some(4.0)))));

    optimizer.build_index(builder).await?;

    // Set UWheelOptimizer as the query planner
    let session_state = ctx.state().with_optimizer_rules(vec![optimizer.clone()]);
    let ctx = SessionContext::new_with_state(session_state);

    // Register the table using the underlying provider
    ctx.register_table("yellow_tripdata", optimizer.provider())
        .unwrap();

    // This query will then use the UWheelOptimizer to execute
    let df = ctx
        .sql(
            "SELECT COUNT(*) FROM yellow_tripdata
             WHERE tpep_dropoff_datetime >= '2022-01-01T00:00:00Z'
             AND tpep_dropoff_datetime < '2022-02-01T00:00:00Z'",
        )
        .await?;

    let plan = df.create_physical_plan().await?;
    // Execute the plan
    let results: Vec<RecordBatch> = collect(plan, ctx.task_ctx()).await?;
    arrow::util::pretty::print_batches(&results).unwrap();

    let sum_plan = ctx
        .sql(
            "SELECT SUM(fare_amount) FROM yellow_tripdata
             WHERE tpep_dropoff_datetime >= '2022-01-01T00:00:00Z'
             AND tpep_dropoff_datetime < '2022-02-01T00:00:00Z'",
        )
        .await?
        .create_physical_plan()
        .await?;

    let results: Vec<RecordBatch> = collect(sum_plan, ctx.task_ctx()).await?;
    arrow::util::pretty::print_batches(&results).unwrap();

    let filter_plan = ctx
        .sql(
            "SELECT * FROM yellow_tripdata
             WHERE tpep_dropoff_datetime >= '2022-01-01T00:00:00Z'
             AND tpep_dropoff_datetime < '2022-01-01T00:00:02Z'",
        )
        .await?
        .create_physical_plan()
        .await?;

    let results: Vec<RecordBatch> = collect(filter_plan, ctx.task_ctx()).await?;
    arrow::util::pretty::print_batches(&results).unwrap();

    let min_max = ctx
        .sql(
            "SELECT * FROM yellow_tripdata
             WHERE tpep_dropoff_datetime >= '2022-01-01T00:00:00Z'
             AND tpep_dropoff_datetime < '2022-01-02T00:00:00Z'
             AND fare_amount > 1000",
        )
        .await?
        .create_physical_plan()
        .await?;
    let results: Vec<RecordBatch> = collect(min_max, ctx.task_ctx()).await?;
    assert!(results.is_empty());
    arrow::util::pretty::print_batches(&results).unwrap();

    // We created an index for this SQL query earlier so it execute as a UWheelSumExec
    let sum_keyed_plan = ctx
        .sql(
            "SELECT SUM(fare_amount) FROM yellow_tripdata
                 WHERE tpep_dropoff_datetime >= '2022-01-01T00:00:00Z'
                 AND tpep_dropoff_datetime < '2022-02-01T00:00:00Z'
                 AND passenger_count = 4.0",
        )
        .await?
        .create_physical_plan()
        .await?;

    let results: Vec<RecordBatch> = collect(sum_keyed_plan, ctx.task_ctx()).await?;
    arrow::util::pretty::print_batches(&results).unwrap();

    println!(
        "Index size usage: {}",
        human_bytes::human_bytes(optimizer.index_usage_bytes() as u32)
    );

    Ok(())
}
