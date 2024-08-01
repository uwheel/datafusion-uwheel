use std::sync::Arc;

use datafusion::{
    arrow::{self, array::RecordBatch},
    datasource::{
        file_format::parquet::ParquetFormat,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    },
    error::Result,
    physical_plan::{coalesce_batches::CoalesceBatchesExec, collect, empty::EmptyExec},
    prelude::{col, count, lit, SessionContext},
    scalar::ScalarValue,
};
use datafusion_uwheel::{
    builder::Builder,
    exec::{UWheelCountExec, UWheelSumExec},
    AggregateType, IndexBuilder, UWheelOptimizer,
};

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
            .with_min_max_wheels(vec!["fare_amount", "trip_distance"])
            .with_sum_wheels(vec!["fare_amount"])
            .build_with_provider(provider)
            .await
            .unwrap(),
    );

    // Build a wheel for a custom expression
    let builder = IndexBuilder::with_col_and_aggregate("fare_amount", AggregateType::Sum)
        .with_filter(col("passenger_count").eq(lit(ScalarValue::Float64(Some(4.0)))));

    optimizer.build_index(builder).await?;

    // Set UWheelOptimizer as the query planner
    let session_state = ctx.state().with_query_planner(optimizer.clone());
    let ctx = SessionContext::new_with_state(session_state);

    // Register the table using the underlying provider
    ctx.register_table("yellow_tripdata", optimizer.provider())
        .unwrap();

    // This query will then use the UWheelOptimizer to execute
    let plan = ctx
        .sql(
            "SELECT COUNT(*) FROM yellow_tripdata
             WHERE tpep_dropoff_datetime >= '2022-01-01T00:00:00Z'
             AND tpep_dropoff_datetime < '2022-02-01T00:00:00Z'",
        )
        .await?
        .create_physical_plan()
        .await?;

    // The plan should be a UWheelCountExec
    let uwheel_exec = plan.as_any().downcast_ref::<UWheelCountExec>().unwrap();
    dbg!(uwheel_exec);

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

    let physical_plan = ctx
        .sql(
            "SELECT * FROM yellow_tripdata
             WHERE tpep_dropoff_datetime >= '2022-01-01T00:00:00Z'
             AND tpep_dropoff_datetime < '2022-01-01T00:00:02Z'",
        )
        .await?
        .create_physical_plan()
        .await?;

    // Verify that the plan is optimized to EmptyExec
    assert!(physical_plan.as_any().downcast_ref::<EmptyExec>().is_some());

    let physical_plan = ctx
        .sql(
            "SELECT * FROM yellow_tripdata
             WHERE tpep_dropoff_datetime >= '2022-01-01T00:00:00Z'
             AND tpep_dropoff_datetime < '2022-01-02T00:00:00Z'",
        )
        .await?
        .create_physical_plan()
        .await?;

    assert!(physical_plan
        .as_any()
        .downcast_ref::<CoalesceBatchesExec>()
        .is_some());

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
    // verify that it returned an EmptyExec
    assert!(min_max.as_any().downcast_ref::<EmptyExec>().is_some());

    let between_plan = ctx
        .sql(
            "SELECT COUNT(*) FROM yellow_tripdata
             WHERE tpep_dropoff_datetime BETWEEN '2022-01-01T00:00:00Z'
             AND '2022-02-01T00:00:00Z'",
        )
        .await?
        .create_physical_plan()
        .await?;

    // The plan should be a UWheelCountExec
    let uwheel_exec = between_plan
        .as_any()
        .downcast_ref::<UWheelCountExec>()
        .unwrap();
    dbg!(uwheel_exec);

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

    assert!(sum_keyed_plan
        .as_any()
        .downcast_ref::<UWheelSumExec>()
        .is_some());

    let results: Vec<RecordBatch> = collect(sum_keyed_plan, ctx.task_ctx()).await?;
    arrow::util::pretty::print_batches(&results).unwrap();

    Ok(())
}
