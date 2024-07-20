use std::sync::Arc;

use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use datafusion::{
    arrow::{self, array::RecordBatch},
    datasource::{
        file_format::parquet::ParquetFormat,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    },
    error::Result,
    physical_plan::{aggregates::AggregateExec, collect},
    prelude::SessionContext,
};
use uwheel::{
    aggregator::min_max::{F64MinMaxAggregator, MinMaxState},
    wheels::read::ReaderWheel,
    WheelRange,
};
use wheel_manager::{exec::UWheelCountExec, UWheelOptimizer};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    let table_path = "data/";

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
        wheel_manager::builder::Builder::new("tpep_dropoff_datetime")
            .with_name("yellow_tripdata")
            .with_min_max_wheels(vec!["fare_amount", "trip_distance"]) // Create Min/Max wheels for the columns "fare_amount" and "trip_distance"
            .build_with_provider(provider)
            .await
            .unwrap(),
    );

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

    // This plan is currently an AggregateExec but should be optimized!
    assert!(sum_plan.as_any().downcast_ref::<AggregateExec>().is_some());

    // The following wheel can be used to quickly filter queries such as:
    // SELECT * FROM yellow_tripdata
    // WHERE tpep_dropoff_datetime >= '?' and < '?'
    //
    // Or can be used to quickly count the number of records between two dates
    // SELECT COUNT(*) FROM yellow_tripdata
    // WHERE tpep_dropoff_datetime >= '?' and < '?'
    let count_wheel = optimizer.count_wheel();

    println!("Landmark COUNT(*) {:?}", count_wheel.landmark());

    // The following wheel can be used to quickly filter queries such as:
    // SELECT * FROM yellow_tripdata
    // WHERE tpep_dropoff_datetime >= '?' and < '?'
    // AND fare_amount > 1000
    let fare_wheel = optimizer.min_max_wheel("fare_amount").unwrap();

    // let's use the dates 2022-01-01 and 2022-01-10 to illustrate
    let start = NaiveDate::from_ymd_opt(2022, 1, 1).unwrap();
    let start_date = Utc.from_utc_datetime(&start.and_hms_opt(0, 0, 0).unwrap());

    let end = NaiveDate::from_ymd_opt(2022, 1, 10).unwrap();
    let end_date = Utc.from_utc_datetime(&end.and_hms_opt(0, 0, 0).unwrap());

    // Whether there are fare amounts above 1000.0
    // DataFusion Expr: let expr = col("fare_amount").gt(lit(1000.0));
    let fare_pred = |min_max_state: MinMaxState<f64>| min_max_state.max_value() > 1000.0;

    // Check whether the filter between 2022-01-01  and 2022-01-10 can be skipped
    if temporal_filter(&fare_wheel, start_date, end_date, fare_pred) {
        println!("Cannot skip execution since there are rides with fare_amount > 1000.0 between {:?} and {:?}", start_date, end_date);
    } else {
        println!("Skipping execution since there no rides with fare_amount > 1000.0 between {:?} and {:?}", start_date, end_date);
    }

    // Check whether the filter between 2022-01-01 12:30 and 12:50 can be skipped
    let start = NaiveDate::from_ymd_opt(2022, 1, 1).unwrap();
    let start_date = Utc.from_utc_datetime(&start.and_hms_opt(12, 30, 0).unwrap());

    let end = NaiveDate::from_ymd_opt(2022, 1, 1).unwrap();
    let end_date = Utc.from_utc_datetime(&end.and_hms_opt(12, 50, 0).unwrap());

    if temporal_filter(&fare_wheel, start_date, end_date, fare_pred) {
        println!("Cannot skip execution since there are rides with fare_amount > 1000.0 between {:?} and {:?}", start_date, end_date);
    } else {
        println!("Skipping execution since there no rides with fare_amount > 1000.0 between {:?} and {:?}", start_date, end_date);
    }
    Ok(())
}

// Executes a temporal wheel filter to determine whether a query can be skipped
fn temporal_filter(
    wheel: &ReaderWheel<F64MinMaxAggregator>,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    predicate: impl Fn(MinMaxState<f64>) -> bool,
) -> bool {
    let start_ms = start.timestamp_millis() as u64;
    let end_ms = end.timestamp_millis() as u64;

    // Get the Min/Max state between the start and end date
    let min_max_state = wheel
        .combine_range_and_lower(WheelRange::new_unchecked(start_ms, end_ms))
        .unwrap();

    predicate(min_max_state)
}
