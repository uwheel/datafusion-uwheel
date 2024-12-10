use std::sync::Arc;

use chrono::DateTime;
use chrono::Duration;
use chrono::TimeZone;
use chrono::Utc;
use datafusion::arrow::array::{Float64Array, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{Field, Schema, TimeUnit};
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::{
    arrow::{
        array::{Int64Array, RecordBatch},
        datatypes::DataType,
    },
    datasource::provider_as_source,
    error::Result,
    logical_expr::{test::function_stub::count, LogicalPlan, LogicalPlanBuilder},
    prelude::{col, lit, wildcard, Expr, SessionContext},
};
use datafusion_uwheel::{builder::Builder, UWheelOptimizer};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    // Load data into a MemTable
    let mem_table = create_memtable()?;

    // Create UWheelOptimizer
    let optimizer: Arc<UWheelOptimizer> = Arc::new(
        Builder::new("timestamp")
            .with_name("my_table")
            .build_with_provider(Arc::new(mem_table))
            .await?,
    );

    // Register Table
    let ctx = SessionContext::new();
    ctx.register_table("my_table", optimizer.provider().clone())?;

    // Set UWheelOptimizer as optimizer rule
    let session_state = SessionStateBuilder::new()
        .with_optimizer_rules(vec![optimizer.clone()])
        .build();

    let ctx = SessionContext::new_with_state(session_state);

    // Create a Temporal COUNT(*) Aggregation Query
    let temporal_filter = col("timestamp")
        .gt_eq(lit("2024-05-10T00:00:00Z"))
        .and(col("timestamp").lt(lit("2024-05-10T00:00:10Z")));

    let plan =
        LogicalPlanBuilder::scan("my_table", provider_as_source(optimizer.provider()), None)?
            .filter(temporal_filter)?
            .aggregate(Vec::<Expr>::new(), vec![count(wildcard())])?
            .project(vec![count(wildcard())])?
            .build()?;

    // Verify that the optimizer rewrites the query into a plan-time available TableScan
    assert!(matches!(plan, LogicalPlan::Projection(_)));
    let rewritten = optimizer.try_rewrite(&plan).unwrap();
    assert!(matches!(rewritten, LogicalPlan::TableScan(_)));

    // Run the query through the ctx that has our OptimizerRule
    let df = ctx.execute_logical_plan(plan).await?;
    let results = df.collect().await?;

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        10,
    );

    datafusion::arrow::util::pretty::print_batches(&results).unwrap();

    Ok(())
}

// Creates a MemTable with a "timestamp" column and an "aggregate" column that consists of 10
// entries ( 1 per second).
fn create_memtable() -> Result<MemTable> {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("agg_col", DataType::Float64, false),
    ]));

    // Define the start time as 2024-05-10 00:00:00 UTC
    let base_time: DateTime<Utc> = Utc.with_ymd_and_hms(2024, 5, 10, 0, 0, 0).unwrap();
    let timestamps: Vec<i64> = (0..10)
        .map(|i| base_time + Duration::seconds(i))
        .map(|dt| dt.timestamp_micros())
        .collect();

    let agg_values: Vec<f64> = (0..10).map(|i| (i + 1) as f64).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(TimestampMicrosecondArray::from(timestamps)),
            Arc::new(Float64Array::from(agg_values)),
        ],
    )?;

    MemTable::try_new(schema, vec![vec![batch]])
}
