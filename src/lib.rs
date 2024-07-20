use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use datafusion::error::Result;
use datafusion::{
    arrow::{
        array::{Float64Array, RecordBatch, TimestampMicrosecondArray},
        datatypes::{DataType, Field, Schema, SchemaRef},
    },
    datasource::TableProvider,
    execution::{
        context::{QueryPlanner, SessionState},
        TaskContext,
    },
    logical_expr::{
        expr::AggregateFunctionDefinition, AggregateFunction, Between, BinaryExpr, LogicalPlan,
        Operator,
    },
    physical_plan::{common::collect, ExecutionPlan},
    physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner},
    prelude::*,
    scalar::ScalarValue,
};
use exec::UWheelCountExec;
use uwheel::{
    aggregator::min_max::F64MinMaxAggregator,
    aggregator::sum::U32SumAggregator,
    wheels::read::{aggregation::conf::WheelMode, ReaderWheel},
    Conf, Entry, HawConf, RwWheel, WheelRange,
};

/// Custom aggregator implementations that are used by this crate.
mod aggregator;
/// Builder for creating a UWheelOptimizer
pub mod builder;
/// Various Execution Plan implementations
pub mod exec;

pub const COUNT_STAR_ALIAS: &str = "COUNT(*)";

/// A µWheel Provider over a set of parquet files
///
/// An extension that improves time-based analytical queries.
#[allow(dead_code)]
pub struct UWheelOptimizer {
    /// Name of the table
    name: String,
    /// The column in the parquet files that contains the time
    time_column: String,
    /// A COUNT(*) wheel over the parquet files and time column
    count: ReaderWheel<U32SumAggregator>,
    /// Min/Max pruning wheels over the parquet files for a specific column
    min_max_wheels: Arc<Mutex<HashMap<String, ReaderWheel<F64MinMaxAggregator>>>>,
    // TODO: add support for other aggregation wheels
    // aggregation_wheels: Arc<Mutex<HashMap<Expr, ReaderWheel<?>>>>,
    /// Table provider which uwheel indexes are built on top of.
    inner_provider: Arc<dyn TableProvider>,
}

impl UWheelOptimizer {
    /// Create a new UWheelOptimizer
    pub async fn try_new(
        name: impl Into<String>,
        time_column: impl Into<String>,
        min_max_columns: Vec<String>,
        provider: Arc<dyn TableProvider>,
    ) -> Result<Self> {
        let time_column = time_column.into();

        // Create an "initial" provider over the parquet files
        let (count, min_max_wheels) =
            build(provider.clone(), &time_column, min_max_columns).await?;

        Ok(Self {
            name: name.into(),
            time_column,
            count,
            min_max_wheels: Arc::new(Mutex::new(min_max_wheels)),
            inner_provider: provider,
        })
    }

    /// Count the number of rows in the given time range
    ///
    /// Returns `None` if the count is not available
    #[inline]
    pub fn count(&self, range: WheelRange) -> Option<u32> {
        self.count.combine_range_and_lower(range)
    }

    /// Returns the inner TableProvider
    pub fn provider(&self) -> Arc<dyn TableProvider> {
        self.inner_provider.clone()
    }

    /// Returns a reference to the table's COUNT(*) wheel
    #[inline]
    pub fn count_wheel(&self) -> &ReaderWheel<U32SumAggregator> {
        &self.count
    }

    /// Returns a reference to a MIN/MAX wheel for a specified column
    pub fn min_max_wheel(&self, column: &str) -> Option<ReaderWheel<F64MinMaxAggregator>> {
        self.min_max_wheels.lock().unwrap().get(column).cloned()
    }

    /// Builds a wheel for the given DataFusion expression
    ///
    /// Example: `col("PULocationID").eq(lit(120))`
    pub fn build_wheel(&self, _expr: Expr) {
        todo!("Not implemented yet");
    }
}

impl UWheelOptimizer {
    fn count_exec(&self, range: WheelRange) -> Arc<dyn ExecutionPlan> {
        let name = COUNT_STAR_ALIAS.to_string();
        let schema = Arc::new(Schema::new(vec![Field::new(
            name.clone(),
            DataType::Int64,
            true,
        )]));
        Arc::new(UWheelCountExec::new(self.count.clone(), schema, range))
    }
    /// This function takes a logical plan and checks whether it can be optimized using ``uwheel``
    pub fn try_optimize(&self, plan: &LogicalPlan) -> Option<Arc<dyn ExecutionPlan>> {
        match plan {
            LogicalPlan::Aggregate(agg) => {
                if let LogicalPlan::Projection(projection) = agg.input.as_ref() {
                    if let LogicalPlan::Filter(filter) = projection.input.as_ref() {
                        // SELECT AGG FROM X WHERE TIME >= X AND TIME <= Y

                        // Check no GROUP BY and only one aggregation (for now)
                        if agg.group_expr.is_empty() && agg.aggr_expr.len() == 1 {
                            let agg_expr = agg.aggr_expr.first().unwrap();
                            match agg_expr {
                                Expr::Alias(alias) => {
                                    if alias.name == COUNT_STAR_ALIAS {
                                        //dbg!(&alias.expr);
                                        // Extract Wheel Range for temporal filter
                                        if let Some((Some(start), Some(end))) = extract_wheel_range(
                                            &filter.predicate,
                                            &self.time_column,
                                        ) {
                                            let range =
                                                WheelRange::new_unchecked(start as u64, end as u64);
                                            // TODO: check if the range is queryable otherwise return None
                                            //let result = self.count(range).unwrap_or(0) as u32;
                                            return Some(self.count_exec(range));
                                            //return Some(self.count_exec_plan(result).unwrap());
                                        }
                                    }
                                }
                                Expr::AggregateFunction(agg) => {
                                    if matches!(
                                        agg.func_def,
                                        AggregateFunctionDefinition::BuiltIn(
                                            AggregateFunction::Count
                                        )
                                    ) {
                                        dbg!(agg);
                                    }
                                }
                                _ => (),
                            }
                        }
                    }
                }

                None
            }
            _ => None,
        }
    }
}

/// Implement UWheelOptimizer as a custom QueryPlanner
#[async_trait]
impl QueryPlanner for UWheelOptimizer {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // TODO: Check whether this plan is for provider.name()
        match self.try_optimize(&logical_plan) {
            Some(exec) => Ok(exec),
            None => {
                // If the optimization fails, use the default physical planner
                let planner = DefaultPhysicalPlanner::default();
                planner.create_physical_plan(logical_plan, state).await
            }
        }
    }
}

// Helper methods to build the UWheelOptimizer

// Uses the provided TableProvider to build the UWheelOptimizer
async fn build(
    provider: Arc<dyn TableProvider>,
    time_column: &str,
    min_max_columns: Vec<String>,
) -> Result<(
    ReaderWheel<U32SumAggregator>,
    HashMap<String, ReaderWheel<F64MinMaxAggregator>>,
)> {
    let ctx = SessionContext::new();
    let scan = provider.scan(&ctx.state(), None, &[], None).await?;
    let task_ctx = Arc::new(TaskContext::default());
    let stream = scan.execute(0, task_ctx.clone())?;
    let batches = collect(stream).await?;
    let mut timestamps = Vec::new();

    for batch in batches.iter() {
        // Verify the time column exists in the parquet file
        let time_column_exists = batch.schema().index_of(time_column).is_ok();
        assert!(
            time_column_exists,
            "Specified Time column does not exist in the provided data"
        );

        let time_array = batch
            .column_by_name(time_column)
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();

        let batch_timestamps: Vec<_> = time_array.values().iter().copied().collect();
        timestamps.extend_from_slice(&batch_timestamps);
    }

    // Build a COUNT(*) wheel over the timestamps
    let (count_wheel, min_timestamp_ms, max_timestamp_ms) = build_count_wheel(timestamps);

    // Build MinMax wheels for specified columns
    let mut map = HashMap::new();
    for column in min_max_columns.iter() {
        let min_max_wheel = build_min_max_wheel(
            provider.schema(),
            &batches,
            min_timestamp_ms,
            max_timestamp_ms,
            time_column,
            column,
        )
        .await?;
        map.insert(column.to_string(), min_max_wheel);
    }

    Ok((count_wheel.read().clone(), map))
}

async fn build_min_max_wheel(
    schema: SchemaRef,
    batches: &[RecordBatch],
    _min_timestamp_ms: u64,
    max_timestamp_ms: u64,
    time_col: &str,
    min_max_col: &str,
) -> Result<ReaderWheel<F64MinMaxAggregator>> {
    // TODO: remove hardcoded time
    let start = NaiveDate::from_ymd_opt(2022, 1, 1).unwrap();
    let date = Utc.from_utc_datetime(&start.and_hms_opt(0, 0, 0).unwrap());
    let start_ms = date.timestamp_millis() as u64;

    // TODO: get Conf from builder
    let mut conf = HawConf::default()
        .with_watermark(start_ms)
        .with_mode(WheelMode::Index);

    conf.minutes
        .set_retention_policy(uwheel::RetentionPolicy::Keep);

    conf.hours
        .set_retention_policy(uwheel::RetentionPolicy::Keep);

    conf.days
        .set_retention_policy(uwheel::RetentionPolicy::Keep);

    conf.weeks
        .set_retention_policy(uwheel::RetentionPolicy::Keep);

    let mut wheel: RwWheel<F64MinMaxAggregator> = RwWheel::with_conf(
        Conf::default()
            .with_haw_conf(conf)
            .with_write_ahead(64000usize.next_power_of_two()),
    );

    let column_index = schema.index_of(min_max_col)?;
    let column_field = schema.field(column_index);

    if is_numeric_type(column_field.data_type()) {
        for batch in batches {
            let time_array = batch
                .column_by_name(time_col)
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();

            let min_max_column_array = batch
                .column_by_name(min_max_col)
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();

            for (timestamp, value) in time_array
                .values()
                .iter()
                .copied()
                .zip(min_max_column_array.values().iter().copied())
            {
                let timestamp_ms = DateTime::from_timestamp_micros(timestamp as i64)
                    .unwrap()
                    .timestamp_millis() as u64;
                let entry = Entry::new(value, timestamp_ms);
                wheel.insert(entry);
            }
        }
        // Once all data is inserted, advance the wheel to the max timestamp
        wheel.advance_to(max_timestamp_ms);
    } else {
        // TODO: return Datafusion Error?
        panic!("Min/Max column must be a numeric type");
    }

    Ok(wheel.read().clone())
}

/// Builds a COUNT(*) wheel over the given timestamps
///
/// Uses a U32SumAggregator internally with prefix-sum optimization
fn build_count_wheel(timestamps: Vec<i64>) -> (RwWheel<U32SumAggregator>, u64, u64) {
    let min = timestamps.iter().min().copied().unwrap();
    let max = timestamps.iter().max().copied().unwrap();

    let min_ms = DateTime::from_timestamp_micros(min)
        .unwrap()
        .timestamp_millis() as u64;

    let max_ms = DateTime::from_timestamp_micros(max)
        .unwrap()
        .timestamp_millis() as u64;

    let start = NaiveDate::from_ymd_opt(2022, 1, 1).unwrap();
    let date = Utc.from_utc_datetime(&start.and_hms_opt(0, 0, 0).unwrap());
    let start_ms = date.timestamp_millis() as u64;

    let mut conf = HawConf::default()
        .with_watermark(start_ms)
        .with_mode(WheelMode::Index);

    conf.minutes
        .set_retention_policy(uwheel::RetentionPolicy::Keep);

    conf.hours
        .set_retention_policy(uwheel::RetentionPolicy::Keep);

    conf.days
        .set_retention_policy(uwheel::RetentionPolicy::Keep);

    conf.weeks
        .set_retention_policy(uwheel::RetentionPolicy::Keep);

    let mut count_wheel: RwWheel<U32SumAggregator> = RwWheel::with_conf(
        Conf::default()
            .with_haw_conf(conf)
            .with_write_ahead(64000usize.next_power_of_two()),
    );

    for timestamp in timestamps {
        let timestamp_ms = DateTime::from_timestamp_micros(timestamp as i64)
            .unwrap()
            .timestamp_millis() as u64;
        // Record a count
        let entry = Entry::new(1, timestamp_ms);
        count_wheel.insert(entry);
    }
    dbg!(start_ms, max_ms);

    count_wheel.advance_to(max_ms);

    // convert wheel to index
    count_wheel.read().to_simd_wheels();
    count_wheel.read().to_prefix_wheels();

    (count_wheel, min_ms, max_ms)
}

// checks whether the given data type is a numeric type
fn is_numeric_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
    )
}

// UWheelOptimizer helper methods

fn extract_wheel_range(predicate: &Expr, time_column: &str) -> Option<(Option<i64>, Option<i64>)> {
    match predicate {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            //dbg!(left, op, right);
            if let Operator::And = op {
                let left = extract_wheel_range(left, time_column);
                let right = extract_wheel_range(right, time_column);
                dbg!(left, right);

                if let Some((start, _)) = left {
                    if let Some((_, end)) = right {
                        return Some((start, end));
                    }
                }
            } else {
                if let Expr::Column(col) = left.as_ref() {
                    if col.name == time_column {
                        match op {
                            Operator::GtEq | Operator::Gt => {
                                return extract_timestamp(right).map(|ts| (Some(ts), None::<i64>));
                            }
                            Operator::Lt | Operator::LtEq => {
                                return extract_timestamp(right).map(|ts| (None::<i64>, Some(ts)));
                            }

                            _ => return None,
                        };
                    }
                }
            }
        }
        Expr::Between(Between {
            expr,
            negated,
            low,
            high,
        }) => {
            if let Expr::Column(col) = expr.as_ref() {
                if col.name == time_column && !negated {
                    if let Some(start) = extract_timestamp(low) {
                        if let Some(end) = extract_timestamp(high) {
                            return Some((Some(start), Some(end)));
                        }
                    }
                }
            }
        }
        _ => (),
    }
    None
}

fn extract_timestamp(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(date_str))) => DateTime::parse_from_rfc3339(date_str)
            .ok()
            .map(|dt| dt.with_timezone(&Utc).timestamp_millis()),
        Expr::Literal(ScalarValue::TimestampMillisecond(Some(ms), _)) => Some(*ms as i64),
        Expr::Literal(ScalarValue::TimestampMicrosecond(Some(us), _)) => Some(*us / 1000 as i64),
        Expr::Literal(ScalarValue::TimestampNanosecond(Some(ns), _)) => {
            Some(ns / 1_000_000) // Convert nanoseconds to milliseconds
        }
        // Add other cases as needed, e.g., for different date/time formats
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_timestamp_test() {
        let expr = Expr::Literal(ScalarValue::Utf8(Some("2023-01-01T00:00:00Z".to_string())));
        assert_eq!(extract_timestamp(&expr), Some(1672531200000));
    }

    fn create_timestamp(date_string: &str) -> i64 {
        DateTime::parse_from_rfc3339(date_string)
            .unwrap()
            .with_timezone(&Utc)
            .timestamp_millis()
    }

    #[test]
    fn test_single_greater_than_or_equal() {
        let expr = col("timestamp").gt_eq(lit("2023-01-01T00:00:00Z"));
        let result = extract_wheel_range(&expr, "timestamp");
        assert_eq!(
            result,
            Some((Some(create_timestamp("2023-01-01T00:00:00Z")), None))
        );
    }

    #[test]
    fn test_range_with_and() {
        let expr = col("timestamp")
            .gt_eq(lit("2023-01-01T00:00:00Z"))
            .and(col("timestamp").lt(lit("2023-12-31T23:59:59Z")));
        let result = extract_wheel_range(&expr, "timestamp");
        assert_eq!(
            result,
            Some((
                Some(create_timestamp("2023-01-01T00:00:00Z")),
                Some(create_timestamp("2023-12-31T23:59:59Z"))
            ))
        );
    }
    #[test]
    fn test_with_non_matching_column() {
        let expr = col("other_column").gt_eq(lit("2023-01-01T00:00:00Z"));
        let result = extract_wheel_range(&expr, "timestamp");
        assert_eq!(result, None);
    }

    #[test]
    fn test_with_non_temporal_expression() {
        let expr = col("timestamp").eq(lit(42));
        let result = extract_wheel_range(&expr, "timestamp");
        assert_eq!(result, None);
    }

    /*
     #[test]
     fn test_range_with_less_than_or_equal() {
         let expr = col("timestamp")
             .gt_eq(lit("2023-01-01T00:00:00Z"))
             .and(col("timestamp").lt_eq(lit("2023-12-31T23:59:59Z")));
         let result = extract_datetime_range(&expr, "timestamp");
         assert_eq!(
             result,
             Some((
                 create_timestamp("2023-01-01T00:00:00Z"),
                 Some(create_timestamp("2023-12-31T23:59:59Z"))
             ))
         );
     }



    */

    /*
    use datafusion::arrow::datatypes::{Schema, TimeUnit};
    use datafusion::datasource::TableType;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::logical_expr::{LogicalPlanBuilder, TableSource};
    use datafusion::prelude::{col, lit, Expr};
    use std::any::Any;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_uwheel_planner() -> Result<()> {
        // Create a mock schema
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "tpep_dropoff_datetime",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("fare_amount", DataType::Float64, false),
        ]));

        let provider = Arc::new(MockTableProvider::new(schema));
        // Create a mock logical plan
        let logical_plan = LogicalPlanBuilder::scan("yellow_tripdata", provider.clone(), None)?
            .filter(col("tpep_dropoff_datetime").gt_eq(lit("2023-01-01T00:00:00Z")))?
            .filter(col("tpep_dropoff_datetime").lt(lit("2023-01-02T00:00:00Z")))?
            .build()?;

        // Create a mock session state
        let session_state =
            SessionState::new_with_config_rt(SessionConfig::new(), Arc::new(RuntimeEnv::default()));

        let optimizer = Arc::new(
            crate::builder::Builder::new("tpep_dropoff_datetime")
                .with_name("yellow_tripdata")
                .build_with_provider(provider)
                .await?,
        );

        Ok(())
    }

    // Mock TableProvider for testing
    struct MockTableProvider {
        schema: Arc<Schema>,
    }

    impl MockTableProvider {
        fn new(schema: Arc<Schema>) -> Self {
            Self { schema }
        }
    }

    impl TableSource for MockTableProvider {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> Arc<Schema> {
            self.schema.clone()
        }

        fn table_type(&self) -> TableType {
            TableType::Base
        }
    }

    #[async_trait::async_trait]
    impl TableProvider for MockTableProvider {
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn table_type(&self) -> TableType {
            TableType::Base
        }
        fn schema(&self) -> Arc<Schema> {
            self.schema.clone()
        }
        async fn scan(
            &self,
            _state: &SessionState,
            _projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            unimplemented!();
            /*
            Ok(Arc::new(MemoryExec::try_new(&vec![RecordBatch::try_new(
                self.schema.clone(),
                vec![
                    Arc::new(TimestampArray::from_vec(
                        vec![Some(1672531200000), Some(1672617600000)],
                        None,
                    )),
                    Arc::new(Float64Array::from_vec(vec![42.0, 43.0])),
                ],
            )?])?))
            */
        }
    }
    */
}
