use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
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
        expr::AggregateFunctionDefinition, AggregateFunction, Filter, LogicalPlan, Operator,
    },
    physical_plan::{common::collect, empty::EmptyExec, ExecutionPlan},
    physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner},
    prelude::*,
    scalar::ScalarValue,
};
use datafusion::{error::Result, logical_expr::Aggregate};
use exec::{UWheelCountExec, UWheelSumExec};
use expr::{extract_uwheel_expr, extract_wheel_range, MinMaxFilter, UWheelExpr};
use uwheel::{
    aggregator::{
        min_max::{F64MinMaxAggregator, MinMaxState},
        sum::{F64SumAggregator, U32SumAggregator},
    },
    wheels::read::ReaderWheel,
    Conf, Entry, HawConf, RwWheel, WheelRange,
};

/// Custom aggregator implementations that are used by this crate.
mod aggregator;
/// Builder for creating a UWheelOptimizer
pub mod builder;
/// Various Execution Plan implementations
pub mod exec;
/// Various expressions that the optimizer supports
mod expr;

pub const COUNT_STAR_ALIAS: &str = "COUNT(*)";

/// A µWheel optimizer for DataFusion that indexes wheels for time-based analytical queries.
#[allow(dead_code)]
pub struct UWheelOptimizer {
    /// Name of the table
    name: String,
    /// The column that contains the time
    time_column: String,
    /// A COUNT(*) wheel over the underlying table data and time column
    count: ReaderWheel<U32SumAggregator>,
    /// Min/Max pruning wheels for a specific column
    min_max_wheels: Arc<Mutex<HashMap<String, ReaderWheel<F64MinMaxAggregator>>>>,
    // TODO: add support for other aggregation wheels
    sum_wheels: Arc<Mutex<HashMap<String, ReaderWheel<F64SumAggregator>>>>,
    // aggregation_wheels: Arc<Mutex<HashMap<Expr, ReaderWheel<?>>>>,
    /// Table provider which UWheelOptimizer builds indexes on top of
    inner_provider: Arc<dyn TableProvider>,
    /// Default Wheel configuration that is used to build indexes
    haw_conf: HawConf,
}

impl UWheelOptimizer {
    /// Create a new UWheelOptimizer
    async fn try_new(
        name: impl Into<String>,
        time_column: impl Into<String>,
        min_max_columns: Vec<String>,
        sum_columns: Vec<String>,
        provider: Arc<dyn TableProvider>,
        haw_conf: HawConf,
    ) -> Result<Self> {
        let time_column = time_column.into();

        // Create an initial instance of the UWheelOptimizer
        let (count, min_max_wheels, sum_wheels) = build(
            provider.clone(),
            &time_column,
            min_max_columns,
            sum_columns,
            &haw_conf,
        )
        .await?;

        Ok(Self {
            name: name.into(),
            time_column,
            count,
            min_max_wheels: Arc::new(Mutex::new(min_max_wheels)),
            sum_wheels: Arc::new(Mutex::new(sum_wheels)),
            inner_provider: provider,
            haw_conf,
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
    /// This function takes a logical plan and checks whether it can be optimized using ``uwheel``
    ///
    /// If the plan can be optimized, it returns an optimized execution plan. Otherwise, it returns `None`.
    pub fn try_optimize(&self, plan: &LogicalPlan) -> Option<Arc<dyn ExecutionPlan>> {
        match plan {
            LogicalPlan::Aggregate(agg) => self.handle_aggregate(agg),
            LogicalPlan::Filter(filter) => self.handle_filter(filter, plan),
            _ => None,
        }
    }

    fn handle_aggregate(&self, agg: &Aggregate) -> Option<Arc<dyn ExecutionPlan>> {
        if let LogicalPlan::Projection(projection) = agg.input.as_ref() {
            if let LogicalPlan::Filter(filter) = projection.input.as_ref() {
                // SELECT AGG FROM X WHERE TIME >= X AND TIME <= Y

                // Check no GROUP BY and only one aggregation (for now)
                if agg.group_expr.is_empty() && agg.aggr_expr.len() == 1 {
                    let agg_expr = agg.aggr_expr.first().unwrap();
                    match agg_expr {
                        // COUNT(*)
                        Expr::Alias(alias) if alias.name == COUNT_STAR_ALIAS => {
                            let range = extract_wheel_range(&filter.predicate, &self.time_column)?;
                            return Some(self.count_exec(range));
                        }
                        // SUM(col)
                        Expr::AggregateFunction(agg)
                            if agg.args.len() == 1
                                && matches!(
                                    agg.func_def,
                                    AggregateFunctionDefinition::BuiltIn(AggregateFunction::Sum)
                                ) =>
                        {
                            if let Expr::Column(col) = &agg.args[0] {
                                let wheel = self.sum_wheels.lock().unwrap().get(&col.name)?.clone();
                                let range =
                                    extract_wheel_range(&filter.predicate, &self.time_column)?;
                                let table_ref = if let Some(table_ref) = col.relation.as_ref() {
                                    table_ref.to_quoted_string()
                                } else {
                                    "".to_string()
                                };
                                let name = format!("SUM({}.{})", table_ref, col.name);
                                return Some(self.sum_exec(wheel, name, range));
                            }
                        }
                        _ => (),
                    }
                }
            }
        }
        None
    }

    fn handle_filter(&self, filter: &Filter, plan: &LogicalPlan) -> Option<Arc<dyn ExecutionPlan>> {
        // Check COUNT wheel whether a range has any entries at
        if let Some(range) = extract_wheel_range(&filter.predicate, &self.time_column) {
            // query the wheel and if count == 0 then return empty execution.
            let count = self.count(range).unwrap_or(0);
            if count == 0 {
                let schema = Arc::new(plan.schema().clone().as_arrow().clone());
                Some(Arc::new(EmptyExec::new(schema)))
            } else {
                None
            }
        } else if let Some(UWheelExpr::MinMaxFilter(min_max)) =
            extract_uwheel_expr(&filter.predicate, &self.time_column)
        {
            self.maybe_min_max_filter(min_max, plan)
        } else {
            None
        }
    }
    fn maybe_min_max_filter(
        &self,
        min_max: MinMaxFilter,
        plan: &LogicalPlan,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        // First check whether there is a matching min max wheel
        let wheel = self.min_max_wheel(min_max.predicate.name.as_ref())?;

        // Cast the literal scalar value to f64
        let Ok(cast_scalar) = min_max.predicate.scalar.cast_to(&DataType::Float64) else {
            return None;
        };

        // extract the f64 value from the scalar
        let ScalarValue::Float64(Some(value)) = cast_scalar else {
            return None;
        };
        // query the MinMax state from our wheel
        let min_max_agg = wheel.combine_range_and_lower(min_max.range)?;

        maybe_min_max_exec(value, &min_max.predicate.op, min_max_agg, plan)
    }

    // helper fn to return a UWheelCount Execution plan
    fn count_exec(&self, range: WheelRange) -> Arc<dyn ExecutionPlan> {
        let name = COUNT_STAR_ALIAS.to_string();
        let schema = Arc::new(Schema::new(vec![Field::new(
            name.clone(),
            DataType::Int64,
            true,
        )]));
        Arc::new(UWheelCountExec::new(self.count.clone(), schema, range))
    }

    // helper fn to return a UWheelSum Execution plan
    fn sum_exec(
        &self,
        wheel: ReaderWheel<F64SumAggregator>,
        name: String,
        range: WheelRange,
    ) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            name.clone(),
            DataType::Float64,
            true,
        )]));
        Arc::new(UWheelSumExec::new(wheel, schema, range))
    }
}

// helper function to check whether we can return an empty execution plan based on min/max pruning
fn maybe_min_max_exec(
    value: f64,
    op: &Operator,
    min_max_agg: MinMaxState<f64>,
    plan: &LogicalPlan,
) -> Option<Arc<dyn ExecutionPlan>> {
    if op == &Operator::Gt && min_max_agg.max_value() < value
        || op == &Operator::GtEq && min_max_agg.max_value() <= value
        || op == &Operator::Lt && min_max_agg.min_value() > value
        || op == &Operator::LtEq && min_max_agg.min_value() >= value
    {
        Some(Arc::new(EmptyExec::new(Arc::new(
            plan.schema().clone().as_arrow().clone(),
        ))))
    } else {
        None
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
        // If the optimization fails, use the default physical planner
        match self.try_optimize(logical_plan) {
            Some(exec) => Ok(exec),
            None => {
                DefaultPhysicalPlanner::default()
                    .create_physical_plan(logical_plan, state)
                    .await
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
    sum_columns: Vec<String>,
    haw_conf: &HawConf,
) -> Result<(
    ReaderWheel<U32SumAggregator>,
    HashMap<String, ReaderWheel<F64MinMaxAggregator>>,
    HashMap<String, ReaderWheel<F64SumAggregator>>,
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
    let (count_wheel, min_timestamp_ms, max_timestamp_ms) = build_count_wheel(timestamps, haw_conf);

    // Build MinMax wheels for specified columns
    let mut min_max_map = HashMap::new();
    for column in min_max_columns.iter() {
        let min_max_wheel = build_min_max_wheel(
            provider.schema(),
            &batches,
            min_timestamp_ms,
            max_timestamp_ms,
            time_column,
            column,
            haw_conf,
        )
        .await?;
        min_max_map.insert(column.to_string(), min_max_wheel);
    }

    // Build Sum wheels for specified columns
    let mut sum_map = HashMap::new();
    for column in sum_columns.iter() {
        let sum_wheel = build_sum_wheel(
            provider.schema(),
            &batches,
            min_timestamp_ms,
            max_timestamp_ms,
            time_column,
            column,
            haw_conf,
        )
        .await?;
        sum_map.insert(column.to_string(), sum_wheel);
    }

    Ok((count_wheel.read().clone(), min_max_map, sum_map))
}

async fn build_min_max_wheel(
    schema: SchemaRef,
    batches: &[RecordBatch],
    _min_timestamp_ms: u64,
    max_timestamp_ms: u64,
    time_col: &str,
    min_max_col: &str,
    haw_conf: &HawConf,
) -> Result<ReaderWheel<F64MinMaxAggregator>> {
    // TODO: remove hardcoded time
    let start = NaiveDate::from_ymd_opt(2022, 1, 1).unwrap();
    let date = Utc.from_utc_datetime(&start.and_hms_opt(0, 0, 0).unwrap());
    let start_ms = date.timestamp_millis() as u64;

    let conf = haw_conf.with_watermark(start_ms);

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
                let timestamp_ms = DateTime::from_timestamp_micros(timestamp)
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

async fn build_sum_wheel(
    schema: SchemaRef,
    batches: &[RecordBatch],
    _min_timestamp_ms: u64,
    max_timestamp_ms: u64,
    time_col: &str,
    min_max_col: &str,
    haw_conf: &HawConf,
) -> Result<ReaderWheel<F64SumAggregator>> {
    // TODO: remove hardcoded time
    let start = NaiveDate::from_ymd_opt(2022, 1, 1).unwrap();
    let date = Utc.from_utc_datetime(&start.and_hms_opt(0, 0, 0).unwrap());
    let start_ms = date.timestamp_millis() as u64;

    let conf = haw_conf.with_watermark(start_ms);

    let mut wheel: RwWheel<F64SumAggregator> = RwWheel::with_conf(
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
                let timestamp_ms = DateTime::from_timestamp_micros(timestamp)
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
fn build_count_wheel(
    timestamps: Vec<i64>,
    haw_conf: &HawConf,
) -> (RwWheel<U32SumAggregator>, u64, u64) {
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

    let conf = haw_conf.with_watermark(start_ms);

    let mut count_wheel: RwWheel<U32SumAggregator> = RwWheel::with_conf(
        Conf::default()
            .with_haw_conf(conf)
            .with_write_ahead(64000usize.next_power_of_two()),
    );

    for timestamp in timestamps {
        let timestamp_ms = DateTime::from_timestamp_micros(timestamp)
            .unwrap()
            .timestamp_millis() as u64;
        // Record a count
        let entry = Entry::new(1, timestamp_ms);
        count_wheel.insert(entry);
    }
    //dbg!(start_ms, max_ms);

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
