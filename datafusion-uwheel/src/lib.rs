//! datafusion-uwheel is a DataFusion query optimizer that indexes data using [µWheel](https://github.com/uwheel/uwheel) to accelerate temporal query processing.
//!
//! Learn more about the project [here](https://github.com/uwheel/datafusion-wheel).
#![cfg_attr(docsrs, feature(doc_auto_cfg))]
#![deny(nonstandard_style, missing_copy_implementations, missing_docs)]
#![forbid(unsafe_code)]

use std::{
    collections::HashMap,
    fmt::Debug,
    sync::{Arc, Mutex},
};

use chrono::{DateTime, NaiveDate, Utc};
use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion::{
    arrow::{
        array::{
            Array, Date32Array, Date64Array, Float64Array, Int64Array, RecordBatch,
            TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
            TimestampSecondArray,
        },
        datatypes::{DataType, SchemaRef, TimeUnit},
    },
    common::{tree_node::Transformed, DFSchema, DFSchemaRef},
    datasource::{provider_as_source, MemTable, TableProvider},
    error::DataFusionError,
    logical_expr::{
        expr::AggregateFunction, Aggregate, AggregateUDF, Filter, LogicalPlan, LogicalPlanBuilder,
        Operator, Projection, TableScan,
    },
    optimizer::{optimizer::ApplyOrder, OptimizerConfig, OptimizerRule},
    scalar::ScalarValue,
    sql::TableReference,
};
use expr::{
    extract_filter_expr, extract_uwheel_expr, extract_wheel_range, MinMaxFilter, UWheelExpr,
};
use uwheel::{
    aggregator::{
        all::AllAggregator,
        avg::F64AvgAggregator,
        max::F64MaxAggregator,
        min::F64MinAggregator,
        min_max::{F64MinMaxAggregator, MinMaxState},
        sum::{F64SumAggregator, U32SumAggregator},
    },
    wheels::read::ReaderWheel,
    Aggregator, Conf, Entry, HawConf, RwWheel, WheelRange,
};

/// Custom aggregator implementations that are used by this crate.
mod aggregator;
/// Various expressions that the optimizer supports
mod expr;
/// Supported Built-in wheels
mod wheels;

/// Builder for creating a UWheelOptimizer
pub mod builder;
/// Module containing util for creating wheel indices
pub mod index;

pub use index::{IndexBuilder, UWheelAggregate};
use wheels::BuiltInWheels;

const COUNT_STAR_ALIAS: &str = "count(*)";
const STAR_AGGREGATION_ALIAS: &str = "*_AGG";

/// A µWheel optimizer for DataFusion that indexes wheels for time-based analytical queries.
///
/// See [builder::Builder] for how to build an optimizer instance
pub struct UWheelOptimizer {
    /// Name of the table
    name: String,
    /// The column that contains the time
    time_column: String,
    /// Set of built-in aggregation wheels
    wheels: BuiltInWheels,
    /// Table provider which UWheelOptimizer builds indexes on top of
    provider: Arc<dyn TableProvider>,
    /// The minimum timestamp in milliseconds for the underlying data using `time_column`
    min_timestamp_ms: u64,
    /// The maximum timestamp in milliseconds for the underlying data using `time_column`
    max_timestamp_ms: u64,
}

impl UWheelOptimizer {
    /// Create a new UWheelOptimizer
    async fn try_new(
        name: impl Into<String>,
        time_column: impl Into<String>,
        min_max_columns: Vec<String>,
        provider: Arc<dyn TableProvider>,
        haw_conf: HawConf,
        time_range: Option<(ScalarValue, ScalarValue)>,
    ) -> Result<Self> {
        let time_column = time_column.into();

        // Create an initial instance of the UWheelOptimizer
        let (min_timestamp_ms, max_timestamp_ms, count, min_max_wheels) = build(
            provider.clone(),
            &time_column,
            min_max_columns,
            &haw_conf,
            time_range,
        )
        .await?;
        let min_max_wheels = Arc::new(Mutex::new(min_max_wheels));
        let wheels = BuiltInWheels::new(count, min_max_wheels);

        Ok(Self {
            name: name.into(),
            time_column,
            wheels,
            provider,
            min_timestamp_ms,
            max_timestamp_ms,
        })
    }

    /// Count the number of rows in the given time range
    ///
    /// Returns `None` if the count is not available
    #[inline]
    pub fn count(&self, range: WheelRange) -> Option<u32> {
        self.wheels.count.combine_range_and_lower(range)
    }

    /// Returns the inner TableProvider
    pub fn provider(&self) -> Arc<dyn TableProvider> {
        self.provider.clone()
    }

    /// Returns a reference to the table's COUNT(*) wheel
    #[inline]
    pub fn count_wheel(&self) -> &ReaderWheel<U32SumAggregator> {
        &self.wheels.count
    }

    /// Returns the total number of bytes used by all wheel indices
    pub fn index_usage_bytes(&self) -> usize {
        self.wheels.index_usage_bytes()
    }

    /// Returns a reference to a MIN/MAX wheel for a specified column
    pub fn min_max_wheel(&self, column: &str) -> Option<ReaderWheel<F64MinMaxAggregator>> {
        self.wheels.min_max.lock().unwrap().get(column).cloned()
    }

    /// Builds an index using the specified [IndexBuilder]
    pub async fn build_index(&self, builder: IndexBuilder) -> Result<()> {
        let batches = prep_index_data(
            &self.time_column,
            self.provider.clone(),
            &builder.filter,
            &builder.time_range,
        )
        .await?;
        let schema = self.provider.schema();

        // Create a key for this wheel index either using the filter expression or a default STAR_AGGREGATION_ALIAS key
        let expr_key = builder
            .filter
            .as_ref()
            .map(|f| f.to_string())
            .unwrap_or(STAR_AGGREGATION_ALIAS.to_string());
        let col = builder.col.to_string();

        // Build Key for the wheel (table_name.column_name.expr)
        let expr_key = format!("{}.{}.{}", self.name, col, expr_key);

        match builder.agg_type {
            UWheelAggregate::Sum => {
                let wheel = build_uwheel::<F64SumAggregator>(
                    schema,
                    &batches,
                    self.min_timestamp_ms,
                    self.max_timestamp_ms,
                    &self.time_column,
                    &builder,
                )
                .await?;
                self.wheels.sum.lock().unwrap().insert(expr_key, wheel);
            }
            UWheelAggregate::Avg => {
                let wheel = build_uwheel::<F64AvgAggregator>(
                    schema,
                    &batches,
                    self.min_timestamp_ms,
                    self.max_timestamp_ms,
                    &self.time_column,
                    &builder,
                )
                .await?;
                self.wheels.avg.lock().unwrap().insert(expr_key, wheel);
            }
            UWheelAggregate::Min => {
                let wheel = build_uwheel::<F64MinAggregator>(
                    schema,
                    &batches,
                    self.min_timestamp_ms,
                    self.max_timestamp_ms,
                    &self.time_column,
                    &builder,
                )
                .await?;
                self.wheels.min.lock().unwrap().insert(expr_key, wheel);
            }
            UWheelAggregate::Max => {
                let wheel = build_uwheel::<F64MaxAggregator>(
                    schema,
                    &batches,
                    self.min_timestamp_ms,
                    self.max_timestamp_ms,
                    &self.time_column,
                    &builder,
                )
                .await?;
                self.wheels.max.lock().unwrap().insert(expr_key, wheel);
            }
            UWheelAggregate::All => {
                let wheel = build_uwheel::<AllAggregator>(
                    schema,
                    &batches,
                    self.min_timestamp_ms,
                    self.max_timestamp_ms,
                    &self.time_column,
                    &builder,
                )
                .await?;
                self.wheels.all.lock().unwrap().insert(expr_key, wheel);
            }
            _ => unimplemented!(),
        }
        Ok(())
    }
}

impl UWheelOptimizer {
    /// This function takes a logical plan and checks whether it can be rewritten using `uwheel`
    ///
    /// Returns `Some(LogicalPlan)` with the rewritten plan, otherwise None.
    pub fn try_rewrite(&self, plan: &LogicalPlan) -> Option<LogicalPlan> {
        match plan {
            LogicalPlan::Filter(filter) => self.try_rewrite_filter(filter, plan),
            LogicalPlan::Projection(projection) => self.try_rewrite_projection(projection, plan),
            _ => None, // cannot rewrite
        }
    }

    /// This function is used to determine if the Aggregate has a filter applied to its input.
    fn has_filter(agg: &Aggregate) -> bool {
        matches!(agg.input.as_ref(), LogicalPlan::Filter(_))
    }

    // Returns true if the aggregate contains an input Filter and has only one aggr expr
    fn single_aggregate_with_filter(agg: &Aggregate) -> bool {
        Self::has_filter(agg) && Self::single_agg(agg)
    }

    /// Checks whether the Aggregate has no group_expr and aggr_expr has a length of 1
    fn single_agg(agg: &Aggregate) -> bool {
        agg.group_expr.is_empty() && agg.aggr_expr.len() == 1
    }

    // Attemps to rewrite a top-level Projection plan
    fn try_rewrite_projection(
        &self,
        projection: &Projection,
        plan: &LogicalPlan,
    ) -> Option<LogicalPlan> {
        match projection.input.as_ref() {
            // SELECT AGG FROM X WHERE TIME >= X AND TIME <= Y
            LogicalPlan::Aggregate(agg) if Self::single_aggregate_with_filter(agg) => {
                // Only continue if the aggregation has a filter
                let LogicalPlan::Filter(filter) = agg.input.as_ref() else {
                    return None;
                };

                let agg_expr = agg.aggr_expr.first().unwrap();
                match agg_expr {
                    // COUNT(*)
                    Expr::Alias(alias) if alias.name == COUNT_STAR_ALIAS => {
                        self.try_count_rewrite(filter, plan)
                    }
                    // Also check
                    Expr::AggregateFunction(agg) if is_count_star_aggregate(agg) => {
                        self.try_count_rewrite(filter, plan)
                    }
                    // Single Aggregate Function (e.g., SUM(col))
                    Expr::AggregateFunction(agg) if agg.args.len() == 1 => {
                        if let Expr::Column(col) = &agg.args[0] {
                            // Fetch temporal filter range and expr key which is used to identify a wheel
                            let (range, expr_key) =
                                match extract_filter_expr(&filter.predicate, &self.time_column)? {
                                    (range, Some(expr)) => {
                                        (range, maybe_replace_table_name(&expr, &self.name))
                                    }
                                    (range, None) => (range, STAR_AGGREGATION_ALIAS.to_string()),
                                };

                            // build the key for the wheel
                            let wheel_key = format!("{}.{}.{}", self.name, col.name, expr_key);

                            let agg_type = func_def_to_aggregate_type(&agg.func)?;
                            let schema = Arc::new(plan.schema().clone().as_arrow().clone());
                            self.create_uwheel_plan(agg_type, &wheel_key, range, schema)
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            }
            // Check whether it follows the pattern: SELECT * FROM X WHERE TIME >= X AND TIME <= Y
            LogicalPlan::Filter(filter) => self.try_rewrite_filter(filter, plan),
            _ => None,
        }
    }

    // Attemps to rewrite a top-level Filter plan
    fn try_rewrite_filter(&self, filter: &Filter, plan: &LogicalPlan) -> Option<LogicalPlan> {
        // The optimizer supports two types of filtering
        // 1. count-based using the Count wheel
        // 2. min-max filtering using a MinMax wheel

        match extract_uwheel_expr(&filter.predicate, &self.time_column) {
            // Matches a COUNT(*) filter
            Some(UWheelExpr::WheelRange(range)) => self.maybe_count_filter(range, plan),
            // Matches a MinMax filter
            Some(UWheelExpr::MinMaxFilter(min_max)) => self.maybe_min_max_filter(min_max, plan),
            _ => None,
        }
    }

    fn try_count_rewrite(&self, filter: &Filter, plan: &LogicalPlan) -> Option<LogicalPlan> {
        let range = extract_wheel_range(&filter.predicate, &self.time_column)?;
        let count = self.count(range)?; // early return if range is not queryable
        let schema = Arc::new(plan.schema().clone().as_arrow().clone());
        count_scan(count, schema).ok()
    }

    // Queries the range using the count wheel, returning a empty table scan if the count is 0
    // avoiding the need to generate a regular execution plan..
    fn maybe_count_filter(&self, range: WheelRange, plan: &LogicalPlan) -> Option<LogicalPlan> {
        let count = self.count(range)?; // early return if range can't be queried

        if count == 0 {
            let schema = Arc::new(plan.schema().clone().as_arrow().clone());
            let table_ref = extract_table_reference(plan)?;
            empty_table_scan(table_ref, schema).ok()
        } else {
            None
        }
    }

    // Takes a MinMax filter and possibly returns an empty TableScan if the filter indicates that the result is empty
    fn maybe_min_max_filter(
        &self,
        min_max: MinMaxFilter,
        plan: &LogicalPlan,
    ) -> Option<LogicalPlan> {
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

        // If the value is outside the range of the MinMax state, we can return an empty table scan
        if is_empty_range(value, &min_max.predicate.op, min_max_agg) {
            let schema = Arc::new(plan.schema().clone().as_arrow().clone());
            let table_ref = extract_table_reference(plan)?;
            empty_table_scan(table_ref, schema).ok()
        } else {
            None
        }
    }

    // Takes a wheel range and returns a plan-time aggregate result using a `TableScan(MemTable)`
    fn create_uwheel_plan(
        &self,
        agg_type: UWheelAggregate,
        wheel_key: &str,
        range: WheelRange,
        schema: SchemaRef,
    ) -> Option<LogicalPlan> {
        match agg_type {
            UWheelAggregate::Sum => {
                let wheel = self.wheels.sum.lock().unwrap().get(wheel_key)?.clone();
                let result = wheel.combine_range_and_lower(range)?;
                uwheel_agg_to_table_scan(result, schema).ok()
            }
            UWheelAggregate::Avg => {
                let wheel = self.wheels.avg.lock().unwrap().get(wheel_key)?.clone();
                let result = wheel.combine_range_and_lower(range)?;

                uwheel_agg_to_table_scan(result, schema).ok()
            }
            UWheelAggregate::Min => {
                let wheel = self.wheels.min.lock().unwrap().get(wheel_key)?.clone();
                let result = wheel.combine_range_and_lower(range)?;
                uwheel_agg_to_table_scan(result, schema).ok()
            }
            UWheelAggregate::Max => {
                let wheel = self.wheels.max.lock().unwrap().get(wheel_key)?.clone();
                let result = wheel.combine_range_and_lower(range)?;
                uwheel_agg_to_table_scan(result, schema).ok()
            }
            _ => unimplemented!(),
        }
    }
}

fn count_scan(count: u32, schema: SchemaRef) -> Result<LogicalPlan> {
    let data = Int64Array::from(vec![count as i64]);
    let record_batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(data)])?;
    let df_schema = Arc::new(DFSchema::try_from(schema.clone())?);
    let mem_table = MemTable::try_new(schema, vec![vec![record_batch]])?;

    mem_table_as_table_scan(mem_table, df_schema)
}

// Converts a uwheel aggregate result to a TableScan with a MemTable as source
fn uwheel_agg_to_table_scan(result: f64, schema: SchemaRef) -> Result<LogicalPlan> {
    let data = Float64Array::from(vec![result]);
    let record_batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(data)])?;
    let df_schema = Arc::new(DFSchema::try_from(schema.clone())?);
    let mem_table = MemTable::try_new(schema, vec![vec![record_batch]])?;
    mem_table_as_table_scan(mem_table, df_schema)
}

// helper for possibly removing the table name from the expression key
fn maybe_replace_table_name(expr: &Expr, table_name: &str) -> String {
    let expr_str = expr.to_string();
    let replace_key = format!("{}.", table_name);
    expr_str.replace(&replace_key, "")
}

// helper method to extract a table reference from a logical plan
fn extract_table_reference(filter: &LogicalPlan) -> Option<TableReference> {
    match filter {
        LogicalPlan::Projection(projection) => extract_table_reference(projection.input.as_ref()),
        LogicalPlan::Filter(filter) => {
            // Recursively search the input plan
            extract_table_reference(&filter.input)
        }
        LogicalPlan::TableScan(scan) => {
            // We've found a table scan, return the table name
            Some(scan.table_name.clone())
        }
        // Add other cases as needed
        _ => None,
    }
}

// helper function to check whether execution can be skipped based on min/max pruning
fn is_empty_range(value: f64, op: &Operator, min_max_agg: MinMaxState<f64>) -> bool {
    let max = min_max_agg.max_value();
    let min = min_max_agg.min_value();
    op == &Operator::Gt && max < value
        || op == &Operator::GtEq && max <= value
        || op == &Operator::Lt && min > value
        || op == &Operator::LtEq && min >= value
}

// helper fn to create an empty table scan
fn empty_table_scan(
    table_ref: impl Into<TableReference>,
    schema: SchemaRef,
) -> Result<LogicalPlan> {
    let mem_table = MemTable::try_new(schema, vec![])?;
    let source = provider_as_source(Arc::new(mem_table));
    LogicalPlanBuilder::scan(table_ref.into(), source, None)?.build()
}

fn func_def_to_aggregate_type(func_def: &Arc<AggregateUDF>) -> Option<UWheelAggregate> {
    match func_def.name() {
        "max" => Some(UWheelAggregate::Max),
        "min" => Some(UWheelAggregate::Min),
        "avg" => Some(UWheelAggregate::Avg),
        "sum" => Some(UWheelAggregate::Sum),
        "count" => Some(UWheelAggregate::Count),
        _ => None,
    }
}

impl Debug for UWheelOptimizer {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl OptimizerRule for UWheelOptimizer {
    fn name(&self) -> &str {
        "uwheel_optimizer_rewriter"
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        None
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        // Attemps to rewrite a logical plan to a uwheel-based plan that either provides
        // plan-time aggregates or skips execution based on min/max pruning.
        if let Some(rewritten) = self.try_rewrite(&plan) {
            Ok(Transformed::yes(rewritten))
        } else {
            Ok(Transformed::no(plan))
        }
    }
}

// Turns a MemTable into a TableScan which contains pre-computed uwheel aggregates available at plan time
fn mem_table_as_table_scan(table: MemTable, original_schema: DFSchemaRef) -> Result<LogicalPlan> {
    // Convert MemTable to a TableSource
    let source = provider_as_source(Arc::new(table));
    // Create a TableScan using a dummmy table reference name
    let mut table_scan = TableScan::try_new("dummy", source, None, Vec::new(), None)?;
    // Replace the schema with the original schema since we are forced to create a table reference
    // for the TableScan which may be empty in the original plan.
    table_scan.projected_schema = original_schema;
    Ok(LogicalPlan::TableScan(table_scan))
}

fn is_wildcard(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Wildcard {
            qualifier: None,
            ..
        }
    )
}

/// Determines if the given aggregate function is a COUNT(*) aggregate.
///
/// An aggregate function is a COUNT(*) aggregate if its function name is "COUNT" and it either has a single argument that is a wildcard (`*`), or it has no arguments.
///
/// # Arguments
///
/// * `aggregate_function` - The aggregate function to check.
///
/// # Returns
///
/// `true` if the aggregate function is a COUNT(*) aggregate, `false` otherwise.
fn is_count_star_aggregate(aggregate_function: &AggregateFunction) -> bool {
    matches!(aggregate_function,
        AggregateFunction {
            func,
            args,
            ..
        } if func.name() == "COUNT" && (args.len() == 1 && is_wildcard(&args[0]) || args.is_empty()))
}

// Helper methods to build the UWheelOptimizer

// Uses the provided TableProvider to build the UWheelOptimizer
async fn build(
    provider: Arc<dyn TableProvider>,
    time_column: &str,
    min_max_columns: Vec<String>,
    haw_conf: &HawConf,
    time_range: Option<(ScalarValue, ScalarValue)>,
) -> Result<(
    u64,
    u64,
    ReaderWheel<U32SumAggregator>,
    HashMap<String, ReaderWheel<F64MinMaxAggregator>>,
)> {
    let batches = prep_index_data(time_column, provider.clone(), &None, &time_range).await?;
    let mut timestamps = Vec::new();

    for batch in batches.iter() {
        // Verify the time column exists in the parquet file
        let time_column_exists = batch.schema().index_of(time_column).is_ok();
        assert!(
            time_column_exists,
            "Specified Time column does not exist in the provided data"
        );

        let time_array = batch.column_by_name(time_column).unwrap();
        let batch_timestamps = extract_timestamps_from_array(time_array)?;
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

    Ok((
        min_timestamp_ms,
        max_timestamp_ms,
        count_wheel.read().clone(),
        min_max_map,
    ))
}

async fn build_min_max_wheel(
    schema: SchemaRef,
    batches: &[RecordBatch],
    min_timestamp_ms: u64,
    max_timestamp_ms: u64,
    time_col: &str,
    min_max_col: &str,
    haw_conf: &HawConf,
) -> Result<ReaderWheel<F64MinMaxAggregator>> {
    let conf = haw_conf.with_watermark(min_timestamp_ms);

    let mut wheel: RwWheel<F64MinMaxAggregator> = RwWheel::with_conf(
        Conf::default()
            .with_haw_conf(conf)
            .with_write_ahead(64000usize.next_power_of_two()),
    );

    let column_index = schema.index_of(min_max_col)?;
    let column_field = schema.field(column_index);

    if is_numeric_type(column_field.data_type()) {
        for batch in batches {
            let time_array = batch.column_by_name(time_col).unwrap();

            let time_values = extract_timestamps_from_array(time_array)?;

            let min_max_column_array = batch
                .column_by_name(min_max_col)
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();

            for (timestamp, value) in time_values
                .iter()
                .copied()
                .zip(min_max_column_array.values().iter().copied())
            {
                let entry = Entry::new(value, timestamp);
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

async fn build_uwheel<A>(
    schema: SchemaRef,
    batches: &[RecordBatch],
    min_timestamp_ms: u64,
    max_timestamp_ms: u64,
    time_col: &str,
    builder: &IndexBuilder,
) -> Result<ReaderWheel<A>>
where
    A: Aggregator<Input = f64>,
{
    let target_col = builder.col.to_string();
    let haw_conf = builder.conf;

    // Define the start time for the wheel index

    let (start_ms, end_ms) = builder
        .time_range
        .as_ref()
        .map(|(start, end)| {
            (
                scalar_to_timestamp(start).unwrap() as u64,
                scalar_to_timestamp(end).unwrap() as u64,
            )
        })
        .unwrap_or((min_timestamp_ms, max_timestamp_ms));

    let conf = haw_conf.with_watermark(start_ms);

    let mut wheel: RwWheel<A> = RwWheel::with_conf(
        Conf::default()
            .with_haw_conf(conf)
            .with_write_ahead(64000usize.next_power_of_two()),
    );

    let column_index = schema.index_of(&target_col)?;
    let column_field = schema.field(column_index);

    if is_numeric_type(column_field.data_type()) {
        for batch in batches {
            let time_array = batch.column_by_name(time_col).unwrap();

            let time_values = extract_timestamps_from_array(time_array)?;

            let column_array = batch
                .column_by_name(&target_col)
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();

            for (timestamp, value) in time_values
                .iter()
                .copied()
                .zip(column_array.values().iter().copied())
            {
                let entry = Entry::new(value, timestamp);
                wheel.insert(entry);
            }
        }
        // Once all data is inserted, advance the wheel to the max timestamp + 1 second
        wheel.advance_to(end_ms + 1000);

        // convert wheel to index
        wheel.read().to_simd_wheels();
        // TODO: make this configurable
        if A::invertible() {
            wheel.read().to_prefix_wheels();
        }
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
    timestamps: Vec<u64>,
    haw_conf: &HawConf,
) -> (RwWheel<U32SumAggregator>, u64, u64) {
    let min_ms = timestamps.iter().min().copied().unwrap();
    let max_ms = timestamps.iter().max().copied().unwrap();

    let conf = haw_conf.with_watermark(min_ms);

    let mut count_wheel: RwWheel<U32SumAggregator> = RwWheel::with_conf(
        Conf::default()
            .with_haw_conf(conf)
            .with_write_ahead(64000usize.next_power_of_two()),
    );

    for timestamp in timestamps {
        // Record a count
        let entry = Entry::new(1, timestamp);
        count_wheel.insert(entry);
    }

    count_wheel.advance_to(max_ms + 1000); // + 1 second

    // convert wheel to index
    count_wheel.read().to_simd_wheels();
    count_wheel.read().to_prefix_wheels();

    (count_wheel, min_ms, max_ms)
}

// internal helper that fetches the record batches
async fn prep_index_data(
    time_column: &str,
    provider: Arc<dyn TableProvider>,
    filter: &Option<Expr>,
    time_range: &Option<(ScalarValue, ScalarValue)>,
) -> Result<Vec<RecordBatch>> {
    let ctx = SessionContext::new();
    let df = ctx.read_table(provider)?;

    // Apply filter if it exists
    let df = if let Some(filter) = filter {
        df.filter(filter.clone())?
    } else {
        df
    };

    // Apply time-range filter if specified
    let df = if let Some((start_range, end_range)) = time_range {
        // WHERE "time_column" >= start_range AND "time_column" < end_range
        let time_filter = col(time_column)
            .gt_eq(lit(start_range.clone()))
            .and(col(time_column).lt(lit(end_range.clone())));
        df.filter(time_filter)?
    } else {
        df
    };

    df.collect().await
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

fn scalar_to_timestamp(scalar: &ScalarValue) -> Option<i64> {
    match scalar {
        ScalarValue::Utf8(Some(date_str)) => DateTime::parse_from_rfc3339(date_str)
            .ok()
            .map(|dt| dt.with_timezone(&Utc).timestamp_millis()),
        ScalarValue::TimestampMillisecond(Some(ms), _) => Some(*ms),
        ScalarValue::TimestampMicrosecond(Some(us), _) => Some(*us / 1000_i64),
        ScalarValue::TimestampNanosecond(Some(ns), _) => Some(ns / 1_000_000),
        ScalarValue::Date32(Some(days)) => NaiveDate::from_num_days_from_ce_opt(*days)
            .and_then(|date| date.and_hms_opt(0, 0, 0))
            .map(|dt| dt.and_utc().timestamp_millis()),
        ScalarValue::Date64(Some(ms)) => Some(*ms),
        _ => None,
    }
}

/// Extracts timestamps from a given array of timestamp data.
/// The function supports various timestamp data types, including:
/// - Timestamp (Nanosecond, Microsecond, Millisecond, Second)
/// - Time64 (Nanosecond, Microsecond)
/// - Time32 (Millisecond, Second)
/// - Date32
/// - Date64
///
/// The function returns a `Vec<u64>` containing the extracted timestamps in milliseconds.
fn extract_timestamps_from_array(time_array: &Arc<dyn Array>) -> Result<Vec<u64>> {
    let data_type = time_array.data_type();
    match data_type {
        DataType::Timestamp(TimeUnit::Nanosecond, _) | DataType::Time64(TimeUnit::Nanosecond) => {
            Ok(time_array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap()
                .values()
                .iter()
                .copied()
                .map(|ts| (ts / 1_000_000) as u64)
                .collect())
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) | DataType::Time64(TimeUnit::Microsecond) => {
            Ok(time_array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .values()
                .iter()
                .copied()
                .map(|ts| (ts / 1_000) as u64)
                .collect())
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) | DataType::Time32(TimeUnit::Millisecond) => {
            Ok(time_array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .values()
                .iter()
                .copied()
                .map(|ts| ts as u64)
                .collect())
        }
        DataType::Timestamp(TimeUnit::Second, _) | DataType::Time32(TimeUnit::Second) => {
            Ok(time_array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap()
                .values()
                .iter()
                .copied()
                .map(|ts| (ts * 1_000) as u64)
                .collect())
        }
        DataType::Date32 => Ok(time_array
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap()
            .values()
            .iter()
            .copied()
            .map(|ts| ts as u64)
            .collect()),
        DataType::Date64 => Ok(time_array
            .as_any()
            .downcast_ref::<Date64Array>()
            .unwrap()
            .values()
            .iter()
            .copied()
            .map(|ts| ts as u64)
            .collect()),
        _ => Err(DataFusionError::Internal(
            "Unsupported timestamp data type, this function supports: DataType::Timestamp(), DataType::Time32/64, DataType::Date32/64 ".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use chrono::Duration;
    use chrono::TimeZone;
    use datafusion::arrow::datatypes::{Field, Schema, TimeUnit};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::functions_aggregate::expr_fn::avg;
    use datafusion::functions_aggregate::min_max::{max, min};
    use datafusion::logical_expr::test::function_stub::{count, sum};

    use super::*;
    use builder::Builder;

    fn create_test_memtable() -> Result<MemTable> {
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

    #[tokio::test]
    async fn create_optimizer_with_memtable() {
        let provider = Arc::new(create_test_memtable().unwrap());
        assert!(Builder::new("timestamp")
            .with_name("test")
            .build_with_provider(provider)
            .await
            .is_ok());
    }

    async fn test_optimizer() -> Result<Arc<UWheelOptimizer>> {
        let provider = Arc::new(create_test_memtable()?);
        Ok(Arc::new(
            Builder::new("timestamp")
                .with_name("test")
                .build_with_provider(provider)
                .await?,
        ))
    }

    #[tokio::test]
    async fn count_star_aggregation_rewrite() -> Result<()> {
        let optimizer = test_optimizer().await?;
        let temporal_filter = col("timestamp")
            .gt_eq(lit("2024-05-10T00:00:00Z"))
            .and(col("timestamp").lt(lit("2024-05-10T00:00:10Z")));

        let plan =
            LogicalPlanBuilder::scan("test", provider_as_source(optimizer.provider()), None)?
                .filter(temporal_filter)?
                .aggregate(Vec::<Expr>::new(), vec![count(wildcard())])?
                .project(vec![count(wildcard())])?
                .build()?;

        // Assert that the original plan is a Projection
        assert!(matches!(plan, LogicalPlan::Projection(_)));

        let rewritten = optimizer.try_rewrite(&plan).unwrap();
        // assert it was rewritten to a TableScan
        assert!(matches!(rewritten, LogicalPlan::TableScan(_)));

        Ok(())
    }

    #[tokio::test]
    async fn sum_aggregation_rewrite() -> Result<()> {
        let optimizer = test_optimizer().await?;

        // Build a sum index
        optimizer
            .build_index(IndexBuilder::with_col_and_aggregate(
                "agg_col",
                UWheelAggregate::Sum,
            ))
            .await?;

        let temporal_filter = col("timestamp")
            .gt_eq(lit("2024-05-10T00:00:00Z"))
            .and(col("timestamp").lt(lit("2024-05-10T00:00:10Z")));

        let plan =
            LogicalPlanBuilder::scan("test", provider_as_source(optimizer.provider()), None)?
                .filter(temporal_filter)?
                .aggregate(Vec::<Expr>::new(), vec![sum(col("agg_col"))])?
                .project(vec![sum(col("agg_col"))])?
                .build()?;

        // Assert that the original plan is a Projection
        assert!(matches!(plan, LogicalPlan::Projection(_)));

        let rewritten = optimizer.try_rewrite(&plan).unwrap();
        // assert it was rewritten to a TableScan
        assert!(matches!(rewritten, LogicalPlan::TableScan(_)));

        Ok(())
    }

    #[tokio::test]
    async fn min_aggregation_rewrite() -> Result<()> {
        let optimizer = test_optimizer().await?;

        // Build a min index
        optimizer
            .build_index(IndexBuilder::with_col_and_aggregate(
                "agg_col",
                UWheelAggregate::Min,
            ))
            .await?;

        let temporal_filter = col("timestamp")
            .gt_eq(lit("2024-05-10T00:00:00Z"))
            .and(col("timestamp").lt(lit("2024-05-10T00:00:10Z")));

        let plan =
            LogicalPlanBuilder::scan("test", provider_as_source(optimizer.provider()), None)?
                .filter(temporal_filter)?
                .aggregate(Vec::<Expr>::new(), vec![min(col("agg_col"))])?
                .project(vec![min(col("agg_col"))])?
                .build()?;

        // Assert that the original plan is a Projection
        assert!(matches!(plan, LogicalPlan::Projection(_)));

        let rewritten = optimizer.try_rewrite(&plan).unwrap();
        // assert it was rewritten to a TableScan
        assert!(matches!(rewritten, LogicalPlan::TableScan(_)));

        Ok(())
    }

    #[tokio::test]
    async fn max_aggregation_rewrite() -> Result<()> {
        let optimizer = test_optimizer().await?;

        optimizer
            .build_index(IndexBuilder::with_col_and_aggregate(
                "agg_col",
                UWheelAggregate::Max,
            ))
            .await?;

        let temporal_filter = col("timestamp")
            .gt_eq(lit("2024-05-10T00:00:00Z"))
            .and(col("timestamp").lt(lit("2024-05-10T00:00:10Z")));

        let plan =
            LogicalPlanBuilder::scan("test", provider_as_source(optimizer.provider()), None)?
                .filter(temporal_filter)?
                .aggregate(Vec::<Expr>::new(), vec![max(col("agg_col"))])?
                .project(vec![max(col("agg_col"))])?
                .build()?;

        // Assert that the original plan is a Projection
        assert!(matches!(plan, LogicalPlan::Projection(_)));

        let rewritten = optimizer.try_rewrite(&plan).unwrap();
        // assert it was rewritten to a TableScan
        assert!(matches!(rewritten, LogicalPlan::TableScan(_)));

        Ok(())
    }

    #[tokio::test]
    async fn avg_aggregation_rewrite() -> Result<()> {
        let optimizer = test_optimizer().await?;

        optimizer
            .build_index(IndexBuilder::with_col_and_aggregate(
                "agg_col",
                UWheelAggregate::Avg,
            ))
            .await?;

        let temporal_filter = col("timestamp")
            .gt_eq(lit("2024-05-10T00:00:00Z"))
            .and(col("timestamp").lt(lit("2024-05-10T00:00:10Z")));

        let plan =
            LogicalPlanBuilder::scan("test", provider_as_source(optimizer.provider()), None)?
                .filter(temporal_filter)?
                .aggregate(Vec::<Expr>::new(), vec![avg(col("agg_col"))])?
                .project(vec![avg(col("agg_col"))])?
                .build()?;

        // Assert that the original plan is a Projection
        assert!(matches!(plan, LogicalPlan::Projection(_)));

        let rewritten = optimizer.try_rewrite(&plan).unwrap();
        // assert it was rewritten to a TableScan
        assert!(matches!(rewritten, LogicalPlan::TableScan(_)));

        Ok(())
    }

    #[tokio::test]
    async fn count_star_aggregation_invalid_rewrite() -> Result<()> {
        let optimizer = test_optimizer().await?;
        // invalid temporal filter
        let temporal_filter = col("timestamp")
            .gt_eq(lit("2024-05-11T00:00:00Z"))
            .and(col("timestamp").lt(lit("2024-05-11T00:00:10Z")));

        let plan =
            LogicalPlanBuilder::scan("test", provider_as_source(optimizer.provider()), None)?
                .filter(temporal_filter)?
                .aggregate(Vec::<Expr>::new(), vec![count(wildcard())])?
                .project(vec![count(wildcard())])?
                .build()?;

        assert!(optimizer.try_rewrite(&plan).is_none());

        Ok(())
    }

    #[tokio::test]
    async fn count_star_aggregation_exec() -> Result<()> {
        let optimizer = test_optimizer().await?;
        let temporal_filter = col("timestamp")
            .gt_eq(lit("2024-05-10T00:00:00Z"))
            .and(col("timestamp").lt(lit("2024-05-10T00:00:10Z")));

        let plan =
            LogicalPlanBuilder::scan("test", provider_as_source(optimizer.provider()), None)?
                .filter(temporal_filter)?
                .aggregate(Vec::<Expr>::new(), vec![count(wildcard())])?
                .project(vec![count(wildcard())])?
                .build()?;

        let ctx = SessionContext::new();
        ctx.register_table("test", optimizer.provider().clone())?;

        // Set UWheelOptimizer as optimizer rule
        let session_state = SessionStateBuilder::new()
            .with_optimizer_rules(vec![optimizer.clone()])
            .build();
        let uwheel_ctx = SessionContext::new_with_state(session_state);

        // Run the query through the ctx that has our OptimizerRule
        let df = uwheel_ctx.execute_logical_plan(plan).await?;
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

        Ok(())
    }

    #[tokio::test]
    async fn sum_aggregation_exec() -> Result<()> {
        let optimizer = test_optimizer().await?;
        optimizer
            .build_index(IndexBuilder::with_col_and_aggregate(
                "agg_col",
                UWheelAggregate::Sum,
            ))
            .await?;

        let temporal_filter = col("timestamp")
            .gt_eq(lit("2024-05-10T00:00:00Z"))
            .and(col("timestamp").lt(lit("2024-05-10T00:00:10Z")));

        let plan =
            LogicalPlanBuilder::scan("test", provider_as_source(optimizer.provider()), None)?
                .filter(temporal_filter)?
                .aggregate(Vec::<Expr>::new(), vec![sum(col("agg_col"))])?
                .project(vec![sum(col("agg_col"))])?
                .build()?;

        let ctx = SessionContext::new();
        ctx.register_table("test", optimizer.provider().clone())?;

        // Set UWheelOptimizer as optimizer rule
        let session_state = SessionStateBuilder::new()
            .with_optimizer_rules(vec![optimizer.clone()])
            .build();
        let uwheel_ctx = SessionContext::new_with_state(session_state);

        // Run the query through the ctx that has our OptimizerRule
        let df = uwheel_ctx.execute_logical_plan(plan).await?;
        let results = df.collect().await?;

        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0]
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            55.0 //  1 + 2 +3 + ... + 9 + 10 = 55
        );

        Ok(())
    }

    #[tokio::test]
    async fn min_aggregation_exec() -> Result<()> {
        let optimizer = test_optimizer().await?;
        optimizer
            .build_index(IndexBuilder::with_col_and_aggregate(
                "agg_col",
                UWheelAggregate::Min,
            ))
            .await?;

        let temporal_filter = col("timestamp")
            .gt_eq(lit("2024-05-10T00:00:00Z"))
            .and(col("timestamp").lt(lit("2024-05-10T00:00:10Z")));

        let plan =
            LogicalPlanBuilder::scan("test", provider_as_source(optimizer.provider()), None)?
                .filter(temporal_filter)?
                .aggregate(Vec::<Expr>::new(), vec![min(col("agg_col"))])?
                .project(vec![min(col("agg_col"))])?
                .build()?;

        let ctx = SessionContext::new();
        ctx.register_table("test", optimizer.provider().clone())?;

        // Set UWheelOptimizer as optimizer rule
        let session_state = SessionStateBuilder::new()
            .with_optimizer_rules(vec![optimizer.clone()])
            .build();
        let uwheel_ctx = SessionContext::new_with_state(session_state);

        // Run the query through the ctx that has our OptimizerRule
        let df = uwheel_ctx.execute_logical_plan(plan).await?;
        let results = df.collect().await?;

        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0]
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            1.0
        );

        Ok(())
    }

    #[tokio::test]
    async fn max_aggregation_exec() -> Result<()> {
        let optimizer = test_optimizer().await?;
        optimizer
            .build_index(IndexBuilder::with_col_and_aggregate(
                "agg_col",
                UWheelAggregate::Max,
            ))
            .await?;

        let temporal_filter = col("timestamp")
            .gt_eq(lit("2024-05-10T00:00:00Z"))
            .and(col("timestamp").lt(lit("2024-05-10T00:00:10Z")));

        let plan =
            LogicalPlanBuilder::scan("test", provider_as_source(optimizer.provider()), None)?
                .filter(temporal_filter)?
                .aggregate(Vec::<Expr>::new(), vec![max(col("agg_col"))])?
                .project(vec![max(col("agg_col"))])?
                .build()?;

        let ctx = SessionContext::new();
        ctx.register_table("test", optimizer.provider().clone())?;

        // Set UWheelOptimizer as optimizer rule
        let session_state = SessionStateBuilder::new()
            .with_optimizer_rules(vec![optimizer.clone()])
            .build();
        let uwheel_ctx = SessionContext::new_with_state(session_state);

        // Run the query through the ctx that has our OptimizerRule
        let df = uwheel_ctx.execute_logical_plan(plan).await?;
        let results = df.collect().await?;

        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0]
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            10.0
        );

        Ok(())
    }

    #[tokio::test]
    async fn avg_aggregation_exec() -> Result<()> {
        let optimizer = test_optimizer().await?;
        optimizer
            .build_index(IndexBuilder::with_col_and_aggregate(
                "agg_col",
                UWheelAggregate::Avg,
            ))
            .await?;

        let temporal_filter = col("timestamp")
            .gt_eq(lit("2024-05-10T00:00:00Z"))
            .and(col("timestamp").lt(lit("2024-05-10T00:00:10Z")));

        let plan =
            LogicalPlanBuilder::scan("test", provider_as_source(optimizer.provider()), None)?
                .filter(temporal_filter)?
                .aggregate(Vec::<Expr>::new(), vec![avg(col("agg_col"))])?
                .project(vec![avg(col("agg_col"))])?
                .build()?;

        let ctx = SessionContext::new();
        ctx.register_table("test", optimizer.provider().clone())?;

        // Set UWheelOptimizer as optimizer rule
        let session_state = SessionStateBuilder::new()
            .with_optimizer_rules(vec![optimizer.clone()])
            .build();
        let uwheel_ctx = SessionContext::new_with_state(session_state);

        // Run the query through the ctx that has our OptimizerRule
        let df = uwheel_ctx.execute_logical_plan(plan).await?;
        let results = df.collect().await?;

        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0]
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            5.5,
        );

        Ok(())
    }
}
