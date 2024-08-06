use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use datafusion::error::Result;
use datafusion::{
    arrow::{
        array::{ArrayRef, Float64Array, Int64Array, RecordBatch, TimestampMicrosecondArray},
        datatypes::{DataType, SchemaRef},
    },
    common::{tree_node::Transformed, DFSchema, DFSchemaRef},
    datasource::{provider_as_source, MemTable, TableProvider},
    execution::TaskContext,
    logical_expr::{
        expr::AggregateFunctionDefinition, Filter, LogicalPlan, LogicalPlanBuilder, Operator,
        Projection, TableScan,
    },
    optimizer::{optimizer::ApplyOrder, OptimizerConfig, OptimizerRule},
    physical_plan::common::collect,
    prelude::*,
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
/// Builder for creating a UWheelOptimizer
pub mod builder;
/// Various expressions that the optimizer supports
mod expr;

mod index;

pub use index::{AggregateType, IndexBuilder};

pub const COUNT_STAR_ALIAS: &str = "count(*)";
pub const STAR_AGGREGATION_ALIAS: &str = "*_AGG";

pub type WheelMap<A> = Arc<Mutex<HashMap<String, ReaderWheel<A>>>>;

#[derive(Clone)]
pub struct BuiltInWheels {
    /// A COUNT(*) wheel over the underlying table data and time column
    pub count: ReaderWheel<U32SumAggregator>,
    /// Min/Max pruning wheels for a specific column
    pub min_max: WheelMap<F64MinMaxAggregator>,
    /// SUM Aggregation Wheel Indices
    pub sum: WheelMap<F64SumAggregator>,
    /// AVG Aggregation Wheel Indices
    pub avg: WheelMap<F64AvgAggregator>,
    /// MAX Aggregation Wheel Indices
    pub max: WheelMap<F64MaxAggregator>,
    /// MIN Aggregation Wheel Indices
    pub min: WheelMap<F64MinAggregator>,
    /// ALL (SUM, AVG, MAX, MIN, COUNT) Aggregation Wheel Indices
    pub all: WheelMap<AllAggregator>,
}
impl BuiltInWheels {
    pub fn new(
        count: ReaderWheel<U32SumAggregator>,
        min_max_wheels: WheelMap<F64MinMaxAggregator>,
    ) -> Self {
        Self {
            count,
            min_max: min_max_wheels,
            sum: Default::default(),
            avg: Default::default(),
            min: Default::default(),
            max: Default::default(),
            all: Default::default(),
        }
    }
}

/// A µWheel optimizer for DataFusion that indexes wheels for time-based analytical queries.
#[allow(dead_code)]
pub struct UWheelOptimizer {
    /// Name of the table
    name: String,
    /// The column that contains the time
    time_column: String,
    /// Set of built-in aggregation wheels
    wheels: BuiltInWheels,
    /// Table provider which UWheelOptimizer builds indexes on top of
    provider: Arc<dyn TableProvider>,
    /// Default Wheel configuration that is used to build indexes
    haw_conf: HawConf,
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
    ) -> Result<Self> {
        let time_column = time_column.into();

        // Create an initial instance of the UWheelOptimizer
        let (min_timestamp_ms, max_timestamp_ms, count, min_max_wheels) =
            build(provider.clone(), &time_column, min_max_columns, &haw_conf).await?;
        let min_max_wheels = Arc::new(Mutex::new(min_max_wheels));
        let wheels = BuiltInWheels::new(count, min_max_wheels);

        Ok(Self {
            name: name.into(),
            time_column,
            wheels,
            provider,
            haw_conf,
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

    /// Returns a reference to a MIN/MAX wheel for a specified column
    pub fn min_max_wheel(&self, column: &str) -> Option<ReaderWheel<F64MinMaxAggregator>> {
        self.wheels.min_max.lock().unwrap().get(column).cloned()
    }

    /// Builds an index using the specified [IndexBuilder]
    pub async fn build_index(&self, builder: IndexBuilder) -> Result<()> {
        let batches = self.prep_index_data(&builder).await?;
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
            AggregateType::Sum => {
                let wheel = build_uwheel::<F64SumAggregator>(
                    schema,
                    &batches,
                    self.min_timestamp_ms,
                    self.max_timestamp_ms,
                    &self.time_column,
                    &builder.col.to_string(),
                    &builder.conf,
                )
                .await?;
                self.wheels.sum.lock().unwrap().insert(expr_key, wheel);
            }
            AggregateType::Avg => {
                let wheel = build_uwheel::<F64AvgAggregator>(
                    schema,
                    &batches,
                    self.min_timestamp_ms,
                    self.max_timestamp_ms,
                    &self.time_column,
                    &builder.col.to_string(),
                    &builder.conf,
                )
                .await?;
                self.wheels.avg.lock().unwrap().insert(expr_key, wheel);
            }
            AggregateType::Min => {
                let wheel = build_uwheel::<F64MinAggregator>(
                    schema,
                    &batches,
                    self.min_timestamp_ms,
                    self.max_timestamp_ms,
                    &self.time_column,
                    &builder.col.to_string(),
                    &builder.conf,
                )
                .await?;
                self.wheels.min.lock().unwrap().insert(expr_key, wheel);
            }
            AggregateType::Max => {
                let wheel = build_uwheel::<F64MaxAggregator>(
                    schema,
                    &batches,
                    self.min_timestamp_ms,
                    self.max_timestamp_ms,
                    &self.time_column,
                    &builder.col.to_string(),
                    &builder.conf,
                )
                .await?;
                self.wheels.max.lock().unwrap().insert(expr_key, wheel);
            }
            AggregateType::All => {
                let wheel = build_uwheel::<AllAggregator>(
                    schema,
                    &batches,
                    self.min_timestamp_ms,
                    self.max_timestamp_ms,
                    &self.time_column,
                    &builder.col.to_string(),
                    &builder.conf,
                )
                .await?;
                self.wheels.all.lock().unwrap().insert(expr_key, wheel);
            }
            _ => unimplemented!(),
        }
        Ok(())
    }

    // internal helper that takes a index builder and fetches the record batches used to build the index
    async fn prep_index_data(&self, builder: &IndexBuilder) -> Result<Vec<RecordBatch>> {
        let ctx = SessionContext::new();
        let df = ctx.read_table(self.provider.clone())?;

        // Apply filter if it exists
        let df = if let Some(filter) = &builder.filter {
            df.filter(filter.clone())?
        } else {
            df
        };
        df.collect().await
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

    // Attemps to rewrite a top-level Projection plan
    fn try_rewrite_projection(
        &self,
        projection: &Projection,
        plan: &LogicalPlan,
    ) -> Option<LogicalPlan> {
        match projection.input.as_ref() {
            LogicalPlan::Aggregate(agg) => {
                // SELECT AGG FROM X WHERE TIME >= X AND TIME <= Y

                // Only continue if the aggregation has a filter
                let LogicalPlan::Filter(filter) = agg.input.as_ref() else {
                    return None;
                };

                // Check no GROUP BY and only one aggregation (for now)
                if agg.group_expr.is_empty() && agg.aggr_expr.len() == 1 {
                    let agg_expr = agg.aggr_expr.first().unwrap();
                    match agg_expr {
                        // COUNT(*)
                        Expr::Alias(alias) if alias.name == COUNT_STAR_ALIAS => {
                            let range = extract_wheel_range(&filter.predicate, &self.time_column)?;
                            let schema = Arc::new(plan.schema().clone().as_arrow().clone());
                            self.count_scan(schema, range).ok()
                        }
                        // Single Aggregate Function (e.g., SUM(col))
                        Expr::AggregateFunction(agg) if agg.args.len() == 1 => {
                            if let Expr::Column(col) = &agg.args[0] {
                                // Fetch temporal filter range and expr key which is used to identify a wheel
                                let (range, expr_key) = match extract_filter_expr(
                                    &filter.predicate,
                                    &self.time_column,
                                )? {
                                    (range, Some(expr)) => {
                                        (range, maybe_replace_table_name(&expr, &self.name))
                                    }
                                    (range, None) => (range, STAR_AGGREGATION_ALIAS.to_string()),
                                };

                                // build the key for the wheel
                                let wheel_key = format!("{}.{}.{}", self.name, col.name, expr_key);

                                let agg_type = func_def_to_aggregate_type(&agg.func_def)?;
                                let schema = Arc::new(plan.schema().clone().as_arrow().clone());
                                self.create_uwheel_plan(agg_type, &wheel_key, range, schema)
                            } else {
                                None
                            }
                        }
                        _ => None,
                    }
                } else {
                    None
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
        // 1. count-based using COUNT(*) on the Count wheel
        // 2. min-max filtering using a MinMax wheel

        match extract_uwheel_expr(&filter.predicate, &self.time_column) {
            // Matches a COUNT(*) filter
            Some(UWheelExpr::WheelRange(range)) => self.maybe_count_filter(range, plan),
            // Matches a MinMax filter
            Some(UWheelExpr::MinMaxFilter(min_max)) => self.maybe_min_max_filter(min_max, plan),
            _ => None,
        }
    }

    fn maybe_count_filter(&self, range: WheelRange, plan: &LogicalPlan) -> Option<LogicalPlan> {
        let count = self.count(range)?; // early return if range can't be queried

        // query the wheel and if count == 0 then return empty table scan
        if count == 0 {
            let schema = Arc::new(plan.schema().clone().as_arrow().clone());
            let table_ref = extract_table_reference(plan)?;
            empty_table_scan(table_ref, schema).ok()
        } else {
            None
        }
    }

    // Takes a MinMax filter and possibly returns an Empty an empty TableScan if the filter indicates that the result is empty
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
        agg_type: AggregateType,
        wheel_key: &str,
        range: WheelRange,
        schema: SchemaRef,
    ) -> Option<LogicalPlan> {
        match agg_type {
            AggregateType::Sum => {
                let wheel = self.wheels.sum.lock().unwrap().get(wheel_key)?.clone();
                let result = wheel.combine_range_and_lower(range).unwrap_or(0.0);
                uwheel_agg_to_table_scan(result, schema).ok()
            }
            AggregateType::Avg => {
                let wheel = self.wheels.avg.lock().unwrap().get(wheel_key)?.clone();
                let result = wheel
                    .combine_range_and_lower(range)
                    .map(F64AvgAggregator::lower) // # Replace when fixed: https://github.com/uwheel/uwheel/issues/140
                    .unwrap_or(0.0);
                uwheel_agg_to_table_scan(result, schema).ok()
            }
            AggregateType::Min => {
                let wheel = self.wheels.min.lock().unwrap().get(wheel_key)?.clone();
                let result = wheel.combine_range_and_lower(range).unwrap_or(0.0);
                uwheel_agg_to_table_scan(result, schema).ok()
            }
            AggregateType::Max => {
                let wheel = self.wheels.max.lock().unwrap().get(wheel_key)?.clone();
                let result = wheel.combine_range_and_lower(range).unwrap_or(0.0);
                uwheel_agg_to_table_scan(result, schema).ok()
            }
            _ => unimplemented!(),
        }
    }

    fn count_scan(&self, schema: SchemaRef, range: WheelRange) -> Result<LogicalPlan> {
        let count = self.count(range).unwrap_or(0);
        let name = COUNT_STAR_ALIAS.to_string();
        let data = Int64Array::from(vec![count as i64]);
        let record_batch = RecordBatch::try_from_iter(vec![(&name, Arc::new(data) as ArrayRef)])?;
        let df_schema = Arc::new(DFSchema::try_from(schema.clone())?);
        let mem_table = MemTable::try_new(schema, vec![vec![record_batch]])?;

        mem_table_as_table_scan(mem_table, df_schema)
    }
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

fn func_def_to_aggregate_type(func_def: &AggregateFunctionDefinition) -> Option<AggregateType> {
    match func_def {
        AggregateFunctionDefinition::BuiltIn(datafusion::logical_expr::AggregateFunction::Max) => {
            Some(AggregateType::Max)
        }
        AggregateFunctionDefinition::BuiltIn(datafusion::logical_expr::AggregateFunction::Min) => {
            Some(AggregateType::Min)
        }
        AggregateFunctionDefinition::UDF(udf) if udf.name() == "sum" => Some(AggregateType::Sum),
        AggregateFunctionDefinition::UDF(udf) if udf.name() == "count" => {
            Some(AggregateType::Count)
        }
        _ => None,
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

// Helper methods to build the UWheelOptimizer

// Uses the provided TableProvider to build the UWheelOptimizer
async fn build(
    provider: Arc<dyn TableProvider>,
    time_column: &str,
    min_max_columns: Vec<String>,
    haw_conf: &HawConf,
) -> Result<(
    u64,
    u64,
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

async fn build_uwheel<A>(
    schema: SchemaRef,
    batches: &[RecordBatch],
    _min_timestamp_ms: u64,
    max_timestamp_ms: u64,
    time_col: &str,
    sum_col: &str,
    haw_conf: &HawConf,
) -> Result<ReaderWheel<A>>
where
    A: Aggregator<Input = f64>,
{
    // TODO: remove hardcoded time
    let start = NaiveDate::from_ymd_opt(2022, 1, 1).unwrap();
    let date = Utc.from_utc_datetime(&start.and_hms_opt(0, 0, 0).unwrap());
    let start_ms = date.timestamp_millis() as u64;

    let conf = haw_conf.with_watermark(start_ms);

    let mut wheel: RwWheel<A> = RwWheel::with_conf(
        Conf::default()
            .with_haw_conf(conf)
            .with_write_ahead(64000usize.next_power_of_two()),
    );

    let column_index = schema.index_of(sum_col)?;
    let column_field = schema.field(column_index);

    if is_numeric_type(column_field.data_type()) {
        for batch in batches {
            let time_array = batch
                .column_by_name(time_col)
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();

            let sum_column_array = batch
                .column_by_name(sum_col)
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();

            for (timestamp, value) in time_array
                .values()
                .iter()
                .copied()
                .zip(sum_column_array.values().iter().copied())
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

        // convert wheel to index
        wheel.read().to_simd_wheels();
        // TODO: make this configurable
        wheel.read().to_prefix_wheels();
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
