use chrono::{DateTime, Utc};
use datafusion::{
    logical_expr::{BinaryExpr, Operator},
    prelude::Expr,
    scalar::ScalarValue,
};
use uwheel::WheelRange;

#[derive(Debug, Clone, PartialEq)]
pub enum UWheelExpr {
    MinMaxFilter(MinMaxFilter),
    TemporalFilter(Option<i64>, Option<i64>),
}

/// MinMaxFilter Expr
#[derive(Debug, Clone, PartialEq)]
pub struct MinMaxFilter {
    /// The temporal range that is to be checked
    pub range: WheelRange,
    /// The predicate (e.g., x > 1000)
    pub predicate: MinMaxPredicate,
}

/// Predicate for temporal min/max pruning
#[derive(Debug, Clone, PartialEq)]
pub struct MinMaxPredicate {
    /// Name of the column
    pub name: String,
    /// Comparison operator (e.g., >, <, >=, <=)
    pub op: Operator,
    /// Scalar value (e.g., 1000.0)
    pub scalar: ScalarValue,
}

/// Attempts to extract a UWheelExpr from a Datafusion expr
pub fn extract_uwheel_expr(predicate: &Expr, time_column: &str) -> Option<UWheelExpr> {
    match predicate {
        Expr::BinaryExpr(binary_expr) => handle_binary_expr(binary_expr, time_column),
        Expr::Between(_between) => unimplemented!(), //handle_between(between, time_column),
        _ => None,
    }
}

// Tries to extract a temporal filter from a Datafusion expr that matches the `time_column`
pub fn extract_wheel_range(predicate: &Expr, time_column: &str) -> Option<WheelRange> {
    extract_uwheel_expr(predicate, time_column).and_then(|expr| {
        if let UWheelExpr::TemporalFilter(Some(start), Some(end)) = expr {
            if start > end {
                None
            } else {
                WheelRange::new(start as u64, end as u64).ok()
            }
        } else {
            None
        }
    })
}

/// Attemps to extract a MinMaxPredicate from a Datafusion expr
pub fn extract_min_max_predicate(predicate: &Expr) -> Option<MinMaxPredicate> {
    match predicate {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            if let Expr::Column(col) = left.as_ref() {
                // check if the right side is a literal
                if let Expr::Literal(scalar) = right.as_ref() {
                    // check if the operator is a comparison operator
                    if op == &Operator::Gt
                        || op == &Operator::GtEq
                        || op == &Operator::Lt
                        || op == &Operator::LtEq
                    {
                        return Some(MinMaxPredicate {
                            name: col.name.clone(),
                            op: *op,
                            scalar: scalar.clone(),
                        });
                    }
                }
            }
            None
        }
        _ => None,
    }
}

fn handle_binary_expr(binary_expr: &BinaryExpr, time_column: &str) -> Option<UWheelExpr> {
    let BinaryExpr { left, op, right } = binary_expr;

    // Check if the expression can be checked by a MinMaxFilter
    if let Some(min_max_filter) = extract_min_max_filter(left, op, right, time_column) {
        return Some(UWheelExpr::MinMaxFilter(min_max_filter));
    }

    match op {
        Operator::And => handle_and_operator(left, right, time_column),
        _ => handle_comparison_operator(left, op, right, time_column),
    }
}

pub fn extract_filter_expr(
    predicate: &Expr,
    time_column: &str,
) -> Option<(WheelRange, Option<Expr>)> {
    match predicate {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            if op == &Operator::And {
                // TODO: refactor
                if let Some(range) = extract_wheel_range(left, time_column) {
                    return Some((range, Some(*right.clone())));
                }
                if let Some(range) = extract_wheel_range(right, time_column) {
                    return Some((range, Some(*left.clone())));
                }
                // try to extract only a temporal WheelRange filter
                return extract_wheel_range(predicate, time_column).map(|range| (range, None));
            }
            None
        }
        _ => None,
    }
}

fn extract_min_max_filter(
    left: &Expr,
    op: &Operator,
    right: &Expr,
    time_column: &str,
) -> Option<MinMaxFilter> {
    // Helper closure to extract a MinMaxFilter from a left and right expression
    let min_max_filter = |left: &Expr, right: &Expr| {
        let range = extract_wheel_range(left, time_column)?;
        let min_max = extract_min_max_predicate(right)?;
        Some(MinMaxFilter {
            range,
            predicate: min_max,
        })
    };

    // Check if the expression can be checked by a MinMaxFilter
    // TODO: handle BETWEEN?
    if op == &Operator::And {
        if let Some(min_max) = min_max_filter(left, right) {
            return Some(min_max);
        }
        if let Some(min_max) = min_max_filter(right, left) {
            return Some(min_max);
        }
    }
    None
}

fn handle_and_operator(left: &Expr, right: &Expr, time_column: &str) -> Option<UWheelExpr> {
    let left_range = extract_uwheel_expr(left, time_column);
    let right_range = extract_uwheel_expr(right, time_column);

    match (left_range, right_range) {
        (Some(UWheelExpr::TemporalFilter(start, _)), Some(UWheelExpr::TemporalFilter(_, end))) => {
            Some(UWheelExpr::TemporalFilter(start, end))
        }
        _ => None,
    }
}

fn handle_comparison_operator(
    left: &Expr,
    op: &Operator,
    right: &Expr,
    time_column: &str,
) -> Option<UWheelExpr> {
    if let Expr::Column(col) = left {
        if col.name == time_column {
            match op {
                Operator::GtEq | Operator::Gt => {
                    extract_timestamp(right).map(|ts| UWheelExpr::TemporalFilter(Some(ts), None))
                }
                Operator::Lt | Operator::LtEq => {
                    extract_timestamp(right).map(|ts| UWheelExpr::TemporalFilter(None, Some(ts)))
                }
                _ => None,
            }
        } else {
            None
        }
    } else {
        None
    }
}

fn extract_timestamp(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(date_str))) => DateTime::parse_from_rfc3339(date_str)
            .ok()
            .map(|dt| dt.with_timezone(&Utc).timestamp_millis()),
        Expr::Literal(ScalarValue::TimestampMillisecond(Some(ms), _)) => Some(*ms),
        Expr::Literal(ScalarValue::TimestampMicrosecond(Some(us), _)) => Some(*us / 1000_i64),
        Expr::Literal(ScalarValue::TimestampNanosecond(Some(ns), _)) => {
            Some(ns / 1_000_000) // Convert nanoseconds to milliseconds
        }
        // Add other cases as needed, e.g., for different date/time formats
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::{col, lit};

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
        let result = extract_uwheel_expr(&expr, "timestamp");
        assert_eq!(
            result,
            Some(UWheelExpr::TemporalFilter(
                Some(create_timestamp("2023-01-01T00:00:00Z")),
                None
            ))
        );
    }

    #[test]
    fn test_range_with_and() {
        let expr = col("timestamp")
            .gt_eq(lit("2023-01-01T00:00:00Z"))
            .and(col("timestamp").lt(lit("2023-12-31T23:59:59Z")));
        let result = extract_uwheel_expr(&expr, "timestamp");
        assert_eq!(
            result,
            Some(UWheelExpr::TemporalFilter(
                Some(create_timestamp("2023-01-01T00:00:00Z")),
                Some(create_timestamp("2023-12-31T23:59:59Z"))
            ))
        );
    }
    #[test]
    fn test_with_non_matching_column() {
        let expr = col("other_column").gt_eq(lit("2023-01-01T00:00:00Z"));
        let result = extract_uwheel_expr(&expr, "timestamp");
        assert_eq!(result, None);
    }

    #[test]
    fn test_with_non_temporal_expression() {
        let expr = col("timestamp").eq(lit(42));
        let result = extract_uwheel_expr(&expr, "timestamp");
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
