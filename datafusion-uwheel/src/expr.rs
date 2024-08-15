use chrono::{DateTime, Utc};
use datafusion::{
    common::Column,
    logical_expr::{Between, BinaryExpr, Operator},
    prelude::Expr,
    scalar::ScalarValue,
};
use uwheel::WheelRange;

#[derive(Debug, Clone, PartialEq)]
pub enum UWheelExpr {
    MinMaxFilter(MinMaxFilter),
    TemporalFilter(Option<i64>, Option<i64>),
    WheelRange(WheelRange),
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

// Tries to extract a temporal filter from a Datafusion expr that matches the `time_column`
pub fn extract_wheel_range(predicate: &Expr, time_column: &str) -> Option<WheelRange> {
    match extract_uwheel_expr(predicate, time_column) {
        Some(UWheelExpr::WheelRange(range)) => Some(range),
        _ => None,
    }
}

// Converts a TemporalFilter expr to WheelRange
fn filter_to_range(start: Option<i64>, end: Option<i64>) -> Option<WheelRange> {
    match (start, end) {
        (Some(start), Some(end)) => {
            if start > end {
                None
            } else {
                WheelRange::new(start as u64, end as u64).ok()
            }
        }
        _ => None,
    }
}

/// Attempts to extract a UWheelExpr from a Datafusion expr
pub fn extract_uwheel_expr(predicate: &Expr, time_column: &str) -> Option<UWheelExpr> {
    match predicate {
        Expr::BinaryExpr(binary_expr) => handle_binary_expr(binary_expr, time_column),
        Expr::Between(between) => handle_between(between, time_column),
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

// TODO: handle BETWEEN more efficiently, currently rewrites to BinaryExpr
fn handle_between(between: &Between, time_column: &str) -> Option<UWheelExpr> {
    let Between {
        expr,
        negated,
        low,
        high,
    } = between;

    if *negated {
        None
    } else {
        let lower_bound = BinaryExpr::new(expr.clone(), Operator::GtEq, low.clone());
        let upper_bound = BinaryExpr::new(expr.clone(), Operator::LtEq, high.clone());

        // Combine the two comparisons with AND
        let between_expr = BinaryExpr::new(
            Box::new(Expr::BinaryExpr(lower_bound)),
            Operator::And,
            Box::new(Expr::BinaryExpr(upper_bound)),
        );
        handle_binary_expr(&between_expr, time_column)
    }
}

/// Attemps to extract a MinMaxPredicate from a Datafusion expr
pub fn extract_min_max_predicate(predicate: &Expr) -> Option<MinMaxPredicate> {
    let process_scalar = |scalar: &ScalarValue, col: &Column, op: &Operator| {
        if op == &Operator::Gt
            || op == &Operator::GtEq
            || op == &Operator::Lt
            || op == &Operator::LtEq
        {
            Some(MinMaxPredicate {
                name: col.name.clone(),
                op: *op,
                scalar: scalar.clone(),
            })
        } else {
            None
        }
    };
    match predicate {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            if let Expr::Column(col) = left.as_ref() {
                match right.as_ref() {
                    Expr::Cast(cast) => {
                        if let Expr::Literal(scalar) = cast.expr.as_ref() {
                            process_scalar(scalar, col, op)
                        } else {
                            None
                        }
                    }
                    Expr::Literal(scalar) => process_scalar(scalar, col, op),
                    _ => None,
                }
            } else {
                None
            }
        }
        _ => None,
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
            filter_to_range(start, end).map(UWheelExpr::WheelRange)
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
    let handle_col =
        |col: &Column| {
            if col.name == time_column {
                match op {
                    Operator::GtEq | Operator::Gt => extract_timestamp_ms(right)
                        .map(|ts| UWheelExpr::TemporalFilter(Some(ts), None)),
                    Operator::Lt | Operator::LtEq => extract_timestamp_ms(right)
                        .map(|ts| UWheelExpr::TemporalFilter(None, Some(ts))),
                    _ => None,
                }
            } else {
                None
            }
        };

    match left {
        Expr::Cast(cast) => {
            if let Expr::Column(col) = cast.expr.as_ref() {
                handle_col(col)
            } else {
                None
            }
        }
        Expr::Column(col) => handle_col(col),
        _ => None,
    }
}

/// Attempts to extract a timestamp from a Datafusion expr that is represented as unix timestamp in milliseconds
fn extract_timestamp_ms(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(date_str))) => DateTime::parse_from_rfc3339(date_str)
            .ok()
            .map(|dt| dt.with_timezone(&Utc).timestamp_millis()),
        Expr::Literal(ScalarValue::TimestampMillisecond(Some(ms), _)) => Some(*ms),
        Expr::Literal(ScalarValue::TimestampMicrosecond(Some(us), _)) => Some(*us / 1000_i64),
        Expr::Literal(ScalarValue::TimestampNanosecond(Some(ns), _)) => {
            Some(ns / 1_000_000) // Convert nanoseconds to milliseconds
        }
        Expr::Cast(cast) => extract_timestamp_ms(&cast.expr),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use datafusion::{
        arrow::datatypes::DataType,
        logical_expr::Cast,
        prelude::{col, lit},
    };

    use super::*;

    #[test]
    fn extract_timestamp_utf8_() {
        let expr = Expr::Literal(ScalarValue::Utf8(Some("2023-01-01T00:00:00Z".to_string())));
        assert_eq!(extract_timestamp_ms(&expr), Some(1672531200000));
    }

    #[test]
    fn extract_timestamp_millisecond() {
        let expr = Expr::Literal(ScalarValue::TimestampMillisecond(Some(1672531200000), None));
        assert_eq!(extract_timestamp_ms(&expr), Some(1672531200000));
    }

    #[test]
    fn extract_timestamp_microsecond() {
        let expr = Expr::Literal(ScalarValue::TimestampMicrosecond(
            Some(1672531200000000),
            None,
        ));
        assert_eq!(extract_timestamp_ms(&expr), Some(1672531200000));
    }

    #[test]
    fn extract_timestamp_nanosecond() {
        let expr = Expr::Literal(ScalarValue::TimestampNanosecond(
            Some(1672531200000000000),
            None,
        ));
        assert_eq!(extract_timestamp_ms(&expr), Some(1672531200000));
    }

    #[test]
    fn extract_timestamp_cast() {
        let expr = Expr::Cast(Cast::new(
            Box::new(Expr::Literal(ScalarValue::Utf8(Some(
                "2023-01-01T00:00:00Z".to_string(),
            )))),
            DataType::Utf8,
        ));
        assert_eq!(extract_timestamp_ms(&expr), Some(1672531200000));
    }

    fn create_timestamp(date_string: &str) -> i64 {
        DateTime::parse_from_rfc3339(date_string)
            .unwrap()
            .with_timezone(&Utc)
            .timestamp_millis()
    }

    #[test]
    fn single_greater_than_or_equal() {
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
    fn range_with_and() {
        let expr = col("timestamp")
            .gt_eq(lit("2023-01-01T00:00:00Z"))
            .and(col("timestamp").lt(lit("2023-12-31T23:59:59Z")));
        let result = extract_uwheel_expr(&expr, "timestamp");
        assert_eq!(
            result,
            Some(UWheelExpr::WheelRange(WheelRange::new_unchecked(
                create_timestamp("2023-01-01T00:00:00Z") as u64,
                create_timestamp("2023-12-31T23:59:59Z") as u64
            ))),
        );
    }
    #[test]
    fn with_non_matching_column() {
        let expr = col("other_column").gt_eq(lit("2023-01-01T00:00:00Z"));
        let result = extract_uwheel_expr(&expr, "timestamp");
        assert_eq!(result, None);
    }

    #[test]
    fn with_non_temporal_expression() {
        let expr = col("timestamp").eq(lit(42));
        let result = extract_uwheel_expr(&expr, "timestamp");
        assert_eq!(result, None);
    }

    #[test]
    fn range_with_less_than_or_equal() {
        let expr = col("timestamp")
            .gt_eq(lit("2023-01-01T00:00:00Z"))
            .and(col("timestamp").lt_eq(lit("2023-12-31T23:59:59Z")));

        let result = extract_uwheel_expr(&expr, "timestamp");
        assert_eq!(
            result,
            Some(UWheelExpr::WheelRange(WheelRange::new_unchecked(
                create_timestamp("2023-01-01T00:00:00Z") as u64,
                create_timestamp("2023-12-31T23:59:59Z") as u64
            ))),
        );
    }

    #[test]
    fn between() {
        let expr =
            col("timestamp").between(lit("2023-01-01T00:00:00Z"), lit("2023-12-31T23:59:59Z"));
        let result = extract_uwheel_expr(&expr, "timestamp");
        assert_eq!(
            result,
            Some(UWheelExpr::WheelRange(WheelRange::new_unchecked(
                create_timestamp("2023-01-01T00:00:00Z") as u64,
                create_timestamp("2023-12-31T23:59:59Z") as u64
            ))),
        );
    }

    #[test]
    fn min_max_filter() {
        let expr = col("timestamp")
            .gt_eq(lit("2023-01-01T00:00:00Z"))
            .and(col("timestamp").lt(lit("2023-12-31T23:59:59Z")))
            .and(col("value").gt(lit(1000)));
        let result = extract_uwheel_expr(&expr, "timestamp");
        assert_eq!(
            result,
            Some(UWheelExpr::MinMaxFilter(MinMaxFilter {
                range: WheelRange::new_unchecked(
                    create_timestamp("2023-01-01T00:00:00Z") as u64,
                    create_timestamp("2023-12-31T23:59:59Z") as u64
                ),
                predicate: MinMaxPredicate {
                    name: "value".to_string(),
                    op: Operator::Gt,
                    scalar: ScalarValue::Int32(Some(1000))
                }
            }))
        );
    }

    #[test]
    fn min_max_filter_with_non_matching_column() {
        let expr = col("timestamp")
            .gt_eq(lit("2023-01-01T00:00:00Z"))
            .and(col("timestamp").lt(lit("2023-12-31T23:59:59Z")))
            .and(col("other_column").gt(lit(1000)));
        let result = extract_uwheel_expr(&expr, "other_timestamp");
        assert_eq!(result, None);
    }

    #[test]
    fn min_max_filter_with_between() {
        let expr = col("timestamp")
            .between(lit("2023-01-01T00:00:00Z"), lit("2023-12-31T23:59:59Z"))
            .and(col("value").gt(lit(1000)));
        let result = extract_uwheel_expr(&expr, "timestamp");
        assert_eq!(
            result,
            Some(UWheelExpr::MinMaxFilter(MinMaxFilter {
                range: WheelRange::new_unchecked(
                    create_timestamp("2023-01-01T00:00:00Z") as u64,
                    create_timestamp("2023-12-31T23:59:59Z") as u64
                ),
                predicate: MinMaxPredicate {
                    name: "value".to_string(),
                    op: Operator::Gt,
                    scalar: ScalarValue::Int32(Some(1000))
                }
            }))
        );
    }
}
