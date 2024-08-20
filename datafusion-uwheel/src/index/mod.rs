use datafusion::{common::Column, error::DataFusionError, prelude::Expr, scalar::ScalarValue};
use uwheel::{wheels::read::aggregation::conf::WheelMode, HawConf};

use crate::scalar_to_timestamp;

/// Aggregate types that the UWheelOptimizer supports
#[derive(Debug, Copy, Clone)]
pub enum UWheelAggregate {
    /// Computes the sum of the values in the column.
    Sum,
    /// Computes the average of the values in the column.
    Avg,
    /// Finds the minimum value in the column.
    Min,
    /// Finds the maximum value in the column.
    Max,
    /// Counts the number of values in the column.
    Count,
    /// Computes SUM, AVG, MIN, MAX, and COUNT aggregates.
    All,
}

/// Builder for creating wheel indices
///
/// This struct provides a fluent interface for configuring and building wheel indices.
///
/// # Examples
///
/// ```
/// use datafusion_uwheel::index::{IndexBuilder, UWheelAggregate};
/// use datafusion::logical_expr::{col, lit};
/// use datafusion::common::ScalarValue;
///
/// let builder = IndexBuilder::with_col_and_aggregate("temperature", UWheelAggregate::Max)
///     .with_filter(col("location").eq(lit("New York")))
///     .with_time_range(
///         ScalarValue::Date32(Some(1000)),
///         ScalarValue::Date32(Some(2000))
///     )
///     .unwrap();
/// ```
pub struct IndexBuilder {
    /// The column to build the wheel index on
    pub col: Column,
    /// The type of aggregation to use
    pub agg_type: UWheelAggregate,
    /// Optional filter to apply to the column
    pub filter: Option<Expr>,
    /// Wheel configuration
    pub conf: HawConf,
    /// Optional time range to apply to the index building
    pub time_range: Option<(ScalarValue, ScalarValue)>,
}

impl IndexBuilder {
    /// Creates a new IndexBuilder with the given column and aggregation type
    ///
    /// # Arguments
    ///
    /// * `col` - The column to build the index on
    /// * `agg_type` - The type of aggregation to use
    ///
    /// # Examples
    ///
    /// ```
    /// use datafusion_uwheel::index::{IndexBuilder, UWheelAggregate};
    ///
    /// let builder = IndexBuilder::with_col_and_aggregate("temperature", UWheelAggregate::Max);
    /// ```
    pub fn with_col_and_aggregate(col: impl Into<Column>, agg_type: UWheelAggregate) -> Self {
        Self {
            col: col.into(),
            agg_type,
            filter: None,
            conf: Self::default_haw_conf(),
            time_range: None,
        }
    }

    /// Applies a time range when building the index
    ///
    /// # Arguments
    ///
    /// * `start` - The start of the time range
    /// * `end` - The end of the time range
    ///
    /// # Returns
    ///
    /// * `Result<Self, DataFusionError>` - The builder with the time range applied, or an error if the input is invalid
    ///
    /// # Examples
    ///
    /// ```
    /// use datafusion_uwheel::index::{IndexBuilder, UWheelAggregate};
    /// use datafusion::common::ScalarValue;
    ///
    /// let builder = IndexBuilder::with_col_and_aggregate("temperature", UWheelAggregate::Max)
    ///     .with_time_range(
    ///         ScalarValue::Date32(Some(1000)),
    ///         ScalarValue::Date32(Some(2000))
    ///     )
    ///     .unwrap();
    /// ```
    pub fn with_time_range(
        mut self,
        start: ScalarValue,
        end: ScalarValue,
    ) -> Result<Self, DataFusionError> {
        match (scalar_to_timestamp(&start), scalar_to_timestamp(&end)) {
            (Some(_), Some(_)) => {
                self.time_range = Some((start, end));
                Ok(self)
            }
            _ => Err(DataFusionError::Internal(
                "Not valid time range data types, must be Date32, Date64, or Timestamp".to_string(),
            )),
        }
    }

    /// Applies a filter when building the index
    ///
    /// # Arguments
    ///
    /// * `filter` - The filter expression to apply
    ///
    /// # Examples
    ///
    /// ```
    /// use datafusion_uwheel::index::{IndexBuilder, UWheelAggregate};
    /// use datafusion::logical_expr::{col, lit};
    ///
    /// let builder = IndexBuilder::with_col_and_aggregate("temperature", UWheelAggregate::Max)
    ///     .with_filter(col("location").eq(lit("New York")));
    /// ```
    pub fn with_filter(mut self, filter: Expr) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Sets the configuration for the underlying wheel index
    ///
    /// # Arguments
    ///
    /// * `conf` - The HAW configuration to use
    ///
    /// # Examples
    ///
    /// ```
    /// use datafusion_uwheel::index::{IndexBuilder, UWheelAggregate};
    /// use uwheel::HawConf;
    ///
    /// let conf = HawConf::default();
    /// let builder = IndexBuilder::with_col_and_aggregate("temperature", UWheelAggregate::Max)
    ///     .with_conf(conf);
    /// ```
    pub fn with_conf(mut self, conf: HawConf) -> Self {
        self.conf = conf;
        self
    }

    /// Creates a default HAW configuration
    ///
    /// This configuration sets the wheel mode to Index and sets all retention policies to Keep.
    ///
    /// # Returns
    ///
    /// * `HawConf` - The default HAW configuration
    fn default_haw_conf() -> HawConf {
        let mut conf = HawConf::default().with_mode(WheelMode::Index);
        conf.seconds
            .set_retention_policy(uwheel::RetentionPolicy::Keep);
        conf.minutes
            .set_retention_policy(uwheel::RetentionPolicy::Keep);
        conf.hours
            .set_retention_policy(uwheel::RetentionPolicy::Keep);
        conf.days
            .set_retention_policy(uwheel::RetentionPolicy::Keep);
        conf.weeks
            .set_retention_policy(uwheel::RetentionPolicy::Keep);
        conf
    }
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::{col, lit};

    use super::*;

    #[test]
    fn index_builder_simple() {
        let builder = IndexBuilder::with_col_and_aggregate("col", UWheelAggregate::Sum);
        assert_eq!(builder.col.to_string(), "col");
    }

    #[test]
    fn index_builder_with_filter() {
        let builder = IndexBuilder::with_col_and_aggregate("fare_amount", UWheelAggregate::Sum)
            .with_filter(col("id").eq(lit(1)));
        assert_eq!(builder.filter.unwrap().to_string(), "id = Int32(1)");
    }

    #[test]
    fn index_builder_with_time_range() {
        assert!(
            IndexBuilder::with_col_and_aggregate("fare_amount", UWheelAggregate::Sum)
                .with_time_range(
                    ScalarValue::Utf8(Some("2022-01-01T00:00:00Z".to_string())),
                    ScalarValue::Utf8(Some("2022-02-01T00:00:00Z".to_string())),
                )
                .is_ok()
        );

        assert!(
            IndexBuilder::with_col_and_aggregate("fare_amount", UWheelAggregate::Sum)
                .with_time_range(
                    ScalarValue::Int32(Some(10000)),
                    ScalarValue::Int32(Some(10000))
                )
                .is_err()
        );
    }
}
