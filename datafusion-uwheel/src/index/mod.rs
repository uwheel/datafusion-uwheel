use datafusion::{common::Column, error::DataFusionError, prelude::Expr, scalar::ScalarValue};
use uwheel::{wheels::read::aggregation::conf::WheelMode, HawConf};

use crate::scalar_to_timestamp;

// Todo: change to UWheelAggregate
#[derive(Debug, Clone)]
pub enum AggregateType {
    Sum,
    Avg,
    Min,
    Max,
    Count,
    All,
}

/// Builder for creating a wheel indices
pub struct IndexBuilder {
    /// The column to build the wheel index on
    pub col: Column,
    /// The type of aggregation to use
    pub agg_type: AggregateType,
    /// Optional filter to apply to the column
    pub filter: Option<Expr>,
    /// Wheel configuration
    pub conf: HawConf,
    /// Optional time range to apply to the the index building
    pub time_range: Option<(ScalarValue, ScalarValue)>,
}

impl IndexBuilder {
    /// Creates a new IndexBuilder with the given column and aggregation type
    pub fn with_col_and_aggregate(col: impl Into<Column>, agg_type: AggregateType) -> Self {
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
    /// Input must be a ScalarValue of type Date32, Date64 or Timestamp
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
    pub fn with_filter(mut self, filter: Expr) -> Self {
        self.filter = Some(filter);
        self
    }
    /// Sets the configuration for the underlying wheel index
    pub fn with_conf(mut self, conf: HawConf) -> Self {
        self.conf = conf;
        self
    }
    // helper method to create a default Haw configuration
    fn default_haw_conf() -> HawConf {
        // configure Index mode
        let mut conf = HawConf::default().with_mode(WheelMode::Index);
        // set the retention policy to keep all data

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
        let builder = IndexBuilder::with_col_and_aggregate("col", AggregateType::Sum);
        assert_eq!(builder.col.to_string(), "col");
    }

    #[test]
    fn index_builder_with_filter() {
        let builder = IndexBuilder::with_col_and_aggregate("fare_amount", AggregateType::Sum)
            .with_filter(col("id").eq(lit(1)));
        assert_eq!(builder.filter.unwrap().to_string(), "id = Int32(1)");
    }

    #[test]
    fn index_builder_with_time_range() {
        assert!(
            IndexBuilder::with_col_and_aggregate("fare_amount", AggregateType::Sum)
                .with_time_range(
                    ScalarValue::Utf8(Some("2022-01-01T00:00:00Z".to_string())),
                    ScalarValue::Utf8(Some("2022-02-01T00:00:00Z".to_string())),
                )
                .is_ok()
        );

        assert!(
            IndexBuilder::with_col_and_aggregate("fare_amount", AggregateType::Sum)
                .with_time_range(
                    ScalarValue::Int32(Some(10000)),
                    ScalarValue::Int32(Some(10000))
                )
                .is_err()
        );
    }
}
