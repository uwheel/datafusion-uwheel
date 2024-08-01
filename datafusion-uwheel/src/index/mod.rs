use datafusion::{common::Column, prelude::Expr};
use uwheel::HawConf;

pub enum AggregateType {
    Sum,
    Avg,
    Min,
    Max,
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
}

impl IndexBuilder {
    /// Creates a new IndexBuilder with the given column and aggregation type
    pub fn with_col_and_aggregate(col: impl Into<Column>, agg_type: AggregateType) -> Self {
        Self {
            col: col.into(),
            agg_type,
            filter: None,
            conf: Default::default(),
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
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::{col, lit};

    use super::*;

    #[test]
    fn test_index_builder() {
        let builder = IndexBuilder::with_col_and_aggregate("col", AggregateType::Sum);
        assert_eq!(builder.col.to_string(), "col");
    }

    #[test]
    fn test_index_builder_with_filter() {
        let builder = IndexBuilder::with_col_and_aggregate("fare_amount", AggregateType::Sum)
            .with_filter(col("id").eq(lit(1)));
        assert_eq!(builder.filter.unwrap().to_string(), "id = Int32(1)");
    }
}
