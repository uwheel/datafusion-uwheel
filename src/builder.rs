use crate::UWheelOptimizer;
use datafusion::{datasource::TableProvider, error::Result};
use std::sync::Arc;
use uwheel::HawConf;

/// Builder for creating a UWheelOptimizer
#[allow(dead_code)]
pub struct Builder {
    /// Name of the table
    name: String,
    /// The column in the parquet files that contains the time
    time_column: String,
    /// Columns to build min/max wheels for
    min_max_columns: Vec<String>,
    /// Default Haw configuration to use when building wheels
    wheel_conf: HawConf,
}

impl Builder {
    /// Create a new UWheelOptimizer builder
    pub fn new(time_column: impl Into<String>) -> Self {
        Self {
            name: "".to_string(),
            time_column: time_column.into(),
            min_max_columns: Default::default(),
            wheel_conf: HawConf::default(),
        }
    }

    /// Set the name of the table
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Columns to build min/max wheels for
    ///
    /// Columns must be of numeric data types
    pub fn with_min_max_wheels(mut self, columns: Vec<&str>) -> Self {
        self.min_max_columns = columns.iter().map(|s| s.to_string()).collect();
        self
    }
    /// Builds the UWheelOptimizer using the provided TableProvider
    pub async fn build_with_provider(
        self,
        provider: Arc<dyn TableProvider>,
    ) -> Result<UWheelOptimizer> {
        UWheelOptimizer::try_new(self.name, self.time_column, self.min_max_columns, provider).await
    }
}
