use crate::{scalar_to_timestamp, UWheelOptimizer};
use datafusion::{
    datasource::TableProvider,
    error::{DataFusionError, Result},
    scalar::ScalarValue,
};
use std::sync::Arc;
use uwheel::{wheels::read::aggregation::conf::WheelMode, HawConf};

/// Builder for creating a UWheelOptimizer
#[allow(dead_code)]
pub struct Builder {
    /// Name of the table
    name: String,
    /// The column which defines the time
    time_column: String,
    /// Columns to build min/max wheels for
    min_max_columns: Vec<String>,
    /// Default Haw configuration to use when building wheels
    wheel_conf: HawConf,
    /// Optional time range to apply to the default indices  
    time_range: Option<(ScalarValue, ScalarValue)>,
}

impl Builder {
    /// Create a new UWheelOptimizer builder
    pub fn new(time_column: impl Into<String>) -> Self {
        Self {
            name: "".to_string(),
            time_column: time_column.into(),
            min_max_columns: Default::default(),
            wheel_conf: Self::default_haw_conf(),
            time_range: None,
        }
    }
    // helper method to create a default Haw configuration
    fn default_haw_conf() -> HawConf {
        // configure Index mode
        let mut conf = HawConf::default().with_mode(WheelMode::Index);
        // set the retention policy to keep all data on all time dimensions

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

    /// Set the name of the table
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Set the Haw configuration to use when building wheels
    pub fn with_haw_conf(mut self, conf: HawConf) -> Self {
        self.wheel_conf = conf;
        self
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
        UWheelOptimizer::try_new(
            self.name,
            self.time_column,
            self.min_max_columns,
            provider,
            self.wheel_conf,
            self.time_range,
        )
        .await
    }
}
