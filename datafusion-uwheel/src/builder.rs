use crate::{scalar_to_timestamp, UWheelOptimizer};
use datafusion::{
    datasource::TableProvider,
    error::{DataFusionError, Result},
    scalar::ScalarValue,
};
use std::sync::Arc;
use uwheel::{wheels::read::aggregation::conf::WheelMode, HawConf};

/// Builder for creating a UWheelOptimizer
///
/// This struct provides an interface for configuring and creating a UWheelOptimizer.
///
/// # Examples
///
/// ```
/// use datafusion_uwheel::builder::Builder;
/// use datafusion::prelude::*;
/// use datafusion::common::ScalarValue;
/// use datafusion::common::arrow::datatypes::{Schema, DataType, Field};
/// use datafusion::common::arrow::array::{RecordBatch, Date32Array, Float32Array};
/// use datafusion::datasource::MemTable;
/// use std::sync::Arc;
///
/// async fn create_mem_table() -> MemTable {
///     let schema = Arc::new(Schema::new(vec![
///         Field::new("timestamp", DataType::Date32, false),
///         Field::new("temperature", DataType::Float32, false),
///         Field::new("humidity", DataType::Float32, false),
///     ]));
///     
///     let data = RecordBatch::try_new(
///         schema.clone(),
///         vec![
///             Arc::new(Date32Array::from(vec![19000, 19001, 19002])),
///             Arc::new(Float32Array::from(vec![20.5, 21.0, 22.3])),
///             Arc::new(Float32Array::from(vec![0.5, 0.6, 0.7])),
///         ],
///     ).unwrap();
///     
///     MemTable::try_new(schema, vec![vec![data]]).unwrap()
/// }
///
/// async fn create_optimizer() {
///     let mem_table = create_mem_table().await;
///     let optimizer = Builder::new("timestamp")
///         .with_name("my_table")
///         .with_min_max_wheels(vec!["temperature", "humidity"])
///         .with_time_range(
///             ScalarValue::Date32(Some(19000)),
///             ScalarValue::Date32(Some(19100))
///         )
///         .unwrap()
///         .build_with_provider(Arc::new(mem_table))
///         .await
///         .unwrap();
/// }
/// ```
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
    ///
    /// # Arguments
    ///
    /// * `time_column` - The name of the column that represents time in the dataset
    ///
    /// # Examples
    ///
    /// ```
    /// use datafusion_uwheel::builder::Builder;
    ///
    /// let builder = Builder::new("timestamp");
    /// ```
    pub fn new(time_column: impl Into<String>) -> Self {
        Self {
            name: "".to_string(),
            time_column: time_column.into(),
            min_max_columns: Default::default(),
            wheel_conf: Self::default_haw_conf(),
            time_range: None,
        }
    }

    /// Create a default Haw configuration
    ///
    /// This method sets up a HawConf with Index mode and Keep retention policy for all time dimensions.
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

    /// Set the name of the table
    ///
    /// # Arguments
    ///
    /// * `name` - The name to be assigned to the table
    ///
    /// # Examples
    ///
    /// ```
    /// use datafusion_uwheel::builder::Builder;
    ///
    /// let builder = Builder::new("timestamp").with_name("my_table");
    /// ```
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Set the Haw configuration to use when building wheels
    ///
    /// # Arguments
    ///
    /// * `conf` - The HawConf to be used
    ///
    /// # Examples
    ///
    /// ```
    /// use datafusion_uwheel::builder::Builder;
    /// use uwheel::HawConf;
    ///
    /// let custom_conf = HawConf::default();
    /// let builder = Builder::new("timestamp").with_haw_conf(custom_conf);
    /// ```
    pub fn with_haw_conf(mut self, conf: HawConf) -> Self {
        self.wheel_conf = conf;
        self
    }

    /// Applies a time range when building the index
    ///
    /// # Arguments
    ///
    /// * `start` - The start of the time range (inclusive)
    /// * `end` - The end of the time range (inclusive)
    ///
    /// # Returns
    ///
    /// * `Result<Self, DataFusionError>` - Ok if the time range is valid, Err otherwise
    ///
    /// # Examples
    ///
    /// ```
    /// use datafusion_uwheel::builder::Builder;
    /// use datafusion::prelude::*;
    /// use datafusion::common::ScalarValue;
    ///
    /// let builder = Builder::new("timestamp")
    ///     .with_time_range(
    ///         ScalarValue::Date32(Some(19000)),
    ///         ScalarValue::Date32(Some(19100))
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

    /// Columns to build min/max wheels for
    ///
    /// # Arguments
    ///
    /// * `columns` - A vector of column names to build min/max wheels for
    ///
    /// # Examples
    ///
    /// ```
    /// use datafusion_uwheel::builder::Builder;
    ///
    /// let builder = Builder::new("timestamp")
    ///     .with_min_max_wheels(vec!["temperature", "humidity"]);
    /// ```
    pub fn with_min_max_wheels(mut self, columns: Vec<&str>) -> Self {
        self.min_max_columns = columns.iter().map(|s| s.to_string()).collect();
        self
    }

    /// Builds the UWheelOptimizer using the provided TableProvider
    ///
    /// # Arguments
    ///
    /// * `provider` - The TableProvider to build the UWheelOptimizer from
    ///
    /// # Returns
    ///
    /// * `Result<UWheelOptimizer>` - The built UWheelOptimizer if successful
    ///
    /// # Examples
    ///
    /// ```
    /// use datafusion_uwheel::builder::Builder;
    /// use std::sync::Arc;
    /// use datafusion::datasource::TableProvider;
    /// use datafusion::error::Result;
    /// use datafusion_uwheel::UWheelOptimizer;
    ///
    /// async fn build_optimizer(provider: Arc<dyn TableProvider>) -> Result<UWheelOptimizer> {
    ///     Builder::new("timestamp")
    ///         .with_name("my_table")
    ///         .build_with_provider(provider)
    ///         .await
    /// }
    /// ```
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
