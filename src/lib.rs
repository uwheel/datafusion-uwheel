use std::{
    collections::HashMap,
    fs::{DirEntry, File},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use datafusion::{
    arrow::{
        array::{Float64Array, TimestampMicrosecondArray},
        datatypes::DataType,
    },
    error::DataFusionError,
    prelude::*,
};
use datafusion::{error::Result, parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder};
use uwheel::{
    aggregator::sum::U32SumAggregator,
    wheels::read::{aggregation::conf::WheelMode, ReaderWheel},
    Conf, Entry, HawConf, RwWheel, WheelRange,
};

/// A MinMax Aggregator implementation
pub mod min_max;

pub use min_max::{MinMaxAggregator, MinMaxState};

/// Builder for creating a WheelManager
pub struct Builder {
    /// Directory where the raw parquet files are stored
    dir: PathBuf,
    /// The column in the parquet files that contains the time
    time_column: String,
    /// Columns to build min/max wheels for
    min_max_columns: Vec<String>,
    /// Default Haw configuration to use when building wheels
    wheel_conf: HawConf,
    /// Pre-defined filters to apply to the parquet files to build wheels
    filters: Vec<Expr>,
}

impl Builder {
    /// Create a new WheelManager builder
    pub fn new(dir: impl Into<PathBuf>, time_column: impl Into<String>) -> Self {
        Self {
            dir: dir.into(),
            time_column: time_column.into(),
            filters: Default::default(),
            min_max_columns: Default::default(),
            wheel_conf: HawConf::default(),
        }
    }

    /// Columns to build min/max wheels for
    ///
    /// Columns must be of numeric data types
    pub fn with_min_max_wheels(mut self, columns: Vec<&str>) -> Self {
        self.min_max_columns = columns.iter().map(|s| s.to_string()).collect();
        self
    }
    /// Pre-defined filters to apply to the parquet files
    pub fn with_filters(mut self, filters: Vec<Expr>) -> Self {
        self.filters = filters;
        self
    }
    pub fn build(self) -> Result<WheelManager> {
        WheelManager::try_new(self.dir, self.time_column, self.min_max_columns)
    }
}

/// A Wheel Manager over a set of parquet files
///
/// An extension that improves time-based analytical queries.
pub struct WheelManager {
    /// The column in the parquet files that contains the time
    time_column: String,
    /// Directory where the raw parquet files are stored
    dir: PathBuf,
    /// A COUNT(*) wheel over the parquet files and time column
    count: ReaderWheel<U32SumAggregator>,
    // Min/Max pruning wheels over the parquet files for a specific column
    min_max_wheels: Arc<Mutex<HashMap<String, ReaderWheel<MinMaxAggregator>>>>,
}

impl WheelManager {
    /// Create a new WheelManager using the given directory and time column
    ///
    /// If the specified time column does not exist in the parquet files, an error is returned.
    pub fn try_new(
        dir: impl Into<PathBuf>,
        time_column: impl Into<String>,
        min_max_columns: Vec<String>,
    ) -> Result<Self> {
        let dir = dir.into();
        let time_column = time_column.into();

        // Create an "initial" manager over the parquet files
        let (count, min_max_wheels) = build(&dir, &time_column, min_max_columns)?;

        Ok(Self {
            time_column,
            dir,
            count,
            min_max_wheels: Arc::new(Mutex::new(min_max_wheels)),
        })
    }

    /// Count the number of rows in the given time range
    ///
    /// Returns `None` if the count is not available
    #[inline]
    pub fn count(&self, range: WheelRange) -> Option<u32> {
        self.count.combine_range_and_lower(range)
    }

    /// Returns a reference to the table's COUNT(*) wheel
    #[inline]
    pub fn count_wheel(&self) -> &ReaderWheel<U32SumAggregator> {
        &self.count
    }

    /// Returns a reference to a MIN/MAX wheel for a specified column
    pub fn min_max_wheel(&self, column: &str) -> Option<ReaderWheel<MinMaxAggregator>> {
        self.min_max_wheels.lock().unwrap().get(column).cloned()
    }
}

fn build(
    dir: &PathBuf,
    time_column: &str,
    min_max_columns: Vec<String>,
) -> Result<(
    ReaderWheel<U32SumAggregator>,
    HashMap<String, ReaderWheel<MinMaxAggregator>>,
)> {
    let files = read_dir(dir.as_path())?;

    let mut timestamps = Vec::new();
    for file in files.iter() {
        let file = File::open(file.path()).unwrap();
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(file)?
            .with_batch_size(8192)
            .build()?;

        for batch in parquet_reader {
            let batch = batch?;

            // Verify the time column exists in the parquet file
            let time_column_exists = batch.schema().index_of(time_column).is_ok();
            assert!(
                time_column_exists,
                "Specified Time column does not exist in parquet file"
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
    }

    // Build a COUNT(*) wheel over the timestamps
    let (count_wheel, min_timestamp_ms, max_timestamp_ms) = build_count_wheel(timestamps);

    // Build MinMax wheels for specified columns
    let mut map = HashMap::new();
    for column in min_max_columns.iter() {
        let min_max_wheel = build_min_max_wheel(
            &files,
            min_timestamp_ms,
            max_timestamp_ms,
            time_column,
            column,
        )?;
        map.insert(column.to_string(), min_max_wheel);
    }

    Ok((count_wheel.read().clone(), map))
}

fn build_min_max_wheel(
    files: &[DirEntry],
    _min_timestamp_ms: u64,
    max_timestamp_ms: u64,
    time_col: &str,
    min_max_col: &str,
) -> Result<ReaderWheel<MinMaxAggregator>> {
    // TODO: remove hardcoded time
    let start = NaiveDate::from_ymd_opt(2022, 1, 1).unwrap();
    let date = Utc.from_utc_datetime(&start.and_hms_opt(0, 0, 0).unwrap());
    let start_ms = date.timestamp_millis() as u64;

    // TODO: get Conf from builder
    let mut conf = HawConf::default()
        .with_watermark(start_ms)
        .with_mode(WheelMode::Index);

    conf.minutes
        .set_retention_policy(uwheel::RetentionPolicy::Keep);

    conf.hours
        .set_retention_policy(uwheel::RetentionPolicy::Keep);

    conf.days
        .set_retention_policy(uwheel::RetentionPolicy::Keep);

    conf.weeks
        .set_retention_policy(uwheel::RetentionPolicy::Keep);

    let mut wheel: RwWheel<MinMaxAggregator> = RwWheel::with_conf(
        Conf::default()
            .with_haw_conf(conf)
            .with_write_ahead(64000usize.next_power_of_two()),
    );

    for file in files.iter() {
        let file = File::open(file.path()).unwrap();
        let parquet_builder = ParquetRecordBatchReaderBuilder::try_new(file)?.with_batch_size(8192);

        let schema = parquet_builder.schema().clone();
        let parquet_reader = parquet_builder.build()?;

        let column_index = schema.index_of(min_max_col)?;
        let column_field = schema.field(column_index);

        if is_numeric_type(column_field.data_type()) {
            for batch in parquet_reader {
                let batch = batch?;

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
                    let timestamp_ms = DateTime::from_timestamp_micros(timestamp as i64)
                        .unwrap()
                        .timestamp_millis() as u64;
                    let entry = Entry::new(value, timestamp_ms);
                    wheel.insert(entry);
                }
            }
            wheel.advance_to(max_timestamp_ms);
        } else {
            panic!("Min/Max column must be a numeric type");
        }
    }
    Ok(wheel.read().clone())
}

/// Builds a COUNT(*) wheel over the given timestamps
///
/// Uses a U32SumAggregator internally with prefix-sum optimization
fn build_count_wheel(timestamps: Vec<i64>) -> (RwWheel<U32SumAggregator>, u64, u64) {
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

    let mut conf = HawConf::default()
        .with_watermark(start_ms)
        .with_mode(WheelMode::Index);

    conf.minutes
        .set_retention_policy(uwheel::RetentionPolicy::Keep);

    conf.hours
        .set_retention_policy(uwheel::RetentionPolicy::Keep);

    conf.days
        .set_retention_policy(uwheel::RetentionPolicy::Keep);

    conf.weeks
        .set_retention_policy(uwheel::RetentionPolicy::Keep);

    let mut count_wheel: RwWheel<U32SumAggregator> = RwWheel::with_conf(
        Conf::default()
            .with_haw_conf(conf)
            .with_write_ahead(64000usize.next_power_of_two()),
    );

    for timestamp in timestamps {
        let timestamp_ms = DateTime::from_timestamp_micros(timestamp as i64)
            .unwrap()
            .timestamp_millis() as u64;
        // Record a count
        let entry = Entry::new(1, timestamp_ms);
        count_wheel.insert(entry);
    }
    dbg!(start_ms, max_ms);

    count_wheel.advance_to(max_ms);

    // convert wheel to index
    count_wheel.read().to_simd_wheels();
    count_wheel.read().to_prefix_wheels();

    (count_wheel, min_ms, max_ms)
}

/// Return a list of the directory entries in the given directory, sorted by name
fn read_dir(dir: &Path) -> Result<Vec<DirEntry>> {
    let mut files = dir
        .read_dir()
        .map_err(|e| DataFusionError::from(e).context(format!("Error reading directory {dir:?}")))?
        .map(|entry| {
            entry.map_err(|e| {
                DataFusionError::from(e)
                    .context(format!("Error reading directory entry in {dir:?}"))
            })
        })
        .collect::<Result<Vec<DirEntry>>>()?;
    files.sort_by_key(|entry| entry.file_name());
    Ok(files)
}

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
