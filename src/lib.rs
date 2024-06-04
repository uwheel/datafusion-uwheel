use std::{
    collections::HashMap,
    fs::{DirEntry, File},
    path::{Path, PathBuf},
};

use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use datafusion::{arrow::array::TimestampMicrosecondArray, error::DataFusionError, prelude::*};
use datafusion::{error::Result, parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder};
use uwheel::{
    aggregator::sum::U32SumAggregator,
    wheels::read::{aggregation::conf::WheelMode, ReaderWheel},
    Conf, Entry, HawConf, RwWheel, WheelRange,
};

// column_name, aggregate function, data type
// "fare_amount", sum, f32

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
    pub fn with_min_max_columns(mut self, min_max_columns: Vec<&str>) -> Self {
        self.min_max_columns = min_max_columns.iter().map(|s| s.to_string()).collect();
        self
    }
    /// Pre-defined filters to apply to the parquet files
    pub fn with_filters(mut self, filters: Vec<Expr>) -> Self {
        self.filters = filters;
        self
    }
    pub fn build(self) -> Result<WheelManager> {
        WheelManager::try_new(self.dir, self.time_column)
    }
}

/// Manages wheels over a set of parquet files
///
/// Possible Wheels:
/// - COUNT(*): U32
/// - AGGREGATE(column): All
/// - MIN/MAX(column):
pub struct WheelManager {
    /// The column in the parquet files that contains the time
    time_column: String,
    /// Directory where the raw parquet files are stored
    dir: PathBuf,
    /// A COUNT(*) wheel over the parquet files and time column
    count: ReaderWheel<U32SumAggregator>,
    // A Min/Max pruning wheels over the parquet files for a specific column
    min_max_wheels: HashMap<String, ReaderWheel<U32SumAggregator>>,
    // min_max_wheels: Vec<ReaderWheel<MinMaxAggregator>>,
}

impl WheelManager {
    /// Create a new WheelManager using the given directory and time column
    ///
    /// If the specified time column does not exist in the parquet files, an error is returned.
    pub fn try_new(dir: impl Into<PathBuf>, time_column: impl Into<String>) -> Result<Self> {
        let dir = dir.into();
        let time_column = time_column.into();

        // Create an "initial" manager over the parquet files
        let count = build(&dir, &time_column)?;

        Ok(Self {
            time_column,
            dir,
            count,
            min_max_wheels: Default::default(),
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

    /// Return the minimum
    ///
    /// Returns `(None, None)` if the min/max is not available
    pub fn min_max_wheel(&self, _column: usize) -> Option<ReaderWheel<U32SumAggregator>> {
        // TODO: Implement min_max aggregation
        None
    }
    //pub fn build_wheels(&self, _filters: &[Expr]) {}
    //pub fn aggregate(&self, time: WheelRange, filter: &Expr) {}
}

fn build(dir: &PathBuf, time_column: &str) -> Result<ReaderWheel<U32SumAggregator>> {
    let files = read_dir(dir.as_path())?;

    let mut timestamps = Vec::new();
    for file in files {
        let file = File::open(file.path()).unwrap();
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(file)?
            .with_batch_size(8192)
            .build()?;

        for batch in parquet_reader {
            let batch = batch?;
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
    let count_wheel = build_count_wheel(timestamps);

    // TODO: Build min/max wheels for specified columns

    Ok(count_wheel.read().clone())
}

/// Builds a COUNT(*) wheel over the given timestamps
///
/// Uses a U32SumAggregator internally with prefix-sum optimization
fn build_count_wheel(timestamps: Vec<i64>) -> RwWheel<U32SumAggregator> {
    let min = timestamps.iter().min().copied().unwrap();
    let max = timestamps.iter().max().copied().unwrap();

    let _min_ms = DateTime::from_timestamp_micros(min)
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

    count_wheel
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
