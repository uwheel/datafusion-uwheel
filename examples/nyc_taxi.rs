use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use uwheel::{wheels::read::ReaderWheel, WheelRange};
use wheel_manager::{MinMaxAggregator, MinMaxState, WheelManager};

fn main() {
    // Build a WheelManager over parquet files in /data using the time column "tpep_dropoff_datetime"
    let manager: WheelManager = wheel_manager::Builder::new("data/", "tpep_dropoff_datetime")
        .with_min_max_wheels(vec!["fare_amount", "trip_distance"]) // Create Min/Max wheels for the columns "fare_amount" and "trip_distance"
        .build()
        .unwrap();

    // The following wheel can be used to quickly filter queries such as:
    // SELECT * FROM yellow_tripdata
    // WHERE tpep_dropoff_datetime >= '?' and < '?'
    //
    // Or can be used to quickly count the number of records between two dates
    // SELECT COUNT(*) FROM yellow_tripdata
    // WHERE tpep_dropoff_datetime >= '?' and < '?'
    let count_wheel = manager.count_wheel();

    println!("Landmark COUNT(*) {:?}", count_wheel.landmark());

    // The following wheel can be used to quickly filter queries such as:
    // SELECT * FROM yellow_tripdata
    // WHERE tpep_dropoff_datetime >= '?' and < '?'
    // AND fare_amount > 1000
    let fare_wheel = manager.min_max_wheel("fare_amount").unwrap();

    // let's use the dates 2022-01-01 and 2022-01-10 to illustrate
    let start = NaiveDate::from_ymd_opt(2022, 1, 1).unwrap();
    let start_date = Utc.from_utc_datetime(&start.and_hms_opt(0, 0, 0).unwrap());

    let end = NaiveDate::from_ymd_opt(2022, 1, 10).unwrap();
    let end_date = Utc.from_utc_datetime(&end.and_hms_opt(0, 0, 0).unwrap());

    // Whether there are fare amounts above 1000.0
    // DataFusion Expr: let expr = col("fare_amount").gt(lit(1000.0));
    let fare_pred = |min_max_state: MinMaxState| min_max_state.max_value() > 1000.0;

    // Check whether the filter between 2022-01-01  and 2022-01-10 can be skipped
    if temporal_filter(&fare_wheel, start_date, end_date, fare_pred) {
        println!("Cannot skip execution since there are rides with fare_amount > 1000.0 between {:?} and {:?}", start_date, end_date);
    } else {
        println!("Skipping execution since there no rides with fare_amount > 1000.0 between {:?} and {:?}", start_date, end_date);
    }

    // Check whether the filter between 2022-01-01 12:30 and 12:50 can be skipped
    let start = NaiveDate::from_ymd_opt(2022, 1, 1).unwrap();
    let start_date = Utc.from_utc_datetime(&start.and_hms_opt(12, 30, 0).unwrap());

    let end = NaiveDate::from_ymd_opt(2022, 1, 1).unwrap();
    let end_date = Utc.from_utc_datetime(&end.and_hms_opt(12, 50, 0).unwrap());

    if temporal_filter(&fare_wheel, start_date, end_date, fare_pred) {
        println!("Cannot skip execution since there are rides with fare_amount > 1000.0 between {:?} and {:?}", start_date, end_date);
    } else {
        println!("Skipping execution since there no rides with fare_amount > 1000.0 between {:?} and {:?}", start_date, end_date);
    }
}

// Executes a temporal wheel filter to determine whether a query can be skipped
fn temporal_filter(
    wheel: &ReaderWheel<MinMaxAggregator>,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    predicate: impl Fn(MinMaxState) -> bool,
) -> bool {
    let start_ms = start.timestamp_millis() as u64;
    let end_ms = end.timestamp_millis() as u64;

    // Get the Min/Max state between the start and end date
    let min_max_state = wheel
        .combine_range_and_lower(WheelRange::new_unchecked(start_ms, end_ms))
        .unwrap();

    predicate(min_max_state)
}
