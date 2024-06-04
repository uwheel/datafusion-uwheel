use chrono::DateTime;
use wheel_manager::Builder;

fn main() {
    let builder = Builder::new("data/", "tpep_dropoff_datetime")
        .with_min_max_columns(vec!["fare_amount"])
        .build()
        .unwrap();
    let count_wheel = builder.count_wheel();

    let watermark = count_wheel.watermark();
    println!("Landmark COUNT(*) {:?}", count_wheel.as_ref().landmark());

    let watermark = DateTime::from_timestamp_millis(watermark as i64)
        .unwrap()
        .to_utc()
        .naive_utc()
        .to_string();

    println!("WATERMARK: {:?}", watermark);
}
