use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use uwheel::{
    aggregator::{
        all::AllAggregator,
        avg::F64AvgAggregator,
        max::F64MaxAggregator,
        min::F64MinAggregator,
        min_max::F64MinMaxAggregator,
        sum::{F64SumAggregator, U32SumAggregator},
    },
    wheels::read::ReaderWheel,
    Aggregator,
};

type WheelMap<A> = Arc<Mutex<HashMap<String, ReaderWheel<A>>>>;

#[derive(Clone)]
pub struct BuiltInWheels {
    /// A COUNT(*) wheel over the underlying table data and time column
    pub count: ReaderWheel<U32SumAggregator>,
    /// Min/Max pruning wheels for a specific column
    pub min_max: WheelMap<F64MinMaxAggregator>,
    /// SUM Aggregation Wheel Indices
    pub sum: WheelMap<F64SumAggregator>,
    /// AVG Aggregation Wheel Indices
    pub avg: WheelMap<F64AvgAggregator>,
    /// MAX Aggregation Wheel Indices
    pub max: WheelMap<F64MaxAggregator>,
    /// MIN Aggregation Wheel Indices
    pub min: WheelMap<F64MinAggregator>,
    /// ALL (SUM, AVG, MAX, MIN, COUNT) Aggregation Wheel Indices
    pub all: WheelMap<AllAggregator>,
}
impl BuiltInWheels {
    pub fn new(
        count: ReaderWheel<U32SumAggregator>,
        min_max_wheels: WheelMap<F64MinMaxAggregator>,
    ) -> Self {
        Self {
            count,
            min_max: min_max_wheels,
            sum: Default::default(),
            avg: Default::default(),
            min: Default::default(),
            max: Default::default(),
            all: Default::default(),
        }
    }
    /// Returns the total number of bytes used by all wheel indices
    pub fn index_usage_bytes(&self) -> usize {
        let mut bytes = 0;

        fn wheel_bytes<A: Aggregator>(wheels: &WheelMap<A>) -> usize {
            wheels
                .lock()
                .unwrap()
                .iter()
                .map(|(_, wheel)| wheel.as_ref().size_bytes())
                .sum::<usize>()
        }

        bytes += self.count.as_ref().size_bytes();
        bytes += wheel_bytes(&self.min_max);
        bytes += wheel_bytes(&self.avg);
        bytes += wheel_bytes(&self.sum);
        bytes += wheel_bytes(&self.min);
        bytes += wheel_bytes(&self.max);
        bytes += wheel_bytes(&self.all);

        bytes
    }
}
