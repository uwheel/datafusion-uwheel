use uwheel::{aggregator::PartialAggregateType, Aggregator};

#[derive(Debug, Clone, Copy)]
pub struct MinMaxState {
    min: f64,
    max: f64,
}
impl MinMaxState {
    pub fn new(min: f64, max: f64) -> Self {
        Self { min, max }
    }
    #[inline]
    pub fn merge(&mut self, other: Self) {
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
    }
    pub fn min_value(&self) -> f64 {
        self.min
    }
    pub fn max_value(&self) -> f64 {
        self.max
    }
}

impl PartialAggregateType for MinMaxState {}

impl Default for MinMaxState {
    fn default() -> Self {
        Self {
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        }
    }
}

#[derive(Default, Debug, Clone, Copy)]
pub struct MinMaxAggregator;

impl Aggregator for MinMaxAggregator {
    const IDENTITY: Self::PartialAggregate = MinMaxState {
        min: f64::INFINITY,
        max: f64::NEG_INFINITY,
    };

    type Input = f64;
    type PartialAggregate = MinMaxState;
    type MutablePartialAggregate = Self::PartialAggregate;
    type Aggregate = Self::PartialAggregate;

    fn lift(input: Self::Input) -> Self::MutablePartialAggregate {
        Self::PartialAggregate {
            min: input,
            max: input,
        }
    }

    fn combine_mutable(mutable: &mut Self::MutablePartialAggregate, input: Self::Input) {
        let state = Self::lift(input);
        mutable.merge(state)
    }

    fn freeze(a: Self::MutablePartialAggregate) -> Self::PartialAggregate {
        a
    }

    fn combine(mut a: Self::PartialAggregate, b: Self::PartialAggregate) -> Self::PartialAggregate {
        a.merge(b);
        a
    }
    fn lower(a: Self::PartialAggregate) -> Self::Aggregate {
        a
    }
}
