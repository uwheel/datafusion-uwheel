use core::fmt;
use std::{fmt::Formatter, sync::Arc};

use datafusion::{
    arrow::{
        array::{ArrayRef, Int64Array, RecordBatch},
        datatypes::SchemaRef,
    },
    physical_expr::EquivalenceProperties,
    physical_plan::{
        stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionMode,
        ExecutionPlan, Partitioning, PlanProperties,
    },
};
use uwheel::{aggregator::sum::U32SumAggregator, wheels::read::ReaderWheel, WheelRange};

use crate::COUNT_STAR_ALIAS;

/// An ExecutionPlan for COUNT(*) Aggregations on top of a UWheel
pub struct UWheelCountExec {
    wheel: ReaderWheel<U32SumAggregator>,
    schema: SchemaRef,
    range: WheelRange,
    properties: PlanProperties,
}

impl UWheelCountExec {
    pub fn new(wheel: ReaderWheel<U32SumAggregator>, schema: SchemaRef, range: WheelRange) -> Self {
        Self {
            wheel,
            schema: schema.clone(),
            range,
            properties: Self::compute_properties(schema),
        }
    }
    // taken from Datafusion repo
    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema);
        PlanProperties::new(
            eq_properties,
            // Output Partitioning
            Partitioning::UnknownPartitioning(1),
            // Execution Mode
            ExecutionMode::Bounded,
        )
    }
}

impl fmt::Debug for UWheelCountExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let wheel_exec_plan = self
            .wheel
            .as_ref()
            .explain_combine_range(self.range)
            .unwrap();
        write!(f, "UWheelCountExec {:#?}", wheel_exec_plan)
    }
}
impl DisplayAs for UWheelCountExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> Result<(), core::fmt::Error> {
        write!(f, "UWheelCountExec")
    }
}

impl ExecutionPlan for UWheelCountExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &'static str
    where
        Self: Sized,
    {
        "uwheel_count_exec"
    }
    fn execute(
        &self,
        _partition: usize,
        _context: std::sync::Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<datafusion::execution::SendableRecordBatchStream> {
        // Query the wheel for the count
        let count = self.wheel.combine_range_and_lower(self.range).unwrap_or(0);
        let name = COUNT_STAR_ALIAS.to_string();
        let data = Int64Array::from(vec![count as i64]);
        let record_batch = RecordBatch::try_from_iter(vec![(&name, Arc::new(data) as ArrayRef)])?;

        let fut = futures::future::ready(Ok(record_batch.clone()));
        let stream = futures::stream::once(fut);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
    fn properties(&self) -> &PlanProperties {
        &self.properties
    }
    fn statistics(&self) -> datafusion::error::Result<datafusion::common::Statistics> {
        unimplemented!("UWheelCountExec::statistics");
    }
    fn with_new_children(
        self: std::sync::Arc<Self>,
        _children: Vec<std::sync::Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<std::sync::Arc<dyn ExecutionPlan>> {
        unimplemented!("UWheelCountExec::with_new_children");
    }

    fn children(&self) -> Vec<std::sync::Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        None
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
